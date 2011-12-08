#include "redis.h"
#ifndef _WIN32
#include <sys/uio.h>
#endif

void *dupClientReplyValue(void *o) {
    incrRefCount((robj*)o);
    return o;
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a,b);
}

static void initClient(redisClient *c) {
    c->bufpos = 0;
    selectDb(c,0);
    c->querybuf = sdsempty();
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->cmd = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->lastinteraction = time(NULL);
    c->authenticated = 0;
    c->replstate = REDIS_REPL_NONE;
    c->reply = listCreate();
    listSetFreeMethod(c->reply,decrRefCount);
    listSetDupMethod(c->reply,dupClientReplyValue);
    c->bpop.keys = NULL;
    c->bpop.count = 0;
    c->bpop.timeout = 0;
    c->bpop.target = NULL;
    c->io_keys = listCreate();
    c->watched_keys = listCreate();
    listSetFreeMethod(c->io_keys,decrRefCount);
    c->pubsub_channels = dictCreate(&setDictType,NULL);
    c->pubsub_patterns = listCreate();
    listSetFreeMethod(c->pubsub_patterns,decrRefCount);
    listSetMatchMethod(c->pubsub_patterns,listMatchObjects);
    listAddNodeTail(server.clients,c);
    initClientMultiState(c);
}

#ifdef LIBUV
static uv_buf_t readbuf_alloc(uv_handle_t* handle, size_t suggested_size) {
    redisClient *c = handle->data;
    if (c->reqbufinuse == 0) {
        c->reqbufinuse = 1;
        return uv_buf_init(c->reqbuf, REDIS_IOBUF_LEN);
    }
    return uv_buf_init(zmalloc(suggested_size), suggested_size);
}

void freeClientClose(uv_handle_t* handle) {
    zfree(handle);
}

redisClient *createClient(uv_stream_t* stream) {
    int fd;
    redisClient *c = zmalloc(sizeof(redisClient));

    uv_tcp_getsocket((uv_tcp_t *)stream, &fd);
    if (fd != -1) {
        anetTcpNoDelay(NULL,fd);
    }
    stream->data = c;
    if (uv_read_start(stream, readbuf_alloc, readQueryFromClient) != 0) {
        uv_err_t err = uv_last_error(server.uvloop);
        redisLog(REDIS_VERBOSE, "start read from client: %s", uv_strerror(err));
        uv_close((uv_handle_t*)stream, freeClientClose);
        zfree(c);
        return NULL;
    }

    c->stream = stream;
    c->cbpending = 0;
    c->wrData.state = WRITECLIENT_IDLE;
    c->wrData.cli = c;
    c->wrData.bufsUsed = 0;
    c->wrData.listIndex = 0;
    c->reqbufinuse = 0;
    uv_prepare_init(server.uvloop, &c->sendReply_handle);

    initClient(c);
    return c;
}
#else
redisClient *createClient(int fd) {
    redisClient *c = zmalloc(sizeof(redisClient));

    anetNonBlock(NULL,fd);
    anetTcpNoDelay(NULL,fd);
    if (aeCreateFileEvent(server.el,fd,AE_READABLE,
        readQueryFromClient, c) == AE_ERR)
    {
        close(fd);
        zfree(c);
        return NULL;
    }

    c->fd = fd;

    initClient(c);
    return c;
}
#endif

#ifdef LIBUV
/* Set the event loop to callback so we can write accumulated data
 * Typically gets called every time a reply is built. */
int _installWriteEvent(redisClient *c) {
    if (c->cbpending > 0) return REDIS_OK;      /* if stream is NULL, then cbpending must be 0 */
    if (c->stream == NULL) return REDIS_ERR;
    if (c->bufpos == 0 && listLength(c->reply) == 0 &&
        (c->replstate == REDIS_REPL_NONE ||
         c->replstate == REDIS_REPL_ONLINE)) {

        c->sendReply_handle.data = c;
        if (uv_prepare_start(&c->sendReply_handle, sendReplyToClient) != 0) {
            return REDIS_ERR;
        }
        c->cbpending++;
    }
    return REDIS_OK;
}
#else

/* Set the event loop to listen for write events on the client's socket.
 * Typically gets called every time a reply is built. */
int _installWriteEvent(redisClient *c) {
    if (c->fd <= 0) return REDIS_ERR;
    if (c->bufpos == 0 && listLength(c->reply) == 0 &&
        (c->replstate == REDIS_REPL_NONE ||
         c->replstate == REDIS_REPL_ONLINE) &&
        aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
        sendReplyToClient, c) == AE_ERR) return REDIS_ERR;
    return REDIS_OK;
}
#endif

/* Create a duplicate of the last object in the reply list when
 * it is not exclusively owned by the reply list. */
robj *dupLastObjectIfNeeded(list *reply) {
    robj *new, *cur;
    listNode *ln;
    redisAssert(listLength(reply) > 0);
    ln = listLast(reply);
    cur = listNodeValue(ln);
    if (cur->refcount > 1) {
        new = dupStringObject(cur);
        decrRefCount(cur);
        listNodeValue(ln) = new;
    }
    return listNodeValue(ln);
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

int _addReplyToBuffer(redisClient *c, char *s, size_t len) {
    size_t available = sizeof(c->buf)-c->bufpos;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return REDIS_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(c->reply) > 0) return REDIS_ERR;

    /* Check that the buffer has enough space available for this string. */
    if (len > available) return REDIS_ERR;

    memcpy(c->buf+c->bufpos,s,len);
    c->bufpos+=len;
    return REDIS_OK;
}

void _addReplyObjectToList(redisClient *c, robj *o) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        incrRefCount(o);
        listAddNodeTail(c->reply,o);
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+sdslen(o->ptr) <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,o->ptr,sdslen(o->ptr));
        } else {
            incrRefCount(o);
            listAddNodeTail(c->reply,o);
        }
    }
}

/* This method takes responsibility over the sds. When it is no longer
 * needed it will be free'd, otherwise it ends up in a robj. */
void _addReplySdsToList(redisClient *c, sds s) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) {
        sdsfree(s);
        return;
    }

    if (listLength(c->reply) == 0) {
        listAddNodeTail(c->reply,createObject(REDIS_STRING,s));
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+sdslen(s) <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,s,sdslen(s));
            sdsfree(s);
        } else {
            listAddNodeTail(c->reply,createObject(REDIS_STRING,s));
        }
    }
}

void _addReplyStringToList(redisClient *c, char *s, size_t len) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        listAddNodeTail(c->reply,createStringObject(s,len));
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+len <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,s,len);
        } else {
            listAddNodeTail(c->reply,createStringObject(s,len));
        }
    }
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

void addReply(redisClient *c, robj *obj) {
    if (_installWriteEvent(c) != REDIS_OK) return;
    redisAssert(!server.vm_enabled || obj->storage == REDIS_VM_MEMORY);

    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. */
    if (obj->encoding == REDIS_ENCODING_RAW) {
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != REDIS_OK)
            _addReplyObjectToList(c,obj);
    } else {
        /* FIXME: convert the long into string and use _addReplyToBuffer()
         * instead of calling getDecodedObject. As this place in the
         * code is too performance critical. */
        obj = getDecodedObject(obj);
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != REDIS_OK)
            _addReplyObjectToList(c,obj);
        decrRefCount(obj);
    }
}

void addReplySds(redisClient *c, sds s) {
    if (_installWriteEvent(c) != REDIS_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) == REDIS_OK) {
        sdsfree(s);
    } else {
        /* This method free's the sds when it is no longer needed. */
        _addReplySdsToList(c,s);
    }
}

void addReplyString(redisClient *c, char *s, size_t len) {
    if (_installWriteEvent(c) != REDIS_OK) return;
    if (_addReplyToBuffer(c,s,len) != REDIS_OK)
        _addReplyStringToList(c,s,len);
}

void _addReplyError(redisClient *c, char *s, size_t len) {
    addReplyString(c,"-ERR ",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyError(redisClient *c, char *err) {
    _addReplyError(c,err,strlen(err));
}

void addReplyErrorFormat(redisClient *c, const char *fmt, ...) {
    sds s;
    va_list ap;
    va_start(ap,fmt);
    s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    _addReplyError(c,s,sdslen(s));
    sdsfree(s);
}

void _addReplyStatus(redisClient *c, char *s, size_t len) {
    addReplyString(c,"+",1);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyStatus(redisClient *c, char *status) {
    _addReplyStatus(c,status,strlen(status));
}

void addReplyStatusFormat(redisClient *c, const char *fmt, ...) {
    sds s;
    va_list ap;
    va_start(ap,fmt);
    s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    _addReplyStatus(c,s,sdslen(s));
    sdsfree(s);
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *addDeferredMultiBulkLength(redisClient *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredMultiBulkLength() will be called. */
    if (_installWriteEvent(c) != REDIS_OK) return NULL;
    listAddNodeTail(c->reply,createObject(REDIS_STRING,NULL));
    return listLast(c->reply);
}

/* Populate the length object and try glueing it to the next chunk. */
void setDeferredMultiBulkLength(redisClient *c, void *node, long length) {
    robj *len, *next;
    listNode *ln = (listNode*)node;

    /* Abort when *node is NULL (see addDeferredMultiBulkLength). */
    if (node == NULL) return;

    len = listNodeValue(ln);
    len->ptr = sdscatprintf(sdsempty(),"*%ld\r\n",length);
    if (ln->next != NULL) {
        next = listNodeValue(ln->next);

        /* Only glue when the next node is non-NULL (an sds in this case) */
        if (next->ptr != NULL) {
            len->ptr = sdscatlen(len->ptr,next->ptr,sdslen(next->ptr));
            listDelNode(c->reply,ln->next);
        }
    }
}

/* Add a duble as a bulk reply */
void addReplyDouble(redisClient *c, double d) {
    char dbuf[128], sbuf[128];
    int dlen, slen;
#ifdef _WIN32
    if (isnan(d)) {
        dlen = snprintf(dbuf,sizeof(dbuf),"nan");
    } else if (isinf(d)) {
        if (d < 0)
            dlen = snprintf(dbuf,sizeof(dbuf),"-inf");
        else
            dlen = snprintf(dbuf,sizeof(dbuf),"inf");
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    }
#else
    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
#endif
    slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
    addReplyString(c,sbuf,slen);
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void _addReplyLongLong(redisClient *c, long long ll, char prefix) {
    char buf[128];
    int len;
    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

void addReplyLongLong(redisClient *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        _addReplyLongLong(c,ll,':');
}

void addReplyMultiBulkLen(redisClient *c, long length) {
    _addReplyLongLong(c,length,'*');
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(redisClient *c, robj *obj) {
    size_t len;

    if (obj->encoding == REDIS_ENCODING_RAW) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }
    _addReplyLongLong(c,len,'$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(redisClient *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(redisClient *c, void *p, size_t len) {
    _addReplyLongLong(c,len,'$');
    addReplyString(c,p,len);
    addReply(c,shared.crlf);
}

/* Add a C nul term string as bulk reply */
void addReplyBulkCString(redisClient *c, char *s) {
    if (s == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(redisClient *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}


#ifdef LIBUV
int uvPeerToString(uv_stream_t* stream, char *ip, int *port, int iplen) {
    struct sockaddr_in sa;
    socklen_t saLen = sizeof(sa);

    *ip = 0;
    *port = 0;
    if (stream->type == UV_TCP) {
        saLen = sizeof(sa);
        if (uv_tcp_getpeername((uv_tcp_t*)stream, (struct sockaddr*)&sa, &saLen) == 0) {
            if (ip) uv_ip4_name(&sa, ip, iplen);
            if (port) *port = ntohs(sa.sin_port);
            return 0;
        } else {
            return -1;
        }
    } else {
        strncpy(ip, "domain", iplen);
        return 0;
    }
}

static void writedone(uv_write_t* req, int status) {
    uv_write_t* wr = (uv_write_t*) req;
    REDIS_NOTUSED(status);
    zfree(wr);
}

static void acceptCommonHandler(uv_stream_t* stream) {
    redisClient *c;

    if ((c = createClient(stream)) == NULL) {
        redisLog(REDIS_WARNING,"Error allocating resoures for the client");
        return;
    }
    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in nonblocking
     * mode and we can send an error for free using the Kernel I/O */
    if (server.maxclients && listLength(server.clients) > server.maxclients) {
        uv_buf_t bufs[1];
        char *err = "-ERR max number of clients reached\r\n";
        uv_write_t *wr = (uv_write_t*)zmalloc(sizeof(uv_write_t));

        bufs[0] = uv_buf_init(err, strlen(err));

        /* That's a best effort error message, don't check write errors */
        if (uv_write(wr, stream, bufs, 1, writedone)) {
            /* Nothing to do, Just to avoid the warning... */
        }
        freeClient(c);
        return;
    }
    server.stat_numconnections++;
}

void acceptTcpHandler(uv_stream_t* tcpsrv, int status) {
    int cport;
    char cip[128];
    uv_tcp_t* stream;
    int fd;

    if (status != 0) {
        redisLog(REDIS_WARNING,"acceptTcpHandler: %d", status);
        goto accerr;
    }
    stream = zmalloc(sizeof(uv_tcp_t));

    if (uv_tcp_init(server.uvloop, stream) != 0) {
        goto accerr;
    }
    stream->data = tcpsrv;

    if (uv_accept(tcpsrv, (uv_stream_t*)stream) != 0) {
        goto accerr;
    }

    uv_tcp_getsocket(stream, &fd);
    if (fd != -1) {
        int val = 0;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
#ifdef _WIN32
        val = 1;
        setsockopt(fd, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, &val, sizeof(val));
#endif
    }
    uvPeerToString((uv_stream_t*)stream, cip, &cport, sizeof(cip));
    redisLog(REDIS_VERBOSE,"Accepted %s:%d", cip, cport);

    acceptCommonHandler((uv_stream_t*)stream);
    return;

accerr:
    strncpy(server.neterr, uv_strerror(uv_last_error(server.uvloop)), sizeof(server.neterr)-1);
    server.neterr[sizeof(server.neterr)-1] = '\0';
    redisLog(REDIS_WARNING,"Accepting client connection: %s", server.neterr);
    return;
}
void acceptUnixHandler(uv_stream_t* pipesrv, int status) {
    uv_pipe_t* stream;
    if (status != 0) {
        goto accerr;
    }
    stream = zmalloc(sizeof(uv_pipe_t));

    if (uv_pipe_init(server.uvloop, stream, 0) != 0) {
        goto accerr;
    }
    stream->data = pipesrv;

    if (uv_accept(pipesrv, (uv_stream_t*)stream) != 0) {
        goto accerr;
    }

    redisLog(REDIS_VERBOSE,"Accepted connection to %s", server.unixsocket);

    acceptCommonHandler((uv_stream_t*)stream);
    return;

accerr:
    strncpy(server.neterr, uv_strerror(uv_last_error(server.uvloop)), sizeof(server.neterr)-1);
    server.neterr[sizeof(server.neterr)-1] = '\0';
    redisLog(REDIS_WARNING,"Accepting client connection: %s", server.neterr);
    return;
}

void freeClientShutdown(uv_shutdown_t* req, int status) {
    REDIS_NOTUSED(status);
    uv_close((uv_handle_t*)req->handle, freeClientClose);
    zfree(req);
}

void freeClientConnections(redisClient *c) {
    uv_shutdown_t *req;

    /* stop read, prepare callbacks, shutdown send */
    uv_read_stop(c->stream);
    uv_prepare_stop(&c->sendReply_handle);
    req = (uv_shutdown_t*) zmalloc(sizeof(uv_shutdown_t));
    uv_shutdown(req, c->stream, freeClientShutdown);
}

void replyToClientDone(uv_write_t* req, int status);

void doSendReplyToClient(redisClient *c) {
    int nwritten = 0, totwritten = 0, objlen;
    uv_write_t *wr = NULL;
    int status = 0;
    robj *o;

    if (c->wrData.state == WRITECLIENT_SENDING) {
        return;
    }

    if (c->flags & REDIS_MASTER) {
        /* do not send to master */
        c->bufpos = 0;
        c->sentlen = 0;
        while (listLength(c->reply)) {
            listDelNode(c->reply,listFirst(c->reply));
        }
        c->lastinteraction = time(NULL);
        return;
    }

    if (c->wrData.state == WRITECLIENT_IDLE) {
        c->wrData.bufsUsed = 0;
        c->wrData.listIndex = 0;
        while (c->wrData.bufsUsed < MAX_CLIENT_WRITEBUFS && 
              (c->bufpos > 0 || listLength(c->reply) > c->wrData.listIndex)) {
            if (c->bufpos > 0) {
                c->wrData.bufs[c->wrData.bufsUsed] = uv_buf_init(c->buf+c->sentlen, c->bufpos-c->sentlen);
                c->wrData.bufsUsed++;

                nwritten = c->bufpos - c->sentlen;
                c->sentlen += nwritten;
                totwritten += nwritten;

                /* If the buffer is processed, set bufpos to zero to continue with
                 * the remainder of the reply. */
                if (c->sentlen == c->bufpos) {
                    c->bufpos = 0;
                    c->sentlen = 0;
                }
            } else {
                // TODO: use iterator, do not del objects, save last iterator in wrData
                o = listNodeValue(listIndex(c->reply, c->wrData.listIndex));
                objlen = sdslen(o->ptr);

                if (objlen == 0) {
                    c->wrData.listIndex++;
                    continue;
                }

                c->wrData.bufs[c->wrData.bufsUsed] = uv_buf_init(((char*)o->ptr)+c->sentlen, objlen - c->sentlen);
                c->wrData.bufsUsed++;

                nwritten = objlen - c->sentlen;
                c->sentlen += nwritten;
                totwritten += nwritten;

                /* If we fully sent the object on head go to the next one */
                if (c->sentlen == objlen) {
                    c->wrData.listIndex++;
                    c->sentlen = 0;
                }
            }
            /* Note that we avoid to send more thank REDIS_MAX_WRITE_PER_EVENT
             * bytes, in a single threaded server it's a good idea to serve
             * other clients as well, even if a very large request comes from
             * super fast link that is always able to accept data (in real world
             * scenario think about 'KEYS *' against the loopback interfae) */
            if (totwritten > REDIS_MAX_WRITE_PER_EVENT) break;
        }
    }
    if (c->wrData.bufsUsed > 0) {
        wr = &(c->wrData.wr);
        wr->data = c;

        c->wrData.state = WRITECLIENT_SENDING;
        status = uv_write(wr, c->stream, c->wrData.bufs, c->wrData.bufsUsed, replyToClientDone);
        if (status != 0) {
            uv_err_t err = uv_last_error(server.uvloop);
            errno = err.sys_errno_;
            if (errno == UV_EAGAIN) {
                /* schedule a pending event to retry the write */
                c->wrData.state = WRITECLIENT_RETRY;
                if (uv_prepare_init(server.uvloop, &c->sendReply_handle) != 0) {
                    return;
                }
                c->sendReply_handle.data = c;
                if (uv_prepare_start(&c->sendReply_handle, sendReplyToClient) != 0) {
                    return;
                }
                c->cbpending++;
                return;
            } else {
                redisLog(REDIS_VERBOSE,
                    "Error writing to client: %s", strerror(errno));
                freeClient(c);
                return;
            }
        } else {
            c->cbpending++;
            if (totwritten > 0) c->lastinteraction = time(NULL);
            if (c->bufpos == 0 && listLength(c->reply) == 0 && c->cbpending == 0) {
                c->sentlen = 0;
            }
        }
    }
}

void sendReplyToClient(uv_prepare_t* handle, int status) {
    redisClient * c;
    REDIS_NOTUSED(status);

    c = (redisClient *)handle->data;

    /* prevent another callback until more data is added */
    uv_prepare_stop(handle);
    c->cbpending--;

    doSendReplyToClient(c);
}

void replyToClientDone(uv_write_t* req, int status) {
    redisClient *c = (redisClient *)req->data;
    c->cbpending--;
    c->wrData.state = WRITECLIENT_IDLE;

    if (status == 0) {
        /* release sent objects from list */
        while (c->wrData.listIndex > 0 && listLength(c->reply) > 0) {
            listDelNode(c->reply,listFirst(c->reply));
            c->wrData.listIndex--;
        }
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) {
            freeClient(c);
        } else {
            doSendReplyToClient(c);
        }
    } else {
        /* send failure */
        if (c->cbpending == 0) {
            freeClient(c);
        }
    }
}

#else
static void acceptCommonHandler(int fd) {
    redisClient *c;
    if ((c = createClient(fd)) == NULL) {
        redisLog(REDIS_WARNING,"Error allocating resoures for the client");
        close(fd); /* May be already closed, just ingore errors */
        return;
    }
    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in nonblocking
     * mode and we can send an error for free using the Kernel I/O */
    if (server.maxclients && listLength(server.clients) > server.maxclients) {
        char *err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors */
        if (write(c->fd,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        freeClient(c);
        return;
    }
    server.stat_numconnections++;
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    char cip[128];
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    cfd = anetTcpAccept(server.neterr, fd, cip, &cport);
    if (cfd == AE_ERR) {
        redisLog(REDIS_WARNING,"Accepting client connection: %s", server.neterr);
        return;
    }
    redisLog(REDIS_VERBOSE,"Accepted %s:%d", cip, cport);
    acceptCommonHandler(cfd);
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    cfd = anetUnixAccept(server.neterr, fd);
    if (cfd == AE_ERR) {
        redisLog(REDIS_WARNING,"Accepting client connection: %s", server.neterr);
        return;
    }
    redisLog(REDIS_VERBOSE,"Accepted connection to %s", server.unixsocket);
    acceptCommonHandler(cfd);
}

void freeClientConnections(redisClient *c) {
    aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
    aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    close(c->fd);
}

void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen;
    robj *o;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    while(c->bufpos > 0 || listLength(c->reply)) {
        if (c->bufpos > 0) {
            if (c->flags & REDIS_MASTER) {
                /* Don't reply to a master */
                nwritten = c->bufpos - c->sentlen;
            } else {
                nwritten = write(fd,c->buf+c->sentlen,c->bufpos-c->sentlen);
                if (nwritten <= 0) break;
            }
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            if (c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            o = listNodeValue(listFirst(c->reply));
            objlen = sdslen(o->ptr);

            if (objlen == 0) {
                listDelNode(c->reply,listFirst(c->reply));
                continue;
            }

            if (c->flags & REDIS_MASTER) {
                /* Don't reply to a master */
                nwritten = objlen - c->sentlen;
            } else {
                nwritten = write(fd, ((char*)o->ptr)+c->sentlen,objlen-c->sentlen);
                if (nwritten <= 0) break;
            }
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            if (c->sentlen == objlen) {
                listDelNode(c->reply,listFirst(c->reply));
                c->sentlen = 0;
            }
        }
        /* Note that we avoid to send more thank REDIS_MAX_WRITE_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interfae) */
        if (totwritten > REDIS_MAX_WRITE_PER_EVENT) break;
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            redisLog(REDIS_VERBOSE,
                "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    }
    if (totwritten > 0) c->lastinteraction = time(NULL);
    if (c->bufpos == 0 && listLength(c->reply) == 0) {
        c->sentlen = 0;
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);

        /* Close connection after entire reply has been sent. */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) freeClient(c);
    }
}
#endif


static void freeClientArgv(redisClient *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    c->argc = 0;
    c->cmd = NULL;
}

void freeClient(redisClient *c) {
    listNode *ln;

    /* Note that if the client we are freeing is blocked into a blocking
     * call, we have to set querybuf to NULL *before* to call
     * unblockClientWaitingData() to avoid processInputBuffer() will get
     * called. Also it is important to remove the file events after
     * this, because this call adds the READABLE event. */
    sdsfree(c->querybuf);
    c->querybuf = NULL;
    if (c->flags & REDIS_BLOCKED)
        unblockClientWaitingData(c);

    /* UNWATCH all the keys */
    unwatchAllKeys(c);
    listRelease(c->watched_keys);
    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);
    /* Obvious cleanup */
    freeClientConnections(c);
    listRelease(c->reply);
    freeClientArgv(c);

    /* Remove from the list of clients */
    ln = listSearchKey(server.clients,c);
    redisAssert(ln != NULL);
    listDelNode(server.clients,ln);
    /* When client was just unblocked because of a blocking operation,
     * remove it from the list with unblocked clients. */
    if (c->flags & REDIS_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        redisAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
    }
    /* Remove from the list of clients waiting for swapped keys, or ready
     * to be restarted, but not yet woken up again. */
    if (c->flags & REDIS_IO_WAIT) {
        redisAssert(server.vm_enabled);
        if (listLength(c->io_keys) == 0) {
            ln = listSearchKey(server.io_ready_clients,c);

            /* When this client is waiting to be woken up (REDIS_IO_WAIT),
             * it should be present in the list io_ready_clients */
            redisAssert(ln != NULL);
            listDelNode(server.io_ready_clients,ln);
        } else {
            while (listLength(c->io_keys)) {
                ln = listFirst(c->io_keys);
                dontWaitForSwappedKey(c,ln->value);
            }
        }
        server.vm_blocked_clients--;
    }
    listRelease(c->io_keys);
    /* Master/slave cleanup.
     * Case 1: we lost the connection with a slave. */
    if (c->flags & REDIS_SLAVE) {
        list *l;
        if (c->replstate == REDIS_REPL_SEND_BULK && c->repldbfd != -1)
            close(c->repldbfd);
        l = (c->flags & REDIS_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        redisAssert(ln != NULL);
        listDelNode(l,ln);
    }

    /* Case 2: we lost the connection with the master. */
    if (c->flags & REDIS_MASTER) {
        server.master = NULL;
        server.replstate = REDIS_REPL_CONNECT;
        server.repl_down_since = time(NULL);
        /* Since we lost the connection with the master, we should also
         * close the connection with all our slaves if we have any, so
         * when we'll resync with the master the other slaves will sync again
         * with us as well. Note that also when the slave is not connected
         * to the master it will keep refusing connections by other slaves.
         *
         * We do this only if server.masterhost != NULL. If it is NULL this
         * means the user called SLAVEOF NO ONE and we are freeing our
         * link with the master, so no need to close link with slaves. */
        if (server.masterhost != NULL) {
            while (listLength(server.slaves)) {
                ln = listFirst(server.slaves);
                freeClient((redisClient*)ln->value);
            }
        }
    }
    /* Release memory */
    zfree(c->argv);
    freeClientMultiState(c);
    zfree(c);
}

/* resetClient prepare the client to process the next command */
void resetClient(redisClient *c) {
    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;
}

void closeTimedoutClients(void) {
    redisClient *c;
    listNode *ln;
    time_t now = time(NULL);
    listIter li;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);
        if (server.maxidletime &&
            !(c->flags & REDIS_SLAVE) &&    /* no timeout for slaves */
            !(c->flags & REDIS_MASTER) &&   /* no timeout for masters */
            !(c->flags & REDIS_BLOCKED) &&  /* no timeout for BLPOP */
            dictSize(c->pubsub_channels) == 0 && /* no timeout for pubsub */
            listLength(c->pubsub_patterns) == 0 &&
            (now - c->lastinteraction > server.maxidletime))
        {
            redisLog(REDIS_VERBOSE,"Closing idle client");
            freeClient(c);
        } else if (c->flags & REDIS_BLOCKED) {
            if (c->bpop.timeout != 0 && c->bpop.timeout < now) {
                addReply(c,shared.nullmultibulk);
                unblockClientWaitingData(c);
            }
        }
    }
}

int processInlineBuffer(redisClient *c) {
    char *newline = strstr(c->querybuf,"\r\n");
    int argc, j;
    sds *argv;
    size_t querylen;

    /* Nothing to do without a \r\n */
    if (newline == NULL)
        return REDIS_ERR;

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->querybuf);
    argv = sdssplitlen(c->querybuf,querylen," ",1,&argc);

    /* Leave data after the first line of the query in the buffer */
    c->querybuf = sdsrange(c->querybuf,querylen+2,-1);

    /* Setup argv array on client structure */
    if (c->argv) zfree(c->argv);
    c->argv = zmalloc(sizeof(robj*)*argc);

    /* Create redis objects for all arguments. */
    for (c->argc = 0, j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            c->argv[c->argc] = createObject(REDIS_STRING,argv[j]);
            c->argc++;
        } else {
            sdsfree(argv[j]);
        }
    }
    zfree(argv);
    return REDIS_OK;
}

/* Helper function. Trims query buffer to make the function that processes
 * multi bulk requests idempotent. */
static void setProtocolError(redisClient *c, int pos) {
    c->flags |= REDIS_CLOSE_AFTER_REPLY;
    c->querybuf = sdsrange(c->querybuf,pos,-1);
}

int processMultibulkBuffer(redisClient *c) {
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        redisAssert(c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf,'\r');
        if (newline == NULL)
            return REDIS_ERR;

        /* Buffer should also contain \n */
        if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
            return REDIS_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        redisAssert(c->querybuf[0] == '*');
        ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
        if (!ok || ll > 1024*1024) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError(c,pos);
            return REDIS_ERR;
        }

        pos = (newline-c->querybuf)+2;
        if (ll <= 0) {
            c->querybuf = sdsrange(c->querybuf,pos,-1);
            return REDIS_OK;
        }

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
    }

    redisAssert(c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL)
                break;

            /* Buffer should also contain \n */
            if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
                break;

            if (c->querybuf[pos] != '$') {
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[pos]);
                setProtocolError(c,pos);
                return REDIS_ERR;
            }

            ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError(c,pos);
                return REDIS_ERR;
            }

            pos += newline-(c->querybuf+pos)+2;
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            c->argv[c->argc++] = createStringObject(c->querybuf+pos,c->bulklen);
            pos += c->bulklen+2;
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* Trim to pos */
    c->querybuf = sdsrange(c->querybuf,pos,-1);

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) {
        return REDIS_OK;
    }
    return REDIS_ERR;
}

void processInputBuffer(redisClient *c) {
    /* Keep processing while there is something in the input buffer */
    while(sdslen(c->querybuf)) {
        /* Immediately abort if the client is in the middle of something. */
        if (c->flags & REDIS_BLOCKED || c->flags & REDIS_IO_WAIT) return;

        /* REDIS_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands). */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

        /* Determine request type when unknown. */
        if (!c->reqtype) {
            if (c->querybuf[0] == '*') {
                c->reqtype = REDIS_REQ_MULTIBULK;
            } else {
                c->reqtype = REDIS_REQ_INLINE;
            }
        }

        if (c->reqtype == REDIS_REQ_INLINE) {
            if (processInlineBuffer(c) != REDIS_OK) break;
        } else if (c->reqtype == REDIS_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != REDIS_OK) break;
        } else {
            redisPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            /* Only reset the client when the command was executed. */
            if (processCommand(c) == REDIS_OK)
                resetClient(c);
        }
    }
}

#ifdef LIBUV
void readQueryFromClient(uv_stream_t* stream, ssize_t nread, uv_buf_t buf) {
    redisClient *c = (redisClient*) stream->data;

    if (nread < 0)  {
        if (uv_last_error(server.uvloop).code == UV_EOF) {
            redisLog(REDIS_VERBOSE, "Client closed connection");
        } else {
            redisLog(REDIS_VERBOSE, "Reading from client: %s",strerror(errno));
        }
        if (buf.base == c->reqbuf) {
            c->reqbufinuse = 0;
        } else if (buf.base) {
            zfree(buf.base);
        }
        freeClient(c);
        return;
    } else if (nread >= 0) {
        c->querybuf = sdscatlen(c->querybuf, buf.base, nread);
        c->lastinteraction = time(NULL);
    }
    processInputBuffer(c);

    if (buf.base == c->reqbuf) {
        c->reqbufinuse = 0;
    } else if (buf.base) {
        zfree(buf.base);
    }

}
#else
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = (redisClient*) privdata;
    char buf[REDIS_IOBUF_LEN];
    int nread;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    nread = read(fd, buf, REDIS_IOBUF_LEN);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            redisLog(REDIS_VERBOSE, "Reading from client: %s",strerror(errno));
            freeClient(c);
            return;
        }
    } else if (nread == 0) {
        redisLog(REDIS_VERBOSE, "Client closed connection");
        freeClient(c);
        return;
    }
    if (nread) {
        c->querybuf = sdscatlen(c->querybuf,buf,nread);
        c->lastinteraction = time(NULL);
    } else {
        return;
    }
    processInputBuffer(c);
}
#endif

void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    redisClient *c;
    listNode *ln;
    listIter li;
    unsigned long lol = 0, bib = 0;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);

        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

void clientCommand(redisClient *c) {
    listNode *ln;
    listIter li;
    redisClient *client;

    if (!strcasecmp(c->argv[1]->ptr,"list") && c->argc == 2) {
        sds o = sdsempty();
        time_t now = time(NULL);

        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            char ip[32], flags[16], *p;
            int port;
            int fd;

            client = listNodeValue(ln);
#ifdef LIBUV
            uv_tcp_getsocket((uv_tcp_t *)client->stream, &fd);
            if (uvPeerToString(client->stream, ip, &port, sizeof(ip)) == -1) continue;
#else
            fd = client->fd;
            if (anetPeerToString(client->fd,ip,&port) == -1) continue;
#endif
            p = flags;
            if (client->flags & REDIS_SLAVE) {
                if (client->flags & REDIS_MONITOR)
                    *p++ = 'O';
                else
                    *p++ = 'S';
            }
            if (client->flags & REDIS_MASTER) *p++ = 'M';
            if (p == flags) *p++ = 'N';
            if (client->flags & REDIS_MULTI) *p++ = 'x';
            if (client->flags & REDIS_BLOCKED) *p++ = 'b';
            if (client->flags & REDIS_IO_WAIT) *p++ = 'i';
            if (client->flags & REDIS_DIRTY_CAS) *p++ = 'd';
            if (client->flags & REDIS_CLOSE_AFTER_REPLY) *p++ = 'c';
            if (client->flags & REDIS_UNBLOCKED) *p++ = 'u';
            *p++ = '\0';
            o = sdscatprintf(o,
                "addr=%s:%d fd=%d idle=%ld flags=%s db=%d sub=%d psub=%d\n",
                ip,port,fd,
                (long)(now - client->lastinteraction),
                flags,
                client->db->id,
                (int) dictSize(client->pubsub_channels),
                (int) listLength(client->pubsub_patterns));
        }
        addReplyBulkCBuffer(c,o,sdslen(o));
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"kill") && c->argc == 3) {
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            char ip[32], addr[64];
            int port;

            client = listNodeValue(ln);
#ifdef LIBUV
            if (uvPeerToString(client->stream, ip, &port, sizeof(ip)) == -1) continue;
#else
            if (anetPeerToString(client->fd,ip,&port) == -1) continue;
#endif
            snprintf(addr,sizeof(addr),"%s:%d",ip,port);
            if (strcmp(addr,c->argv[2]->ptr) == 0) {
                addReply(c,shared.ok);
                if (c == client) {
                    client->flags |= REDIS_CLOSE_AFTER_REPLY;
                } else {
                    freeClient(client);
                }
                return;
            }
        }
        addReplyError(c,"No such client");
    } else {
        addReplyError(c, "Syntax error, try CLIENT (LIST | KILL ip:port)");
    }
}

void rewriteClientCommandVector(redisClient *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;
        
        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    /* We free the objects in the original vector at the end, so we are
     * sure that if the same objects are reused in the new vector the
     * refcount gets incremented before it gets decremented. */
    for (j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
    zfree(c->argv);
    /* Replace argv and argc with our new versions. */
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommand(c->argv[0]->ptr);
    redisAssert(c->cmd != NULL);
    va_end(ap);
}
