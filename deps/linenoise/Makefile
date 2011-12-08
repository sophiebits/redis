uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

linenoise_example: linenoise.h linenoise.c

linenoise_example: linenoise.o example.o
	$(CC) $(ARCH) -Wall -W -Os -g -o linenoise_example linenoise.o example.o

.c.o:
	$(CC) $(ARCH) -c -Wall -W -Os -g $<

clean:
	rm -f linenoise_example *.o *.exe
