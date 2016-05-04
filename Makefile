all: librmutil redex

clean:
	$(MAKE) -C rmutil clean
	$(MAKE) -C src clean

.PHONY: redex
redex:
	$(MAKE) -C src

.PHONY: librmutil
librmutil:
	$(MAKE) -C rmutil
