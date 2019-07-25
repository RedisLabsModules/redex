FROM redislabsmodules/rmbuilder:latest as builder

# Build the source
ADD . /REDEX
WORKDIR /REDEX
RUN set -ex;\
    make clean; \
    make all;

# Package the runner
FROM redis:latest
ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN set -ex;\
    mkdir -p "$LIBDIR";
COPY --from=builder /REDEX/src/rxgeo.so "$LIBDIR"
COPY --from=builder /REDEX/src/rxkeys.so "$LIBDIR"
COPY --from=builder /REDEX/src/rxsets.so "$LIBDIR"
COPY --from=builder /REDEX/src/rxzsets.so "$LIBDIR"
COPY --from=builder /REDEX/src/rxhashes.so "$LIBDIR"
COPY --from=builder /REDEX/src/rxlists.so "$LIBDIR"
COPY --from=builder /REDEX/src/rxstrings.so "$LIBDIR"


CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/rxgeo.so", "--loadmodule", "/usr/lib/redis/modules/rxkeys.so", "--loadmodule", "/usr/lib/redis/modules/rxsets.so", "--loadmodule", "/usr/lib/redis/modules/rxzsets.so", "--loadmodule", "/usr/lib/redis/modules/rxhashes.so", "--loadmodule", "/usr/lib/redis/modules/rxlists.so", "--loadmodule", "/usr/lib/redis/modules/rxstrings.so"]

