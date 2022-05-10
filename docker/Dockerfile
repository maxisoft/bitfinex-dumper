FROM ghcr.io/maxisoft/nim-docker-images/nim as builder
# Build the executable
RUN mkdir -p '/usr/src/bitfinexdumper'
COPY ./src/ /usr/src/bitfinexdumper
WORKDIR /usr/src/bitfinexdumper
RUN nimble install -y ws asynctools sorta
RUN nim c -d:release --stackTrace:on --opt:speed -d:ssl --app:console --filenames:canonical --define:useRealtimeGC -o:/bin/bitfinex_dumper "/usr/src/bitfinexdumper/main.nim"


FROM ghcr.io/maxisoft/nim-docker-images/nim-runtime
ARG uid=1001 # the User id used to run program / create new output files
ARG gid=1001
LABEL maintainer="github.com/maxisoft" name="bitfinex dumper" description="Track and save bitfinex's order books of all available crypto pairs." url="https://github.com/maxisoft/bitfinex_dumper" vcs-url="https://github.com/maxisoft/bitfinex_dumper" org.opencontainers.image.source="https://github.com/maxisoft/bitfinex_dumper"
RUN \
    addgroup --system --gid $gid bitfinexdumper && \
    adduser --system --uid $uid --ingroup bitfinexdumper --shell /bin/sh bitfinexdumper && \
    mkdir -p "/bitfinexdumper" && \
    chown -R bitfinexdumper:bitfinexdumper "/bitfinexdumper"
COPY --from=builder /bin/bitfinex_dumper /bin/bitfinex_dumper
ENV DBPATH=/bitfinexdumper IS_DOCKER=1
VOLUME [ "/bitfinexdumper" ]
WORKDIR /bitfinexdumper
USER bitfinexdumper
ENTRYPOINT [ "/bin/bitfinex_dumper" ]
#CMD [ ]