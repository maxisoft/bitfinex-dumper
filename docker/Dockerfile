FROM ghcr.io/maxisoft/nim-docker-images/nim as builder
# Build the executable
RUN mkdir -p '/usr/src/bitfinexdumper'
COPY ./src/ /usr/src/bitfinexdumper
WORKDIR /usr/src/bitfinexdumper
RUN nimble install -y ws asynctools sorta
RUN nim c -d:release --stackTrace:on --opt:speed -d:ssl --app:console --filenames:canonical --define:useRealtimeGC --styleCheck:hint --showAllMismatches:on -o:/bin/bitfinex_dumper "/usr/src/bitfinexdumper/main.nim"


FROM ghcr.io/maxisoft/nim-docker-images/nim-runtime
ARG uid=912 # the User id used to run program / create new output files
ARG gid=912
ARG destfolder=/bitfinexdumper
ARG username=bitfinexdumper
ARG groupname=bitfinexdumper
LABEL maintainer="github.com/maxisoft" name="bitfinex dumper" description="Track and save bitfinex's order books of all available crypto pairs." url="https://github.com/maxisoft/bitfinex_dumper" vcs-url="https://github.com/maxisoft/bitfinex_dumper" org.opencontainers.image.source="https://github.com/maxisoft/bitfinex_dumper"
COPY --from=builder /bin/bitfinex_dumper /usr/bin/bitfinex-dumper
ADD --chown=$uid:$gid ./docker/start_bitfinex_dumper.sh /start_bitfinex_dumper.sh
RUN \
    apk add --no-cache su-exec shadow && \
    addgroup --system --gid $gid $groupname && \
    adduser --system --uid $uid --ingroup $groupname --shell /bin/sh $username && \
    mkdir -p "$destfolder" && \
    chown -R $uid:$gid "$destfolder" /usr/bin/bitfinex-dumper && \
    ln -s /start_bitfinex_dumper.sh /usr/bin/start_bitfinex_dumper && \
    chmod +x /start_bitfinex_dumper.sh /usr/bin/start_bitfinex_dumper /usr/bin/bitfinex-dumper && \
    ln -s /usr/bin/bitfinex-dumper /bin/bitfinex_dumper
ENV PUID=$uid PGID=$gid DBPATH=$destfolder IS_DOCKER=1 NICE_ADJUSTEMENT=5 IONICE_CLASS=3 IONICE_CLASSDATA=7 BITFINEX_DEST_FOLDER=$destfolder BITFINEX_USER_NAME=$username BITFINEX_GROUP_NAME=$groupname BITFINEX_DUMPER_EXE=/usr/bin/bitfinex-dumper
VOLUME [ "$destfolder" ]
WORKDIR "$destfolder"
ENTRYPOINT [ "/bin/sh", "/start_bitfinex_dumper.sh" ]