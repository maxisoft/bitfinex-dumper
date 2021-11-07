FROM ghcr.io/maxisoft/nim-docker-images/nim as builder
# Build the executable
RUN mkdir -p '/usr/src/bitfinexdumper'
COPY ./src/ /usr/src/bitfinexdumper
WORKDIR /usr/src/bitfinexdumper
RUN nimble install -y ws asynctools sorta
RUN nim c -d:release --stackTrace:on --opt:speed -d:ssl --app:console --filenames:canonical -o:/bin/bitfinex_dumper "/usr/src/bitfinexdumper/main.nim"


FROM alpine
LABEL maintainer="github.com/maxisoft" name="bitfinex dumper" description="Track and save bitfinex's order books of all available crypto pairs." url="https://github.com/maxisoft/bitfinex_dumper" vcs-url="https://github.com/maxisoft/bitfinex_dumper" org.opencontainers.image.source="https://github.com/maxisoft/bitfinex_dumper"
RUN apk add --no-cache openssl binutils sqlite-dev ca-certificates
COPY --from=builder /bin/bitfinex_dumper /bin/bitfinex_dumper
ENTRYPOINT [ "/bin/bitfinex_dumper" ]
#CMD [ ]