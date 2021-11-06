FROM alpine as builder
RUN apk add --no-cache build-base wget unzip python3-dev openssl-dev alpine-sdk zlib-dev git sqlite-dev g++ curl tar xz nodejs
RUN mkdir -p '/usr/src/bitfinexdumper'
COPY ./src/ /usr/src/bitfinexdumper
WORKDIR /usr/src/bitfinexdumper
# install nim
RUN mkdir -p /nim
RUN wget -q --show-progress -O "/tmp/nim-1.6.0.tar.xz" "https://nim-lang.org/download/nim-1.6.0.tar.xz" 
RUN tar -xJf "/tmp/nim-1.6.0.tar.xz" -C /nim --strip-components=1
WORKDIR /nim
RUN sh build.sh && ln -s `pwd`/bin/nim /bin/nim
RUN nim c koch && ./koch boot -d:release && ./koch tools && ln -s `pwd`/bin/nimble /bin/nimble && ln -s `pwd`/bin/nimsuggest /bin/nimsuggest && ln -s `pwd`/bin/testament /bin/testament
# Build the executable
RUN nimble install -y ws asynctools sorta
RUN nim c -d:release --stackTrace:on --opt:speed -d:ssl --app:console --filenames:canonical -o:/bin/bitfinex_dumper "/usr/src/bitfinexdumper/main.nim"


FROM alpine
LABEL maintainer="github.com/maxisoft" name="bitfinex dumper" description="Track and save bitfinex's order books of all available crypto pairs." url="https://github.com/maxisoft/bitfinex_dumper" vcs-url="https://github.com/maxisoft/bitfinex_dumper" 
#RUN apk add --no-cache zlib openssl-dev binutils
COPY --from=builder /bin/bitfinex_dumper /bin/bitfinex_dumper
ENTRYPOINT [ "/bin/bitfinex_dumper" ]
#CMD [ ]