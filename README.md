# Bitfinex Dumper
Track and save bitfinex's order books of all available pairs.

## Description
This project builds an orderbooks historical sqlite database.  
Such database may allow one to develop investment strategies, charts, analysis, ... without relying on external services (ie just from your own and raw data from the cex).  
This is a rewritting of a python version into a more standalone, lightweight and somewhat performant nim version. 

## Requirement
A standard working nim environment with
- recent nim version (tested with nim 1.6)
- C compiler
- nimble
- sqlite devel lib
- ssl devel lib

## Dependencies
```sh
nimble install ws asynctools sorta
```

## Compilation
```sh
nim c -d:release --stackTrace:on --opt:speed -d:ssl --app:console --filenames:canonical -o:bitfinex_dumper ./src/main.nim
```

## Usage
Start `./bitfinex_dumper` and it'll write different orderbooks deeps for every bitfinex pairs into a `bitfinex.db` sqlite database on schedule.  

One should use external restart mechanical/loop such as *systemd* to restart the soft in case of crash (eg internet disconnections)


## FAQ
### How can I configure deeps, schedule, tracked symbols ?
TODO, Schedule and deeps are hardcoded for now.
All symbols are tracked by default
### Can I edit the database schema ?
You should NOT but you can safely add indexes for faster querying
