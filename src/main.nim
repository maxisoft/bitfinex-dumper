import asyncdispatch
import std/json
import std/monotimes
import std/db_sqlite
import std/times
import std/strutils
import std/sets
import std/algorithm
import bitfinex/websocket
import bitfinex/databasewriter
import bitfinex/scheduler
import std/logging
import std/httpclient


const BITFINEX_PUBLIC_WS = "wss://api-pub.bitfinex.com/ws/2"

let logLevel = when defined(release):
    lvlInfo
else:
    lvlDebug
var logger = newConsoleLogger(logLevel, useStderr=true)
var saveStdErrPos {.threadvar.}: int64

proc flushStderr(f: File = stderr): bool {.discardable.} =
    if saveStdErrPos == -1:
        flushFile(stderr)
        return true
    try:
        let p = getFilePos(f)
        if p != saveStdErrPos:
            flushFile(stderr)
            saveStdErrPos = getFilePos(f)
            result = true
    except IOError:
        # getFilePos(stderr) throws on linux
        saveStdErrPos = -1

proc listPairs(): seq[string] =
    var client = newHttpClient(timeout = 30_000)
    defer:
        client.close()
    let c = client.getContent("https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange")
    let j = parseJson(c)
    assert j.kind == JArray
    assert len(j) == 1
    assert j{0}.kind == JArray
    for p in j{0}:
        assert p.kind == JString
        result.add p.getStr()

proc initScheduler(scheduler: var JobScheduler, ws: BitFinexWebSocket, dbW: DatabaseWriter) =
    var pairs = listPairs()
    sort(pairs)
    logger.log(lvlInfo, "tracking ", len(pairs), " pairs")
    let args = [("p1", "5m"), ("p2", "10m"), ("p3", "1h")]
    var dejaVu = initOrderedSet[OrderBookCollectorJobArgument]()
    for (precision, resamplePeriod) in args:
        for pair in pairs:
            let arg = OrderBookCollectorJobArgument(symbol: "t" & pair, precision: precision, frequency: "f1", length: 25, resamplePeriod: resamplePeriod, debounceTimeMs: 1000)
            if arg in dejaVu:
                # prevent duplicate job argument as it's not handled by the websocket subscribe logics
                raise Exception.newException("duplicate job")
            dejaVu.incl(arg)
            let job = newOrderBookCollectorJob(arg, ws, dbW)
            scheduler.add(job)

proc main() =
    let db = open("bitfinex.db", "", "", "")
    defer:
        db.close()
    let connectionRateLimiterFactory = newConnectionRateLimiterFactory()
    let ws = newBitFinexWebSocket(BITFINEX_PUBLIC_WS, connectionRateLimiterFactory)
    let dbW = newDatabaseWriter(db)
    asyncCheck ws.loop()
    waitFor ws.waitConnected()
    var scheduler = newJobScheduler()
    initScheduler(scheduler, ws, dbW)
    asyncCheck scheduler.loop()
    while ws.isRunning:
        let iterTime = getMonoTime()
        flushStderr()
        waitFor ws.waitConnected()
        if dbW.hasWork:
            dbW.step(150)
        waitFor sleepAsync(100)
        if getMonoTime() - iterTime < initDuration(milliseconds = 300):
            waitFor sleepAsync(250)

when isMainModule:
    main()