import asyncdispatch
import os
import std/json
import std/monotimes
import std/db_sqlite
import std/times
import std/strutils
import std/sets
import std/algorithm
import bitfinex/websocket
import bitfinex/websocketpool
import bitfinex/databasewriter
import bitfinex/scheduler
import std/logging
import std/httpclient
import std/parseopt
import std/strformat
import std/options

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

proc initScheduler(scheduler: var JobScheduler, wsPool: BitFinexWebSocketPool, dbW: DatabaseWriter) =
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
            let job = newOrderBookCollectorJob(arg, wsPool, dbW)
            scheduler.add(job)

proc maintainWS(wsPool: BitFinexWebSocketPool, i: int64) {.async.} =
    if i mod 100 == 0:
        wsPool.cleanup()
    var tasks = newSeq[Future[void]]()
    for ws in wsPool.websockets():
        if not ws.isRunning and not ws.requestStop:
            tasks.add ws.loop()
    await all tasks

when defined(useRealtimeGC):
    const GC_MAX_PAUSE = initDuration(milliseconds=100).inMicroseconds.int
    proc GC_realtime(strongAdvice = false) {.inline.} =
        GC_step(GC_MAX_PAUSE div 3, strongAdvice)
else:
    proc GC_realtime(strongAdvice = false) {.inline.} =
        discard

proc getArgv(): string =
    when declared(paramStr) and declared(paramCount):
        for i in 1..paramCount():
            result.add(paramStr(i))
            result.add(' ')
        while len(result) > 0 and result[^1] == ' ':
            result.setLen(len(result) - 1)

type 
    CmdArg = object
        argv: string
        sqliteWal: bool
        sqliteRollingPeriod: Duration
        enableGc: bool


proc parseArgv(argv = ""): CmdArg =
    var cmd = argv
    if argv == "":
        cmd = getArgv()

    var p = initOptParser(cmd)
    result.argv = cmd
    result.sqliteWal = len(getEnv("SQLITE_WAL", "1")) > 0
    result.enableGc = true
    result.sqliteRollingPeriod = initDuration(days=7)

    template checkNoValForArgument() =
        if p.val != "":
            let msg = fmt"Argument Error: {p.key} doesn't expect a value"
            stderr.writeLine(msg)
            flushStderr()
            raise Exception.newException(msg)

    for _ in 0..1024:
        p.next()
        case p.kind
        of cmdEnd: break
        of cmdShortOption, cmdLongOption:
            let key = p.key.toLower()
            if key == "wal":
                result.sqliteWal = true
                checkNoValForArgument()
            elif key in ["nowal", "no-wal", "disable-wal"]:
                result.sqliteWal = false
                checkNoValForArgument()
            elif key in ["nogc", "no-gc", "disable-gc"]:
                result.enableGc = false
                checkNoValForArgument()
            elif key in ["dbperiod", "dbrolling"]:
                let val = p.val.toLower()
                if val in ["0", "-1", "no", "disable"]:
                    result.sqliteRollingPeriod = initDuration(0)
                elif val in ["monthly", "month"]:
                    result.sqliteRollingPeriod = initDuration(days=31)
                elif val in ["weekly", "week", "w"]:
                    result.sqliteRollingPeriod = initDuration(days=7)
                elif val in ["daily", "day", "d"]:
                    result.sqliteRollingPeriod = initDuration(days=1)
                elif val in ["hourly", "hour", "h"]:
                    result.sqliteRollingPeriod = initDuration(hours=1)
                else:
                    result.sqliteRollingPeriod = parsePeriod(p.val)
            else:
                let msg = fmt"Argument Error: unexpected key {p.key}"
                stderr.writeLine(msg)
                flushStderr()
                raise Exception.newException(msg)
        of cmdArgument:
            discard

const 
    hourly_format = "yyyy'_'MM'_'dd'__'HH" 
    daily_format = "yyyy'_'MM'_'dd"
    monthly_format = "yyyy'_'MM"

    defaultDatabaseName = "bitfinex"
    defaultDatabaseExt = "sqlite"

proc databaseName(cargs: var CmdArg): string =
    if cargs.sqliteRollingPeriod <= initDuration(0):
        return fmt"{defaultDatabaseName}.{defaultDatabaseExt}"
    var dt: DateTime = now().utc()
    var dformat = hourly_format
    var hour = dt.hour.int64 div max(cargs.sqliteRollingPeriod.inHours, 1)
    hour *= max(cargs.sqliteRollingPeriod.inHours, 1)
    hour = hour mod 24
    dt = dateTime(year=dt.year, month=dt.month, monthday=dt.monthday, hour=hour, minute=0, second=0, zone=utc())
    if cargs.sqliteRollingPeriod >= initDuration(days=30):
        dt = dateTime(year=dt.year, month=dt.month, monthday=1, hour=0, minute=0, second=0, zone=utc())
        dformat = monthly_format
    if cargs.sqliteRollingPeriod >= initDuration(days=1):
        dformat = daily_format
        var monthday = max(dt.monthday.int64 - 1, 0)
        monthday = monthday div cargs.sqliteRollingPeriod.inDays
        monthday *= cargs.sqliteRollingPeriod.inDays
        monthday = monthday mod 31
        monthday += 1
        dt = dateTime(year=dt.year, month=dt.month, monthday=monthday, hour=0, minute=0, second=0, zone=utc())
    let dtf = dt.format(dformat)
    return fmt"{defaultDatabaseName}_{dtf}.{defaultDatabaseExt}"

proc openDatabase(name: string, cargs: var CmdArg): DbConn =
    let db = open(name, "", "", "")
    try:
        if cargs.sqliteWal:
            db.exec(sql"PRAGMA journal_mode=WAL;")
        else:
            db.exec(sql"PRAGMA journal_mode=DELETE;")
    except DbError:
        db.close()
        raise
    return db

proc main() =
    var cargs = parseArgv()
    var db: Option[DbConn]
    var prevDbName = ""

    template closeDb() =
        if db.isSome():
            if prevDbName != "":
                logger.log(lvlDebug, "closing database ", prevDbName)
            if cargs.sqliteWal:
                # force the db to get back to the delete mode
                # in order to reduce the number of files
                db.get().exec(sql"PRAGMA journal_mode=DELETE;")
            db.get().close()
            db = Option[DbConn]()
        
    defer:
        closeDb()    

    let connectionRateLimiterFactory = newConnectionRateLimiterFactory()
    let dbW = newDatabaseWriter()
    let wsFactory = BitFinexWebSocketFactory(url: BITFINEX_PUBLIC_WS, rateLimiterFactory: connectionRateLimiterFactory)
    var wsPool = newBitFinexWebSocketPool(wsFactory)
    var scheduler = newJobScheduler()
    initScheduler(scheduler, wsPool, dbW)
    asyncCheck scheduler.loop()
    var i: int64 = 0
    when defined(useRealtimeGC):
        GC_setMaxPause(GC_MAX_PAUSE)
        GC_step(GC_MAX_PAUSE, true)
        if not cargs.enableGc:
            GC_disable()
            logger.log(lvlInfo, "Automatic garbage collector disabled.")
        else:
            logger.log(lvlDebug, "Automatic garbage collector enabled.")
    while true:
        GC_realtime()
        let iterTime = getMonoTime()
        if i mod 5 == 0:
            flushStderr()
        asyncCheck maintainWS(wsPool, i)
        if dbW.hasWork:
            var dbName = databaseName(cargs)
            if prevDbName != dbName or db.isNone:
                logger.log(lvlInfo, "openning database ", dbName)
                closeDb()
                db = openDatabase(dbName, cargs).some()
                prevDbName = dbName
            dbW.step(db.get(), 150)
        GC_realtime()
        waitFor sleepAsync(100)
        if getMonoTime() - iterTime < initDuration(milliseconds = 300):
            when defined(useRealtimeGC):
                GC_realtime(true)
                waitFor sleepAsync(150)
            else:
                waitFor sleepAsync(250)
        inc i

when isMainModule:
    main()