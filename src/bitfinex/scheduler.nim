import asyncdispatch
import std/json
import std/times
import std/monotimes
import std/strformat
import std/strutils
import std/parseutils
import std/heapqueue
import std/options
import std/logging
import std/hashes
import ./databasewriter
import ./websocket
import ./orderbook
import ./websocketpool

let logLevel = when defined(release):
    lvlInfo
else:
    lvlDebug
var logger = newConsoleLogger(logLevel, useStderr=true)

type 
    BaseJob {.inheritable.} = ref object of RootObj
        dueTime: MonoTime
        errorCounter: int64

    OrderBookCollectorJobArgument* = object
        symbol*: string
        precision*: string
        frequency*: string
        length*: int

        resamplePeriod*: string
        debounceTimeMs*: int

    OrderBookCollectorWebSocketCallback = object
        identifier: string
        expectedOrderBookLength: int
        dbWritter: DatabaseWriter
        ws: BitFinexWebSocket
        orderbook: OrderBook
        debounceTime: MonoTime
        debounceTimeLimit: Duration
        finalizer: proc(success: bool)

    OrderBookCollectorJob* = ref object of BaseJob
        args: OrderBookCollectorJobArgument
        wsPool: BitFinexWebSocketPool
        dbWritter: DatabaseWriter
        
        resamplePeriod: Duration
        subscriptionPayload: JsonNode
        callback: Option[OrderBookCollectorWebSocketCallback]
        callBackVersion: int64
        

func hash*(x: OrderBookCollectorJobArgument): Hash =
    ## Compute the hash of an OrderBookCollectorJobArgument.
    ## Note that we ignore resamplePeriod and debounceTimeMs.
    result = x.symbol.hash !& x.precision.hash !& x.frequency.hash !& x.length.hash
    result = !$result

func `==`*(left: OrderBookCollectorJobArgument, right: OrderBookCollectorJobArgument): bool =
    ## Is two OrderBookCollectorJobArgument equals ?
    ## Note that we ignore resamplePeriod and debounceTimeMs.
    if left.symbol != right.symbol:
      return false
    if left.precision != right.precision:
      return false
    if left.frequency != right.frequency:
      return false
    if left.length != right.length:
      return false
    return true

func parseInt(node: JsonNode): int64 =
    if node.kind == JInt:
        result = node.getBiggestInt()
    elif node.kind == JString:
        if parseBiggestInt(node.getStr(), result) == 0:
            raise Exception.newException("unable to parse int")
    else:
        raise Exception.newException(fmt"unable to parse json type {node.kind}")

proc invoke(self: var OrderBookCollectorWebSocketCallback, node: JsonNode) =
    var ok = false
    try:
        self.orderbook.updateFromJson(node{1})
        let now = getMonoTime()
        if unlikely(not self.orderbook.isValid):
            return
        if unlikely(self.orderbook.numOfEntry != self.expectedOrderBookLength):
            return
        if abs(now - self.debounceTime) < self.debounceTimeLimit:
            return
        self.dbWritter.insertOrderBook(self.orderbook, self.identifier)
        self.debounceTime = now
        ok = true
    finally:
        self.ws.unsubscribeSync(parseInt(node{0}))
        if self.finalizer != nil:
            self.finalizer(ok)

func parsePeriod*(period: string): Duration =
    if unlikely(len(period) < 2):
        raise Exception.newException("not a valid period")
    let last: char = period[^1]
    let tmp = period[0..^2]
    assert len(tmp) == len(period) - 1
    var value: int64
    if unlikely(parseBiggestInt(tmp, value) == 0):
        raise Exception.newException("not a valid period")
    result = (case last
        of 's': initDuration(seconds = value)
        of 'm': initDuration(minutes = value)
        of 'h': initDuration(hours = value)
        of 'd': initDuration(days = value)
        of 'w': initDuration(weeks = value)
        of 'M': initDuration(days = 30 * value)
        else: raise Exception.newException("not a valid period")
    )
        
proc newOrderBookCollectorJob*(arguments: OrderBookCollectorJobArgument, wsPool: BitFinexWebSocketPool, dbWritter: DatabaseWriter): OrderBookCollectorJob =
    result.new()
    assert arguments.debounceTimeMs >= 0
    result.args = arguments
    result.wsPool = wsPool
    result.dbWritter = dbWritter

    result.resamplePeriod = parsePeriod(arguments.resamplePeriod)
    assert arguments.debounceTimeMs <= result.resamplePeriod.inMilliseconds

func `<`(a, b: BaseJob): bool = a.dueTime < b.dueTime

method perform(this: BaseJob) {.base async.} =
    raise Exception.newException("must be implemented")

method incrementDueTime(this: BaseJob) {.base.} =
    raise Exception.newException("must be implemented")

func identifer(self: OrderBookCollectorJobArgument): string =
    let symbol = self.symbol.replace(":", "")
    result = fmt"{symbol}_{self.resamplePeriod}_{self.precision}_{self.frequency}_l{self.length}"

const 
    SCHEDULER_MIN_SLEEP_TICK = 250
    SCHEDULER_JOB_ACTIVE_SLOT = 8
    SCHEDULER_LOOP_YIELD_EVERY_N_TASK = 16
    JOB_MAX_ERROR_COUNTER = 1024


method perform(this: OrderBookCollectorJob) {.async.} =
    let identifier = this.args.identifer()
    var ws = this.wsPool.rent()
    var returnWs = true
    defer:
        if returnWs and ws != nil:
            this.wsPool.`return`(ws)
            ws = nil
    
    if unlikely(not ws.connected or ws.isSubscriptionFull()):
        # reschedule the task asap
        this.dueTime = max(getMonoTime(), this.dueTime) + initDuration(milliseconds = SCHEDULER_MIN_SLEEP_TICK)
        if not ws.connected:
            inc this.errorCounter
            if this.errorCounter >= JOB_MAX_ERROR_COUNTER:
                raise Exception.newException("Job max error counter reached")
        return

    if unlikely(this.subscriptionPayload.isNil):
        this.subscriptionPayload = %* {"event": "subscribe",
        "channel": "book",
        "symbol": this.args.symbol,
        "prec": this.args.precision,
        "freq": this.args.frequency,
        "len": $(this.args.length)}

    var callBackVersion: int64
    
    template versionMatch(): bool = this.callBackVersion == callBackVersion

    template withMatchingVersion(body: untyped) =
        if versionMatch():
            body

    proc finalizer(success: bool) =
        if unlikely(not success):
            inc this.errorCounter
            if ws != nil:
                ws.unsubscribeSync(-1, this.subscriptionPayload)
        else:
            this.errorCounter = 0
            inc this.callBackVersion # we are done with this callback

        if ws != nil:
            this.wsPool.`return`(ws)
            ws = nil

    block:
        let callback = OrderBookCollectorWebSocketCallback(
            identifier: identifier,
            dbWritter: this.dbWritter,
            ws: ws,
            orderbook: newOrderBook(),
            debounceTimeLimit: initDuration(milliseconds = this.args.debounceTimeMs),
            expectedOrderBookLength: this.args.length,
            finalizer: finalizer
        )

        inc this.callBackVersion
        callBackVersion = this.callBackVersion
        shallowCopy(this.callback, callback.some())

    proc cb(node: JsonNode) =
        var invoked = false
        withMatchingVersion:
            if this.callback.isSome:
                invoke(this.callback.get(), node)
                invoked = true

        if not invoked:
            var chanId: int64 = -1
            if node.kind == JArray and len(node) > 0 and node{0}.kind == JInt:
                chanId = parseInt(node{0})
            if ws != nil:
                ws.unsubscribeSync(chanId, this.subscriptionPayload)

    ws.unsubscribeSync(-1, this.subscriptionPayload)
    await ws.subscribe(this.subscriptionPayload, cb)
    returnWs = false

method incrementDueTime(this: OrderBookCollectorJob) =
    this.dueTime = max(this.dueTime, getMonoTime()) + this.resamplePeriod

type 
    JobScheduler* = ref object
        queue: HeapQueue[BaseJob]

proc newJobScheduler*(): JobScheduler =
    result.new()
    result.queue = initHeapQueue[BaseJob]()

proc add*(self: JobScheduler, job: BaseJob) =
    self.queue.push(job)
    
proc loop*(self: JobScheduler) {.async.} =
    var c = 0
    while len(self.queue) > 0:
        let now = getMonoTime()
        let cap = min(SCHEDULER_JOB_ACTIVE_SLOT, len(self.queue))
        var jobs = newSeqOfCap[(BaseJob, MonoTime, Future[void])](cap)
        try:
            for i in 0..<cap:
                let first = self.queue[0]
                if first.dueTime > now:
                    continue
                var item = self.queue.pop()
                try:
                    jobs.add((item, item.dueTime, item.perform()))
                except:
                    logger.log(lvlError, "got error while starting job ", getCurrentExceptionMsg())
                    self.queue.push(item)
                    continue
            
            if len(jobs) == 0:
                let first = self.queue[0]
                let sleepDuration = min(SCHEDULER_MIN_SLEEP_TICK, (first.dueTime - now).inMilliseconds)
                await sleepAsync(sleepDuration.float)
                c = 0
            else:
                var exceptions = newSeqOfCap[ref Exception](len(jobs))
                for (job, prevDueTime, future) in jobs:
                    inc c
                    if c >= SCHEDULER_LOOP_YIELD_EVERY_N_TASK:
                        await sleepAsync(0.0) # allow current task to yield
                        c = 0
                    try:
                        await future
                    except:
                        exceptions.add getCurrentException()
                    if job.dueTime == prevDueTime:
                        job.incrementDueTime()
                if len(exceptions) == 1:
                    raise exceptions[0]
                elif len(exceptions) > 0:
                    # TODO custom aggregate exceptions and better message
                    raise Exception.newException(fmt"there was {len(exceptions)} errors")
        finally:
            for (job, _, _) in jobs:
                self.queue.push(job)
