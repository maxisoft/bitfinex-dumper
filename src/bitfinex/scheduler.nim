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

    OrderBookCollectorWebSocketCallback = ref object
        identifier: string
        expectedOrderBookLength: int
        dbWritter: DatabaseWriter
        ws: BitFinexWebSocket
        orderbook: OrderBook
        debounceTime: MonoTime
        debounceTimeLimit: Duration
        finalizer: proc(success: bool)
        callCounter: int64

    OrderBookCollectorJob* = ref object of BaseJob
        args: OrderBookCollectorJobArgument
        wsPool: BitFinexWebSocketPool
        dbWritter: DatabaseWriter
        
        resamplePeriod: Duration
        subscriptionPayload: JsonNode
        

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

proc invoke(self: OrderBookCollectorWebSocketCallback, node: JsonNode) =
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
        inc self.callCounter

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
    SCHEDULER_JOB_ACTIVE_SLOT = 4 * 1024 #BITFINEX_MAX_NUMBER_OF_CHANNEL * BITFINEX_LIMIT_CONNECTION_PER_MINUTE * 8 div 10
    SCHEDULER_LOOP_YIELD_EVERY_N_TASK = 32
    SCHEDULER_JOB_TIMEOUT_MS = 60 * 1000
    JOB_MAX_ERROR_COUNTER = 1024


method perform(this: OrderBookCollectorJob) {.async.} =
    let identifier = this.args.identifer()
    var ws = await this.wsPool.rentAsync()
    var returnWs = true
    var cbCalled = newFuture[bool]("OrderBookCollectorJob.perform.cbCalled")
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

    proc unsubscribe() =
        if ws != nil:
            ws.unsubscribeSync(-1, this.subscriptionPayload)

    proc finalizer(success: bool) =
        if not cbCalled.finished():
            cbCalled.complete(success)
        if unlikely(not success):
            inc this.errorCounter
            unsubscribe()
        else:
            this.errorCounter = 0

    let callback = OrderBookCollectorWebSocketCallback(
        identifier: identifier,
        dbWritter: this.dbWritter,
        ws: ws,
        orderbook: newOrderBook(),
        debounceTimeLimit: initDuration(milliseconds = this.args.debounceTimeMs),
        expectedOrderBookLength: this.args.length,
        finalizer: finalizer
    )

    proc cb(node: JsonNode) =
        if callback != nil:
            callback.invoke(node)
        if callback.isNil or callback.callCounter == 1:
            var chanId: int64 = -1
            if node.kind == JArray and len(node) > 0 and node{0}.kind == JInt:
                chanId = parseInt(node{0})
            if ws != nil:
                ws.unsubscribeSync(chanId, this.subscriptionPayload)

    await ws.subscribe(this.subscriptionPayload, cb)
    discard await withTimeout(cbCalled, 30_000)
    if cbCalled.finished() and cbCalled.read():
        logger.log(lvlDebug, "completed subscribtion")
    else:
        logger.log(lvlDebug, "unable to complete subscribtion")
        unsubscribe()
        this.dueTime = max(getMonoTime(), this.dueTime) + initDuration(milliseconds = SCHEDULER_MIN_SLEEP_TICK)
       

var runningMonoTimeDiffMs: int64 = 0
const 
    MilliToNanoSeconds = convert(Milliseconds, Nanoseconds, 1.int64)
    SecondToMilliseconds = convert(Seconds, Milliseconds, 1.int64)
    runningMonoTimeDiffExponentialMovingAverageSpan: int64 = 10

proc roundMonotime(dueTime: MonoTime, resamplePeriod: Duration, shift_ns: int64 = -100e+9.int64): MonoTime =
    let now = getMonoTime()
    let t = getTime()
    let timestamp = t.toUnixFloat() * SecondToMilliseconds.float
    var diff = timestamp.int64 - (now.ticks div MilliToNanoSeconds)

    if runningMonoTimeDiffMs == 0:
        runningMonoTimeDiffMs = diff
    diff = (runningMonoTimeDiffExponentialMovingAverageSpan - 1) * runningMonoTimeDiffMs + 1 * diff
    diff = diff div runningMonoTimeDiffExponentialMovingAverageSpan
    runningMonoTimeDiffMs = diff
    diff *= MilliToNanoSeconds

    let resampleNs = resamplePeriod.inNanoseconds
    let dtTicks = dueTime.ticks
    var ns = (dtTicks + diff) div resampleNs
    ns *= resampleNs
    if abs(ns - dtTicks) > resampleNs div 2 and ns < dtTicks:
        ns += resampleNs
    while ns - dtTicks < resampleNs div 2:
        ns += resampleNs
    ns -= diff
    ns += shift_ns
    result = MonoTime() + initDuration(nanoseconds=ns)
    while result - now < initDuration(0):
        result = result + resamplePeriod


method incrementDueTime(this: OrderBookCollectorJob) =
    var dueTime = max(this.dueTime, getMonoTime()) + this.resamplePeriod
    this.dueTime = roundMonotime(dueTime, this.resamplePeriod)
    #logger.log(lvlInfo, fmt"schedule in {this.dueTime - getMonoTime()}")

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
                let sleepDuration = min(SCHEDULER_MIN_SLEEP_TICK, abs(first.dueTime - now).inMilliseconds)
                await sleepAsync(sleepDuration.float)
                c = 0
            else:
                var exceptions = newSeqOfCap[ref Exception](len(jobs))
                var futures = newSeqOfCap[Future[void]](len(jobs))
                for (_, _, future) in jobs:
                    futures.add(future)
                var inTime: bool
                try:
                    inTime = await withTimeout(all futures, SCHEDULER_JOB_TIMEOUT_MS)
                except:
                    discard
                if not inTime:
                    raise Exception.newException("SCHEDULER_JOB_TIMEOUT")
                for (job, prevDueTime, future) in jobs:
                    inc c
                    if c >= SCHEDULER_LOOP_YIELD_EVERY_N_TASK:
                        await sleepAsync(0.0) # allow current task to yield
                        c = 0
                    if future.finished():
                        try:
                            future.read()
                        except:
                            exceptions.add getCurrentException()
                        if job.dueTime == prevDueTime:
                            job.incrementDueTime()
                if len(exceptions) == 1:
                    raise exceptions[0]
                elif len(exceptions) > 0:
                    # TODO custom aggregate exceptions and better message
                    raise Exception.newException(fmt"there was {len(exceptions)} errors", parentException=exceptions[0])
        finally:
            for (job, _, _) in jobs:
                self.queue.push(job)
        if (getMonoTime() - now).inMilliseconds < SCHEDULER_MIN_SLEEP_TICK:
            await sleepAsync(SCHEDULER_MIN_SLEEP_TICK)
