import asyncdispatch
import ws
import std/sets
import std/json
import std/times
import std/monotimes
import std/tables
import std/options
import std/logging
import std/deques
import std/strutils
import asynctools/asyncsync

let logLevel = when defined(release):
    lvlInfo
else:
    lvlDebug
var logger = newConsoleLogger(logLevel, useStderr=true)

type
    SubscriptionCallback* = proc (data: JsonNode)

    Subscription = ref object
        id: int64
        subscriptionInfo: JsonNode
        heartBeatTime: MonoTime
        callback*: SubscriptionCallback

    RateLimiter = ref object
        counter: int64
        time: MonoTime

    RateLimiterFactory[T] = ref object
        limiters: OrderedTable[T, RateLimiter]

    ConnectionRateLimiterFactory* = RateLimiterFactory[string]

    BitFinexWebSocket* = ref object
        ws: WebSocket
        url: string
        connectCounter: int64
        messageCounter: int64
        errorCounter: int64
        pingTime: MonoTime
        requestStop*: bool
        running: bool
        subscriptions: OrderedTable[int64, Subscription]
        pendingSubscriptionCallbacks: OrderedTable[string, SubscriptionCallback]
        infoPayload: JsonNode
        unsubscribeQueue: OrderedSet[int64]
        buggySubscriptions: OrderedSet[int64] # sometimes the remote server won't let us unsubscribe to some open channel
        activeBuggySubscriptions: OrderedSet[int64]
        connectLock: AsyncLock
        pendingSubscriptionLock: AsyncLock
        connectAwaiters: Deque[Future[void]]
        recvFuture: Future[string]
        rateLimiterFactory: ConnectionRateLimiterFactory

func popFirst[T](self: var OrderedSet[T]): T =
    for v in self:
        self.excl(v)
        return v
    raise Exception.newException("empty queue")

func addLast[T](self: var OrderedSet[T], val: T) {.inline.} =
    incl(self, val)

proc close*(self: BitFinexWebSocket) =
    let ws = self.ws
    self.ws = nil
    if ws != nil:
        ws.hangup()
    self.subscriptions.clear()
    self.pendingSubscriptionCallbacks.clear()
    self.unsubscribeQueue.clear()
    self.buggySubscriptions.clear()
    self.activeBuggySubscriptions.clear()
    if self.recvFuture != nil:
        self.recvFuture.callback= proc () = discard
        self.recvFuture = nil

proc finalizer(self: BitFinexWebSocket) = 
    close(self)

proc newSubscription(id: int64, subscriptionInfo: JsonNode): Subscription =
    result.new()
    result.id = id
    result.subscriptionInfo = subscriptionInfo
    result.callback = nil

proc newConnectionRateLimiterFactory*(): ConnectionRateLimiterFactory =
    result.new()
    result.limiters = initOrderedTable[string, RateLimiter](0)

proc newBitFinexWebSocket*(url: string, rateLimiterFactory: ConnectionRateLimiterFactory): BitFinexWebSocket =
    when defined(gcDestructors):
        result.new()
    else:
        result.new(finalizer)
    result.url = url
    result.pingTime = getMonoTime()
    result.subscriptions = initOrderedTable[int64, Subscription]()
    result.running = false
    result.connectLock = newAsyncLock()
    result.pendingSubscriptionLock = newAsyncLock()
    result.rateLimiterFactory = rateLimiterFactory
    result.unsubscribeQueue = initOrderedSet[int64]()

proc newRateLimiter(): RateLimiter =
    result.new()
    result.counter = 0
    result.time = getMonoTime()

proc connectRateLimiter[T](self: RateLimiterFactory[T], key: T): var RateLimiter =
    if key notin self.limiters:
        self.limiters[key] = newRateLimiter()
    result = self.limiters[key]

proc connectRateLimiter*(self: ConnectionRateLimiterFactory, key: string): var RateLimiter {.inline.} =
    result = connectRateLimiter[string](self, key)

proc connectRateLimiter*(self: BitFinexWebSocket): var RateLimiter {.inline.} =
    result = connectRateLimiter(self.rateLimiterFactory, self.url)

const 
    RATE_LIMITER_SLEEP_TICK_MS = 50
    BITFINEX_LIMIT_CONNECTION_PER_MINUTE* = 20 # https://docs.bitfinex.com/docs/requirements-and-limitations#websocket-rate-limits
    RATE_LIMITER_LIMIT_CONNECTION_PER_MINUTE = BITFINEX_LIMIT_CONNECTION_PER_MINUTE
    BITFINEX_MAX_NUMBER_OF_CHANNEL* = 25 # https://docs.bitfinex.com/docs/requirements-and-limitations#websocket-rate-limits

    oneMinute = initDuration(minutes = 1)

proc waitForConnectLimit*(self: BitFinexWebSocket, limit = -1, timeout = -1) {.async.} =
    var l = limit
    if limit == -1:
        l = RATE_LIMITER_LIMIT_CONNECTION_PER_MINUTE
    var stop = false
    let start = getMonoTime()
    let timeoutDuration = initDuration(seconds = timeout)
    while not stop:
        let now = getMonoTime()
        var rl = connectRateLimiter(self)
        if now - rl.time > oneMinute:
            rl.counter = 0
            rl.time = now
        
        stop = rl.counter < l
        if timeout != -1 and now - start > timeoutDuration:
            raise Exception.newException("Timeout")
        if not stop:
            await sleepAsync(RATE_LIMITER_SLEEP_TICK_MS)

proc connect(self: BitFinexWebSocket) {.async.} =
    if self.ws.isNil or self.ws.readyState != ReadyState.Open:
        let prevWs = self.ws
        close(self)
        let ws = await newWebSocket(self.url)
        self.connectCounter += 1
        if self.ws != nil:
            if cast[pointer](prevWs) != cast[pointer](self.ws):
                logger.log(lvlWarn, "a concurrent call to connect() has been made")
            if self.ws.readyState != ReadyState.Closed:
                logger.log(lvlDebug, "closing freshly created websocket")
                try:
                    ws.hangup()
                except:
                    logger.log(lvlWarn, "close ", getCurrentExceptionMsg())
                return
        self.ws = ws
        self.pingTime = getMonoTime()
        logger.log(lvlDebug, "ws connect to ", self.url)

const channelWithSymbol = ["book", "ticker"]
const channelWithKey = ["candles", "status"]

func pendingSubscribtionIdentifier(payload: JsonNode): string =
    let channel = payload["channel"].getStr()
    result = channel
    if channel in channelWithSymbol:
        result.add payload["symbol"].getStr().toLower()
    if channel in channelWithKey:
        result.add payload["key"].getStr().toLower()
    if channel == "book":
        for k in ["prec", "freq", "len"]:
            result.add payload[k].getStr().toLower()
            result.add '_'
    
    while len(result) > 0 and result[^1] == '_':
        result.setLen(len(result) - 1)

proc unsubscribeSync*(self: BitFinexWebSocket, chanId: int64 = -1, subscription: JsonNode = nil) =
    if chanId != -1 and chanId notin self.buggySubscriptions:
        self.unsubscribeQueue.addLast(chanId)
    if subscription != nil:
        let identifier = pendingSubscribtionIdentifier(subscription)
        self.pendingSubscriptionCallbacks.del(identifier)

proc unhandledMessage(self: BitFinexWebSocket, node: JsonNode): bool {.inline.} =
    when not defined(release):
        logger.log(lvlWarn, "unhandled msg: ", node)
    if node.kind == JArray and len(node) > 0 and node{0}.kind == JInt:
        let chanId = node{0}.getBiggestInt()
        if chanId notin self.subscriptions:
            logger.log(lvlDebug, "unsubscribing to unknown channel ", chanId)
            self.unsubscribeSync(chanId)
    result = false

func isHeartBeatMessage(node: JsonNode): bool = 
    result = len(node) == 2 and node{1}.kind == JString and node{1}.getStr() == "hb"

func pendingSubscriptionsCounter*(self: BitFinexWebSocket): int {.inline.} =
    result = len(self.pendingSubscriptionCallbacks)

proc dispatch(self: BitFinexWebSocket, node: JsonNode): Future[bool] {.async.} =
    if node.kind == JObject:
        let event = node{"event"}.getStr()
        if event == "info":
            self.infoPayload = node
        elif event == "subscribed":
            let chanId = node["chanId"].getBiggestInt()
            assert chanId notin self.subscriptions
            var subscription = newSubscription(chanId, node)
            let identifier = pendingSubscribtionIdentifier(node)
            var cbFunc: SubscriptionCallback
            if not self.pendingSubscriptionCallbacks.pop(identifier, cbFunc):
                logger.log(lvlWarn, "No callback registered for ", identifier)
                self.unsubscribeSync(chanId, node)
                return false
            subscription.callback = cbFunc
            self.subscriptions[chanId] = subscription
            logger.log(lvlDebug, "new channel ", chanId, " ", identifier)
            assert self.pendingSubscriptionsCounter >= 0
            logger.log(lvlDebug, node)
            result = true
        elif event == "unsubscribed":
            self.subscriptions.del(node{"chanId"}.getBiggestInt())
        elif event == "error":
            logger.log(lvlDebug, "got error event ", event)
            let chanId = node{"chanId"}.getBiggestInt(-1)
            if node{"msg"}.getStr() == "unsubscribe: invalid":
                self.buggySubscriptions.incl(chanId)
            self.unsubscribeSync(chanId)
            var identifier = ""
            try:
                identifier = pendingSubscribtionIdentifier(node)
            except:
                discard
            if len(identifier) > 0:
                self.unsubscribeSync(subscription=node)
            
        else:
            return unhandledMessage(self, node)
            
    if node.kind == JArray:
        if node{0}.kind != JInt:
            return unhandledMessage(self, node)
        let chanId = node{0}.getBiggestInt()
        let subscription = self.subscriptions.getOrDefault(chanId, nil)
        if isHeartBeatMessage(node):
            if not subscription.isNil:
                subscription.heartBeatTime = getMonoTime()
            return true
        if subscription.isNil:
            if chanId in self.buggySubscriptions:
                self.activeBuggySubscriptions.incl(chanId)
                return false
            return unhandledMessage(self, node)

        if subscription.callback != nil:
            subscription.callback(node)
            result = true
        else:
            return unhandledMessage(self, node)

func connected*(self: BitFinexWebSocket): bool {.inline.} =
    return not self.ws.isNil and self.ws.readyState == ReadyState.Open

proc waitConnected*(self: BitFinexWebSocket) {.async.} =
    if not connected(self):
        var w = newFuture[void]("BitFinexWebSocket.waitConnected")
        self.connectAwaiters.addLast(w)
        yield w

proc notifyConnectAwaiters(self: BitFinexWebSocket) =
    if len(self.connectAwaiters) == 0:
        return
    var waiters = newSeqOfCap[Future[void]](len(self.connectAwaiters))
    while len(self.connectAwaiters) > 0:
        let w = self.connectAwaiters.popFirst()
        if not w.finished:
            waiters.add(w)

    proc completeAll() =
        for f in waiters:
            if not f.finished:
                f.complete()

    callSoon(completeAll)

proc emptyCallback(data: JsonNode) =
    logger.log(lvlWarn, "emptyCallback called with ", $data)

func buggySubscriptionCount*(self: BitFinexWebSocket): int {.inline.} =
    result = len(self.activeBuggySubscriptions)

func subscriptionCount*(self: BitFinexWebSocket): int {.inline.} =
    result = len(self.subscriptions) + self.pendingSubscriptionsCounter + buggySubscriptionCount(self)


func isSubscriptionFull*(self: BitFinexWebSocket) : bool {.inline.} =
    result = self.subscriptionCount() >= BITFINEX_MAX_NUMBER_OF_CHANNEL

proc send*(self: BitFinexWebSocket, text: string, opcode = Opcode.Text, timeoutMs = 10_000, throws=false, close=true): Future[void] {.async.} =
    var sendTask = self.ws.send(text, opcode)
    if not await withTimeout(sendTask, timeoutMs):
        logger.log(lvlWarn, "ws.send() timeout")
        if close:
            # remove any callback as we are going to throw by closing the socket
            sendTask.callback=proc() = discard
            close(self)
        if throws:
            raise Exception.newException("Websocket send timeout")


proc subscribe*(self: BitFinexWebSocket, subscription: JsonNode, callback: proc (data: JsonNode) = emptyCallback) {.async.} =
    if isSubscriptionFull(self):
        raise Exception.newException("too many subscription")
    var buff: string
    toUgly(buff, subscription)
    assert subscription.contains("event")
    assert subscription.contains("channel")
    if not connected(self):
        raise Exception.newException("not connected")
    let identifier = pendingSubscribtionIdentifier(subscription)

    proc waitForpendingSubscriptionCallbacks() {.async.} =
        let start = getMonoTime()
        while identifier in self.pendingSubscriptionCallbacks:
            await sleepAsync(50)
            if getMonoTime() - start > initDuration(seconds = 20):
                raise Exception.newException("waitForpendingSubscriptionCallbacks Timeout")

    await waitForpendingSubscriptionCallbacks()
    await self.pendingSubscriptionLock.acquire()
    try:
        await waitForpendingSubscriptionCallbacks()
        assert identifier notin self.pendingSubscriptionCallbacks
        self.pendingSubscriptionCallbacks[identifier] = callback
        try:
            logger.log(lvlDebug, "subscribing with: ", buff)
            await send(self, buff, throws=true)
        except:
            self.pendingSubscriptionCallbacks.del(identifier)
            raise
    finally:
        self.pendingSubscriptionLock.release()
    

proc subscribe*(self: BitFinexWebSocket, channel: string, event = "subscribe", subscriptionData: JsonNode = nil) {.async.} =
    var payload: JsonNode = %* {"event": event, "channel": channel}
    if subscriptionData != nil and len(subscriptionData) > 0:
        assert subscriptionData.kind == JObject
        for k, v in pairs(subscriptionData):
            payload{k} = v
    await subscribe(self, payload)

proc unsubscribe*(self: BitFinexWebSocket, chanId: int64, event = "unsubscribe", subscriptionData: JsonNode = nil) {.async.} =
    var payload: JsonNode = %* {"event": event, "chanId": chanId}
    if subscriptionData != nil and len(subscriptionData) > 0:
        assert subscriptionData.kind == JObject
        for k, v in pairs(subscriptionData):
            payload{k} = v
    var buff: string
    toUgly(buff, payload)
    assert payload.contains("event")
    assert payload.contains("chanId")
    if not connected(self):
        raise Exception.newException("not connected")
    let ws = self.ws
    if ws != nil:
        await self.pendingSubscriptionLock.acquire()
        try:
            await send(self, buff)
        finally:
            self.pendingSubscriptionLock.release()
        
    self.subscriptions.del(chanId)

const 
    WS_LOOP_SLEEP_MS = 50
    WS_PING_EVERY_MS = 30 * 1000
    WS_CONNECT_WAIT_TIME_MS = 150
    WS_MAX_ERROR_COUNTER = 32

proc restart(self: BitFinexWebSocket, keepRunning: bool = false) =
    assert self.ws.isNil or self.ws.readyState != ReadyState.Open
    self.requestStop = false
    self.running = keepRunning
    close(self)
    #self.ws = nil

func isRunning*(self: BitFinexWebSocket): bool =
    result = self.running

const pingDuration = initDuration(milliseconds = WS_PING_EVERY_MS)

proc connectSafe(self: BitFinexWebSocket) {.async.} =
    assert not connected(self)
    await waitForConnectLimit(self)
    if not self.connectLock.locked:
        await self.connectLock.acquire()
        try:
            await waitForConnectLimit(self)
            assert not connected(self)
            await connect(self)
        except WebSocketFailedUpgradeError:
            inc self.errorCounter
            close(self)
        finally:
            try:
                inc self.connectRateLimiter.counter
            finally:
                self.connectLock.release()
    else:
        if not connected(self):
            inc self.errorCounter

proc processUnsubscribeQueue(self: BitFinexWebSocket, limit = 32) {.async.} =
    var c = 0
    let cap = min(limit, len(self.unsubscribeQueue))
    if cap <= 0:
        return
    var tasks = newSeqOfCap[Future[void]](cap)
    var chanIds = newSeqOfCap[int64](cap)
    while len(self.unsubscribeQueue) > 0:
        let chanId = self.unsubscribeQueue.popFirst()
        chanIds.add chanId
        tasks.add unsubscribe(self, chanId)
        inc c
        if c >= limit:
            break
    try:
        await all tasks
    except:
        for i in tasks.low..tasks.high:
            let fut = tasks[i]
            if not fut.finished():
                yield fut # discard but the future may complete later
                self.unsubscribeQueue.addLast(chanIds[i]) # retry later
        raise

proc stop*(self: BitFinexWebSocket) =
    self.requestStop = true

proc receiveStrPacket(self: BitFinexWebSocket, ws: WebSocket, timeoutMs: int = 100): Future[string] {.async.} =
    ## Receive a string packet within timeout 
    ## No strict garanty that the task ends within timeout, just best effort
    ## Returns empty if no packet has been sent to us (or if the underlaying call returns empty)
    if self.recvFuture.isNil:
        self.recvFuture = ws.receiveStrPacket()

    let recv = self.recvFuture

    template readAndCleanup() =
        try:
            result = recv.read()
        finally:
            # check that recvFuture hasn't changed
            if cast[pointer](recv) == cast[pointer](self.recvFuture):
                self.recvFuture = nil

    if recv.finished():
        readAndCleanup()
    elif await withTimeout(recv, timeoutMs):
        if cast[pointer](recv) == cast[pointer](self.recvFuture):
            readAndCleanup()
        else:
            result = recv.read()
    elif self.recvFuture.isNil:
        self.recvFuture = recv
    elif cast[pointer](recv) != cast[pointer](self.recvFuture):
        # have to wait as another task replaced recvFuture
        # before this current task
        result = await recv
        # TODO use a queue to avoid awaiting ?
                
proc loop*(self: BitFinexWebSocket) {.async.} =
    if self.running:
        return
    while not self.requestStop:
        self.running = true
        if self.errorCounter >= WS_MAX_ERROR_COUNTER:
            raise Exception.newException("ws max error counter reached")
        let t = getMonoTime()
        if not connected(self):
            await connectSafe(self)
            await sleepAsync(WS_CONNECT_WAIT_TIME_MS)
            continue

        notifyConnectAwaiters(self)
        await processUnsubscribeQueue(self)

        let ws = self.ws
        if ws.isNil:
            continue
        try:
            if t - self.pingTime > pingDuration:
                await ws.ping()
                self.pingTime = getMonoTime()
            let data = await self.receiveStrPacket(ws)
            if len(data) > 0:
                try:
                    let node: JsonNode = parseJson(data)
                    discard await dispatch(self, node)
                except JsonParsingError:
                    logger.log(lvlWarn, "invalid json recv ", data, " ", getCurrentExceptionMsg())
                    inc self.errorCounter
                    raise
        except WebSocketClosedError:
            logger.log(lvlDebug, "websocket closed")
            try:
                if ws != nil and cast[pointer](self.ws) == cast[pointer](ws):
                    close(self)
                elif ws != nil and ws.readyState != ReadyState.Closed:
                    logger.log(lvlWarn, "closing dangling websocket")
                    ws.hangup()
            except:
                logger.log(lvlDebug, "unable to close websocket on our end")
                inc self.errorCounter
            restart(self, true)
            continue
        inc self.messageCounter
        self.errorCounter = max(self.errorCounter - 1, 0)
        if getMonoTime() - t < initDuration(milliseconds = WS_LOOP_SLEEP_MS) and not self.requestStop:
            await sleepAsync(WS_LOOP_SLEEP_MS)

    self.running = false
    close(self)

