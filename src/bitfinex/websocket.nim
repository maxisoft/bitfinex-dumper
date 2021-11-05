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
import std/locks
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
        lock: Lock

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
        unsubscribeQueue: Deque[int64]
        connectLock: AsyncLock
        pendingSubscriptionLock: AsyncLock
        connectAwaiters: Deque[Future[void]]
        rateLimiterFactory: ConnectionRateLimiterFactory

proc close*(self: BitFinexWebSocket) =
    if self.ws != nil:
        self.ws.close()
    self.subscriptions.clear()
    self.pendingSubscriptionCallbacks.clear()
    self.unsubscribeQueue.clear()

proc finalizer[T](self: RateLimiterFactory[T]) = 
    deinitLock(self.lock)

proc finalizer(self: BitFinexWebSocket) = 
    close(self)

proc newSubscription(id: int64, subscriptionInfo: JsonNode): Subscription =
    result.new()
    result.id = id
    result.subscriptionInfo = subscriptionInfo
    result.callback = nil

proc newConnectionRateLimiterFactory*(): ConnectionRateLimiterFactory =
    when defined(gcDestructors):
        result.new()
    else:
        result.new(proc (x: ConnectionRateLimiterFactory) = finalizer[string](x))
    result.limiters = initOrderedTable[string, RateLimiter](0)
    initLock(result.lock)

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
    result.unsubscribeQueue = initDeque[int64]()

proc newRateLimiter(): RateLimiter =
    result.new()
    result.counter = 0
    result.time = getMonoTime()

proc connectRateLimiter[T](self: RateLimiterFactory[T], key: T): var RateLimiter =
    if key notin self.limiters:
        withLock(self.lock):
            if key notin self.limiters:
                # copy on write mechanism
                var copy = initOrderedTable[string, RateLimiter](len(self.limiters) + 1)
                for k, v in pairs(self.limiters):
                    copy[k] = v
                copy[key] = newRateLimiter()
                shallowCopy(self.limiters, copy)
    result = self.limiters[key]

proc connectRateLimiter(self: BitFinexWebSocket): var RateLimiter {.inline.} =
    result = connectRateLimiter[string](self.rateLimiterFactory, self.url)

const 
    RATE_LIMITER_SLEEP_TICK_MS = 50
    RATE_LIMITER_LIMIT_CONNECTION_PER_MINUTE = 20 # https://docs.bitfinex.com/docs/requirements-and-limitations#websocket-rate-limits
    MAX_NUMBER_OF_CHANNEL = 25 # https://docs.bitfinex.com/docs/requirements-and-limitations#websocket-rate-limits

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
        close(self)
        self.ws = await newWebSocket(self.url)
        self.pingTime = getMonoTime()
        self.connectCounter += 1
        logger.log(lvlDebug, "ws connect to ", self.url)

proc unhandledMessage(self: BitFinexWebSocket, node: JsonNode): bool {.inline.} =
    logger.log(lvlWarn, "unhandled msg: ", node)
    result = false

func isHeartBeatMessage(node: JsonNode): bool = 
    result = len(node) == 2 and node{1}.kind == JString and node{1}.getStr() == "hb"

func pendingSubscriptionsCounter*(self: BitFinexWebSocket): int {.inline.} =
    result = len(self.pendingSubscriptionCallbacks)

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
                raise Exception.newException("No callback registered for " & identifier)
            subscription.callback = cbFunc
            self.subscriptions[chanId] = subscription
            logger.log(lvlDebug, "new channel ", chanId, " ", identifier)
            assert self.pendingSubscriptionsCounter >= 0
            logger.log(lvlDebug, node)
            result = true
        elif event == "unsubscribed":
            self.subscriptions.del(node{"chanId"}.getBiggestInt())
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

func subscriptionCount*(self: BitFinexWebSocket): int =
    return len(self.subscriptions) + self.pendingSubscriptionsCounter

proc subscribe*(self: BitFinexWebSocket, subscription: JsonNode, callback: proc (data: JsonNode) = emptyCallback) {.async.} =
    if subscriptionCount(self) >= MAX_NUMBER_OF_CHANNEL:
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
    finally:
        self.pendingSubscriptionLock.release()
        
    try:
        logger.log(lvlDebug, "subscribing with: ", buff)
        await self.ws.send(buff)
    except:
        self.pendingSubscriptionCallbacks.del(identifier)
        raise
    

proc subscribe*(self: BitFinexWebSocket, channel: string, event = "subscribe", subscriptionData: JsonNode = nil) {.async.} =
    var payload: JsonNode = %* {"event": event, "channel": channel}
    if subscriptionData != nil and len(subscriptionData) > 0:
        assert subscriptionData.kind == JObject
        for k, v in pairs(subscriptionData):
            payload{k} = v
    await subscribe(self, payload)

proc unsubscribeSync*(self: BitFinexWebSocket, chanId: int64, subscription: JsonNode = nil) =
    if chanId != -1:
        self.unsubscribeQueue.addLast(chanId)
    if subscription != nil:
        let identifier = pendingSubscribtionIdentifier(subscription)
        self.pendingSubscriptionCallbacks.del(identifier)

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

    await self.ws.send(buff)
    self.subscriptions.del(chanId)

const 
    WS_LOOP_SLEEP_MS = 50
    WS_PING_EVERY_MS = 30 * 1000
    WS_CONNECT_WAIT_TIME_MS = 150

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
        finally:
            try:
                self.connectRateLimiter.counter += 1
            finally:
                self.connectLock.release()
    else:
        if not connected(self):
            self.errorCounter += 1

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
                
proc loop*(self: BitFinexWebSocket) {.async.} =
    while not self.requestStop:
        self.running = true
        let t = getMonoTime()
        if not connected(self):
            await connectSafe(self)
            await sleepAsync(WS_CONNECT_WAIT_TIME_MS)
            continue

        notifyConnectAwaiters(self)            
        await processUnsubscribeQueue(self)

        assert self.ws != nil
        try:
            if t - self.pingTime > pingDuration:
                await self.ws.ping()
                self.pingTime = getMonoTime()
            let data = await self.ws.receiveStrPacket()
            if len(data) > 0:
                try:
                    let node: JsonNode = parseJson(data)
                    discard await dispatch(self, node)
                except JsonParsingError:
                    logger.log(lvlWarn, "invalid json recv ", data)
                    self.errorCounter += 1
                    raise
        except WebSocketClosedError:
            logger.log(lvlWarn, "websocket closed")
            try:
                if self.ws != nil:
                    self.ws.close()
            except:
                logger.log(lvlDebug, "unable to close websocket on our end")
            restart(self, true)
            continue
        self.messageCounter += 1
        if getMonoTime() - t < initDuration(milliseconds = WS_LOOP_SLEEP_MS) and not self.requestStop:
            await sleepAsync(WS_LOOP_SLEEP_MS)

    self.running = false
    close(self)

