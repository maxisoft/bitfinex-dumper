import asyncdispatch
import std/times
import std/monotimes
import std/options
import std/lists
import std/deques
import ./websocket

type 
    BitFinexWebSocketFactory* = object
        url*: string
        rateLimiterFactory*: ConnectionRateLimiterFactory

    PoolEntry = ref object
        ws: BitFinexWebSocket
        useCounter: int64
        creationDate: MonoTime
        lastUseDate: MonoTime

    BitFinexWebSocketPool* = ref object
        factory: BitFinexWebSocketFactory
        awaiters: Deque[Future[void]]
        pool: DoublyLinkedList[PoolEntry]
    
    ConnectLimitError* = object of CatchableError

proc newBitFinexWebSocketPool*(factory: BitFinexWebSocketFactory): BitFinexWebSocketPool =
    result.new()
    result.factory = factory
    result.awaiters = initDeque[Future[void]]()
    result.pool = initDoublyLinkedList[PoolEntry]()

proc create*(self: var BitFinexWebSocketFactory): BitFinexWebSocket =
    newBitFinexWebSocket(self.url, self.rateLimiterFactory)

iterator websockets*(self: BitFinexWebSocketPool): var BitFinexWebSocket =
    for item in mitems(self.pool):
        yield item.ws

iterator entries*(self: BitFinexWebSocketPool): var PoolEntry =
    for item in mitems(self.pool):
        yield item

const 
    oneMinute = initDuration(minutes = 1)
    oneHour = initDuration(hours = 1)
    FRESHLY_CREATED_WEBSOCKET_LIMIT_PER_MINUTE* = BITFINEX_LIMIT_CONNECTION_PER_MINUTE div 2

proc effectiveUseCounter(e: var PoolEntry): int64 {.inline.} =
    ## Use counter of a pool entry that account for buggySubscriptionCount
    result = e.useCounter + e.ws.buggySubscriptionCount

proc rent*(self: BitFinexWebSocketPool, useCount = 1, throwOnConnectLimit=false): BitFinexWebSocket =
    var freshlyCreatedCounter = 0
    let now = getMonoTime()
    for n in nodes(self.pool):
        if abs(now - n.value.creationDate) < oneHour and n.value.effectiveUseCounter() == n.value.useCounter and not n.value.ws.isSubscriptionFull and n.value.effectiveUseCounter() + useCount < BITFINEX_MAX_NUMBER_OF_CHANNEL:
            inc n.value.useCounter, useCount
            n.value.lastUseDate = now
            # move current node to the end of the list
            # in order to mimic a priority queue
            let cpy = n.value
            self.pool.remove(n)
            self.pool.add(cpy)
            return self.pool.tail.value.ws
        if abs(now - n.value.creationDate) < oneMinute:
            inc freshlyCreatedCounter

    if freshlyCreatedCounter >= FRESHLY_CREATED_WEBSOCKET_LIMIT_PER_MINUTE:
        if throwOnConnectLimit:
            raise ConnectLimitError.newException("throwOnConnectLimit")
        assert freshlyCreatedCounter > 0
        assert not self.pool.head.isNil
        var best = self.pool.tail
        for n in nodes(self.pool):
            if n.value.effectiveUseCounter < best.value.effectiveUseCounter and abs(now - n.value.creationDate) < oneHour:
                best = n
        inc best.value.useCounter, useCount
        let cpy = best.value
        self.pool.remove(best)
        self.pool.add(cpy)
        return self.pool.tail.value.ws
    
    self.pool.add(PoolEntry(ws: self.factory.create(), creationDate: now, lastUseDate: now))
    inc self.pool.tail.value.useCounter, useCount
    return self.pool.tail.value.ws

proc rentAsync*(self: BitFinexWebSocketPool): Future[BitFinexWebSocket] {.async.}=
    while true:
        try:
            return self.rent(1, throwOnConnectLimit = true)
        except ConnectLimitError:
            var w = newFuture[void]("BitFinexWebSocketPool.rentAsync")
            self.awaiters.addLast(w)
            yield w

proc notify(self: BitFinexWebSocketPool, n = 1) =
    if len(self.awaiters) == 0 or n <= 0:
        return
    var waiters = newSeqOfCap[Future[void]](n)
    while len(self.awaiters) > 0 and len(waiters) < n:
        let w = self.awaiters.popFirst()
        if not w.finished:
            waiters.add(w)

    proc completeAll() =
        for f in waiters:
            if not f.finished:
                f.complete()
    
    callSoon(completeAll)

proc shouldClose(n: DoublyLinkedNode[PoolEntry]): bool {.inline.} =
    result = false
    let now = getMonoTime()
    if n.value.useCounter <= 0 and n.value.effectiveUseCounter != n.value.useCounter and abs(now - n.value.creationDate) > oneMinute:
        return true
    if not n.value.ws.connected and abs(now - n.value.creationDate) > oneMinute:
        return true
    if abs(now - n.value.lastUseDate) > 2 * oneHour: # no connections should last 2hr
        return true

proc `return`*(self: BitFinexWebSocketPool, ws: BitFinexWebSocket, useCount = 1) =
    for n in nodes(self.pool):
        if cast[pointer](n.value.ws) == cast[pointer](ws):
            dec n.value.useCounter, useCount
            n.value.useCounter = max(n.value.useCounter, 0)
            if shouldClose(n):
                ws.stop()
                ws.close()
                self.pool.remove(n)
            elif n.value.useCounter == 0:
                let cpy = n.value
                self.pool.remove(n)
                self.pool.prepend(cpy)
            notify(self)
            return
    raise Exception.newException("Trying to return a not managed websocket")

proc cleanup*(self: BitFinexWebSocketPool) =
    var stable = false
    var c = 0
    while not stable:
        stable = true
        for n in nodes(self.pool):
            if shouldClose(n):
                let ws = n.value.ws
                if ws.isNil:
                    continue
                ws.stop()
                ws.close()
                self.pool.remove(n)
                stable = false
                inc c
                break
    
    stable = false
    while len(self.awaiters) > 0 and not stable:
        stable = true
        let first = self.awaiters[0]
        if first.finished:
            self.awaiters.popFirst()
            stable = false
        let last = (if len(self.awaiters) > 0: self.awaiters[^1] else: first)
        if cast[pointer](first) == cast[pointer](last):
            break
        if last.finished:
            self.awaiters.popLast()
            stable = false

    notify(self, c)


template withWebSocket*(self: BitFinexWebSocketPool, code: untyped) =
    let ws {.inject.} = self.rent()
    try:
        code
    finally:
        self.`return`(ws)

template withWebSocket*(self: BitFinexWebSocketPool, useCount: int, code: untyped) =
    let ws {.inject.} = self.rent(useCount=useCount)
    try:
        code
    finally:
        self.`return`(ws, useCount=useCount)

