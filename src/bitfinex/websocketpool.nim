import std/times
import std/monotimes
import std/options
import std/lists
import ./websocket

type 
    BitFinexWebSocketFactory* = object
        url*: string
        rateLimiterFactory*: ConnectionRateLimiterFactory

    PoolEntry = object
        ws: BitFinexWebSocket
        useCounter: int64
        creationDate: MonoTime
        lastUseDate: MonoTime

    BitFinexWebSocketPool* = ref object
        factory*: BitFinexWebSocketFactory
        pool: DoublyLinkedList[PoolEntry]

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
    FRESHLY_CREATED_WEBSOCKET_LIMIT_PER_MINUTE = BITFINEX_LIMIT_CONNECTION_PER_MINUTE div 2

proc rent*(self: BitFinexWebSocketPool, useCount = 1, throwOnConnectLimit=false): BitFinexWebSocket =
    var freshlyCreatedCounter = 0
    let now = getMonoTime()
    for e in entries(self):
        if abs(now - e.creationDate) < oneHour and not e.ws.isSubscriptionFull and e.useCounter + useCount <= BITFINEX_MAX_NUMBER_OF_CHANNEL:
            inc e.useCounter, useCount
            e.lastUseDate = getMonoTime()
            return e.ws
        if abs(now - e.creationDate) < oneMinute:
            inc freshlyCreatedCounter

    if freshlyCreatedCounter >= FRESHLY_CREATED_WEBSOCKET_LIMIT_PER_MINUTE:
        if throwOnConnectLimit:
            raise Exception.newException("throwOnConnectLimit")
        assert freshlyCreatedCounter > 0
        assert not self.pool.head.isNil
        var best = self.pool.tail
        for n in nodes(self.pool):
            if n.value.useCounter < best.value.useCounter and abs(now - n.value.creationDate) < oneHour:
                best = n
        return best.value.ws
    
    self.pool.add(PoolEntry(ws: self.factory.create(), creationDate: now, lastUseDate: now))
    return self.pool.tail.value.ws

proc shouldClose(n: DoublyLinkedNode[PoolEntry]): bool {.inline.} =
    result = false
    let now = getMonoTime()
    if n.value.useCounter <= 0 and abs(now - n.value.creationDate) > 5 * oneMinute:
        return true
    if not n.value.ws.connected and abs(now - n.value.lastUseDate) > oneMinute:
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
            return
    raise Exception.newException("Trying to return a not managed websocket")

proc cleanup*(self: BitFinexWebSocketPool) =
    var stable = false
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
                break

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

