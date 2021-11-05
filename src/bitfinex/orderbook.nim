import std/sets
import std/json
import std/jsonutils
import std/strformat
import std/tables
import std/deques
import parseutils
import sorta

type
    OrderBookEntry* = object
        price*: float64
        count*: int64
        amount*: float64

    OrderBook* = ref object
        asks: SortedTable[float64, OrderBookEntry]
        bids: SortedTable[float64, OrderBookEntry]

proc newOrderBook*(): OrderBook =
    result.new()
    result.asks = initSortedTable[float64, OrderBookEntry]()
    result.bids = initSortedTable[float64, OrderBookEntry]()

func isValid*(self: OrderBook): bool {.inline.} =
    result = len(self.asks) == len(self.bids)

func numOfEntry*(self: OrderBook): int64 =
    assert isValid(self)
    return len(self.asks)

iterator askIterator*(self: OrderBook): var OrderBookEntry =
    for v in mvalues(self.asks):
        yield v

iterator bidIterator*(self: OrderBook): var OrderBookEntry =
    for v in mvalues(self.bids):
        yield v

func parseFloat(bitfinexnode: JsonNode): float64 =
    if bitfinexnode.kind == JFloat:
        result = bitfinexnode.getFloat()
    elif bitfinexnode.kind == JInt:
        result = bitfinexnode.getBiggestInt().float64
    elif bitfinexnode.kind == JString:
        if parseBiggestFloat(bitfinexnode.getStr(), result) == 0:
            raise Exception.newException("unable to parse float")
    else:
        raise Exception.newException(fmt"unable to parse json type {bitfinexnode.kind}")

func parseInt(bitfinexnode: JsonNode): int64 =
    if bitfinexnode.kind == JInt:
        result = bitfinexnode.getBiggestInt()
    elif bitfinexnode.kind == JString:
        if parseBiggestInt(bitfinexnode.getStr(), result) == 0:
            raise Exception.newException("unable to parse int")
    else:
        raise Exception.newException(fmt"unable to parse json type {bitfinexnode.kind}")

func toJson[K, V](self: SortedTable[K, V]): JsonNode =
    result = newJObject()
    for k, v in pairs(self):
        result[k] = toJson(v)

func toJson*(orderbook: OrderBook): JsonNode =
    result = newJObject()
    var asks = newJArray()
    for k, v in pairs(orderbook.asks):
        asks.add(toJson(v))
    result["asks"] = asks

    var bids = newJArray()
    for k, v in pairs(orderbook.bids):
        bids.add(toJson(v))
    result["bids"] = bids

proc updateFromEntry*(orderbook: OrderBook, entry: OrderBookEntry) =
    # algo taken from https://docs.bitfinex.com/reference#ws-public-books
    if entry.count == 0:
        if entry.amount == 1:
            orderbook.bids.del(entry.price)
        elif entry.amount == -1:
            orderbook.asks.del(entry.price)
        else:
            raise Exception.newException(fmt"invalid entry {entry}")
    elif entry.count > 0:
        if entry.amount > 0:
            orderbook.bids[entry.price] = entry
        elif entry.amount < 0:
            orderbook.asks[entry.price] = entry
        else:
            discard # Or raise exception ?
    else:
        raise Exception.newException("negative entry.count")


proc updateFromJson*(orderbook: OrderBook, node: JsonNode) =
    assert node.kind == JArray

    proc update(e: JsonNode) =
        assert e.kind == JArray
        assert len(e) == 3
        let price = parseFloat(e{0})
        let count = parseInt(e{1})
        let amount = parseFloat(e{2})
        let entry = OrderBookEntry(price: price, count: count, amount: amount)
        updateFromEntry(orderbook, entry)

    if len(node) == 3 and node{0}.kind in [JFloat, JInt, JString]:
        update(node)
    else:
        for e in node:
            update(e)