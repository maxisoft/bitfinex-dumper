import std/db_sqlite
import asyncdispatch
import std/sets
import std/deques
import std/json
import std/times
import std/monotimes
import std/sequtils
import std/tables
import std/options
import std/logging
import std/heapqueue
import std/strbasics
import locks
import ./orderbook
import ./database


let logLevel = when defined(release):
    lvlInfo
else:
    lvlDebug
var logger = newConsoleLogger(logLevel, useStderr=true)

type
    DatabaseWriterContext* = object
        identifier: string
        time: int64
        createTable: bool

    DatabaseWriterJob {.inheritable.} = ref object of RootObj
        startTime: int64
        ctx: DatabaseWriterContext

    OrderBookJob = ref object of DatabaseWriterJob
        orderbook: OrderBook
        update: bool

    DatabaseWriter* = ref object
        queue: HeapQueue[DatabaseWriterJob]
        insertTimeHistory: OrderedTable[string, int64]
        prevDb: pointer

proc newDatabaseWriter*(): DatabaseWriter =
    result.new()
    result.queue = initHeapQueue[DatabaseWriterJob]()
    result.insertTimeHistory = initOrderedTable[string, int64]()

proc insertOrderBook*(self: DatabaseWriter, orderbook: OrderBook, identifier: string, startTime: int64 = -1) =
    var st = startTime
    if st == -1:
        st = (now().utc.toTime.toUnixFloat * 1000).toBiggestInt
    
    let savedTime = self.insertTimeHistory.getOrDefault(identifier, -1)
    let ctx = DatabaseWriterContext(identifier: identifier, time: st, createTable: savedTime == -1)
    var job = OrderBookJob(startTime: st, ctx: ctx, orderbook: orderbook)
    job.update = savedTime == st
    self.queue.push(job)

func `<`*(a, b: DatabaseWriterJob): bool {.inline.} = 
    result = a.startTime < b.startTime

method perform(this: DatabaseWriterJob, db: DbConn) {.base.} =
    raise Exception.newException("need to be implemented")

proc insert(this: OrderBookJob, db: DbConn) =
    let orderBookLength = this.orderbook.numOfEntry.int
    assert orderBookLength > 0
    if this.ctx.createTable:
        logger.log(lvlDebug, "trying to create table for ", this.ctx.identifier)
        discard db.execAffectedRows(createOrderBookTableCreateQuery(this.ctx.identifier, orderBookLength = orderBookLength).sql)
    let query = createOrderBookInsertQuery(this.ctx.identifier, orderBookLength = orderBookLength)
    
    var args = newSeqOfCap[string](this.orderbook.numOfEntry * 2 * 3 + 1)
    args.add $(this.ctx.time)

    template addWithIt(it: untyped) =
        for entry in it:
            args.add $(entry.price)
            args.add $(entry.count)
            args.add $(entry.amount)

    addWithIt(this.orderbook.askIterator)
    addWithIt(this.orderbook.bidIterator)

    assert query.count('?') == args.len()
    let rowid = db.insertID(query.sql, args)
    assert rowid > 0

proc update(this: OrderBookJob, db: DbConn) =
    let orderBookLength = this.orderbook.numOfEntry.int
    assert orderBookLength > 0
    let query = createOrderBookUpdateQuery(this.ctx.identifier, orderBookLength = orderBookLength)
    
    var args = newSeqOfCap[string](this.orderbook.numOfEntry * 2 * 3 + 1)

    template addWithIt(it: untyped) =
        for entry in it:
            args.add $(entry.price)
            args.add $(entry.count)
            args.add $(entry.amount)

    addWithIt(this.orderbook.askIterator)
    addWithIt(this.orderbook.bidIterator)
    args.add $(this.ctx.time)

    assert query.count('?') == args.len()
    let affectedCount = db.execAffectedRows(query.sql, args)
    assert affectedCount > 0

method perform(this: OrderBookJob, db: DbConn) =
    if this.update:
        update(this, db)
    else:
        insert(this, db)

func hasWork*(self: DatabaseWriter): bool {.inline.} =
    result = len(self.queue) > 0

proc step*(self: DatabaseWriter, conn: DbConn, maxTimeMs: int = 100) =
    let startDate = getMonoTime()
    let maxTimeDuration = initDuration(milliseconds = maxTimeMs)
    var createTable = cast[pointer](conn) != self.prevDb
    if createTable:
        self.insertTimeHistory.clear()
    createTable = createTable and hasWork(self)
    while hasWork(self) and getMonoTime() - startDate < maxTimeDuration:
        let first = self.queue.pop()
        first.ctx.createTable = first.ctx.createTable or createTable
        first.perform(conn)
        self.insertTimeHistory[first.ctx.identifier] = max(first.ctx.time, self.insertTimeHistory.getOrDefault(first.ctx.identifier, 0))
    
    if createTable:
        self.prevDb = cast[pointer](conn)

