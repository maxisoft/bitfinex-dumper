import std/json
import std/strformat
import std/tables
import std/strbasics
import std/re
import std/macros

const TIME_COLUMN_NAME = "time"
const ASK_PRICE_COLUMN_NAME = "ask_price"
const ASK_COUNT_COLUMN_NAME = "ask_count"
const ASK_AMOUNT_COLUMN_NAME = "ask_amount"
const BID_PRICE_COLUMN_NAME = "bid_price"
const BID_COUNT_COLUMN_NAME = "bid_count"
const BID_AMOUNT_COLUMN_NAME = "bid_amount"
const ORDERBOOK_NUMBER_OF_ENTRY_FIELDS = ["PRICE", "COUNT", "AMOUNT"]
const ORDERBOOK_NUMBER_OF_FIELDS_PER_ENTRY = len(ORDERBOOK_NUMBER_OF_ENTRY_FIELDS)

func rstrip(s: var string, c: char=' ') {.inline.} =
    while len(s) > 0 and s[^1] == c:
        s.setLen(len(s) - 1)

let sqlIdentifierRe = re(r"^[a-zA-Z0-9_]+$")

proc validSqlIdentifer(identifier: string): bool {.inline.} =
    result = match(identifier, sqlIdentifierRe)

macro callGenCol(prefix: static[string]) =
    result = newCall(ident"genCol")
    result.add ident"result"
    for field in ORDERBOOK_NUMBER_OF_ENTRY_FIELDS:
        result.add ident(prefix & "_" & field & "_COLUMN_NAME")
    result.add ident"orderBookLength"

proc createOrderBookTableCreateQuery*(identifier: string, orderBookLength: int = 25): string =
    assert validSqlIdentifer(identifier)
    result = fmt"""
CREATE TABLE IF NOT EXISTS "bitfinex_{identifier}" (
    "{TIME_COLUMN_NAME}" INTEGER UNIQUE NOT NULL,
"""

    func genCol(buff: var string, price: static[string], count: static[string], amount: static[string], limit: int) =
        for i in 1..limit:
            buff.add fmt"""
    "{price}_{i}" REAL NOT NULL,
    "{count}_{i}" INTEGER NOT NULL,
    "{amount}_{i}" REAL NOT NULL,
"""

    callGenCol("ASK")
    callGenCol("BID")

    result.add fmt"""
    PRIMARY KEY("{TIME_COLUMN_NAME}")
)
"""

proc createOrderBookInsertQuery*(identifier: string, orderBookLength: int = 25): string = 
    assert validSqlIdentifer(identifier)
    result = fmt"""
INSERT INTO "bitfinex_{identifier}" (
    "{TIME_COLUMN_NAME}",
"""

    func genCol(buff: var string, price: static[string], count: static[string], amount: static[string], limit: int) {.inline.} =
        for i in 1..limit:
            buff.add fmt""" "{price}_{i}", "{count}_{i}", "{amount}_{i}",
"""

    callGenCol("ASK")
    callGenCol("BID")
    strip(result, leading = false)
    rstrip(result, ',')
    result.add fmt""")
VALUES (?, """
    for i in 0..<(orderBookLength * 2 * ORDERBOOK_NUMBER_OF_FIELDS_PER_ENTRY):
        result.add "?, "
    strip(result, leading = false)
    rstrip(result, ',')
    result.add fmt""")
"""


proc createOrderBookUpdateQuery*(identifier: string, orderBookLength: int = 25): string = 
    assert validSqlIdentifer(identifier)
    result = fmt"""
UPDATE "bitfinex_{identifier}" 
SET 
"""

    func genCol(buff: var string, price: static[string], count: static[string], amount: static[string], limit: int) {.inline.} =
        for i in 1..limit:
            buff.add fmt""" "{price}_{i}" = ?, "{count}_{i}" = ?, "{amount}_{i}"= ? ,
"""

    callGenCol("ASK")
    callGenCol("BID")
    strip(result, leading = false)
    rstrip(result, ',')
    result.add fmt"""
WHERE {TIME_COLUMN_NAME} = ? """

proc createOrderBookSelectQuery*(identifier: string, orderBookLength: int = 25): string = 
    assert validSqlIdentifer(identifier)
    func genCol(buff: var string, price: static[string], count: static[string], amount: static[string], limit: int) =
        for i in 1..limit:
            buff.add fmt"""
    "{price}_{i}",
    "{count}_{i}",
    "{amount}_{i}",
"""

    result = fmt"""
SELECT
    "{TIME_COLUMN_NAME}",
""" 
    callGenCol("ASK")
    callGenCol("BID")
    strip(result, leading = false)
    rstrip(result, ',')

    result.add fmt"""
FROM "bitfinex_{identifier}""""

    

    