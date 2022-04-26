#!/usr/bin/env python3
"""
This script compress oldest bitfinex databases into multiple zip files.
Usage: ./compress_databases.py path/to/databases
"""
from asyncio import as_completed
import importlib
import re
import sys
import os
import zipfile
import logging
import time
import sqlite3

from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

from typing import Any, Callable, Collection, List, Union

from injector import T

logger = logging.getLogger("bitfinex.databases.compress")


def _compression_mode():
    if os.environ.get("COMPRESS_DEFLATE", False) not in ('', '0', 'false', 'no', False, None):
        return zipfile.ZIP_DEFLATED
    if os.environ.get("COMPRESS_BZIP2", False) not in ('', '0', 'false', 'no', False, None):
        return zipfile.ZIP_BZIP2

    try:
        lzma = bool(importlib.util.find_spec('lzma'))
    except ImportError:
        lzma = False

    return zipfile.ZIP_LZMA if lzma else zipfile.ZIP_DEFLATED

def compress_file(p: Path):
    assert p.is_file()
    with zipfile.ZipFile(
        str(p) + ".zip", "w", compression=_compression_mode()
    ) as out:
        out.write(str(p.resolve()), p.name)


def list_file(p: Path, patterns=("**/*.sqlite",)):
    deja_vu = set()
    for pattern in patterns:
        for sub in p.glob(pattern):
            if sub not in deja_vu:
                deja_vu.add(sub)
                yield sub


_db_pattern = re.compile(
    r"^bitfinex(?:_|\s)(?P<year>\d+)(?:_|\s)(?P<month>\d+)(?:_|\s)(?P<day>\d+)(?:(?:_|\s)*\d+)*?\.sqlite(?:(?:-wal)|(?:-shm)?)$",
    re.UNICODE | re.IGNORECASE,
)


def filter_file_time(p: Path):
    m = re.match(_db_pattern, p.name)
    if m:
        date = datetime(**{k: int(m.group(k)) for k in ("year", "month", "day")})
        if datetime.utcnow() - date > timedelta(days=31):
            stat = p.stat()
            if time.time() - stat.st_mtime > timedelta(days=7).total_seconds():
                return True
    return False


def _set_journal_mode(db_path: Path, journal_mode='DELETE'):
    stat = db_path.stat()
    try:
        uri = f'{db_path.resolve().as_uri()}?mode=ro'
        with sqlite3.connect(uri, uri=True) as db:
            current_mode = db.execute('''PRAGMA journal_mode''')
            current_mode = current_mode.fetchone()[0]
        if current_mode.upper() == journal_mode.upper():
            return
        try:
            with sqlite3.connect(str(db_path), uri=True) as db:
                db.execute(f'''PRAGMA journal_mode={journal_mode};''')
        except:
            pass
    finally:
        if db_path.stat().st_mtime != stat.st_mtime:
            os.utime(str(db_path), (stat.st_atime, stat.st_mtime))

def _compress_task_fn(path):
    print(f"compressing {path} ...")
    _set_journal_mode(path)
    compress_file(path)
    return path


def _walk(path: Path, collect_fn: Callable[[Path], Any]):
    if not path.exists():
        logger.warning("%s doesn't exists", path)
        return tuple()
    path = path.resolve()
    if path.is_dir():
        for sub in list_file(path):
            yield from _walk(sub, collect_fn)
    elif filter_file_time(path):
        yield collect_fn(path)
    return tuple()
    

def main(*paths: Collection[Union[str, Path]]):
    with ThreadPoolExecutor(
        os.cpu_count(), thread_name_prefix="compressor"
    ) as executor:
    
        def submit(path: Path):
            return executor.submit(_compress_task_fn, path)

        def cleanup(fut: Future):
            if fut and fut.done():
                path: Path = fut.result()
                if path.is_file():
                    path.unlink()

        try:
            tasks: List[Future] = []
            task: Future
            for path in map(Path, paths):
                for fut in _walk(path, submit):
                    tasks.append(fut)
                    tasks[-1].add_done_callback(cleanup)

            for task in as_completed(tasks):
                ex = task.exception()
                if isinstance(ex, KeyboardInterrupt):
                    task.result()
        except KeyboardInterrupt:
            for task in tasks:
                task.cancel()
            executor.shutdown(wait=False)
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv):
        main(*sys.argv)
    else:
        main(Path())
