from typing import List, Optional, Union, Dict
import concurrent
import threading
import time
from collections import deque
from datetime import timedelta, date, datetime
import psycopg2

from ..core import decompress, fetch_day, Logger
from ..core.csv_dumper import CSVDumper, DBWriter, CSVStoreDumper
from ..core.utils import is_debug_mode, TimeFrame, Destination

SATURDAY = 5
day_counter = 0

MAP_WRITER = {Destination.CSV: CSVDumper, Destination.DB: DBWriter}


class FakeFuture(concurrent.futures.Future):

    def __init__(self, fn, *args, **kwargs):
        super(FakeFuture, self).__init__()
        self._state = 'FINISHED'
        try:
            self._result = fn(*args, **kwargs)
        except Exception as e:
            self._result = e

    def result(self, **kwargs):
        # if code failed raise exception like real future
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


class FakeExecutor(concurrent.futures.ProcessPoolExecutor):

    def submit(self, fn, *args, **kwargs):
        return FakeFuture(fn, *args, **kwargs)


def build_writer(
        destination: Destination,
        symbol: str,
        timeframe: TimeFrame,
        start: date,
        end: date,
        folder: Optional[str] = None,
        include_header: Optional[bool] = None,
        connection: Optional[psycopg2.extensions.connection] = None,
        cursor: Optional[psycopg2._psycopg.cursor] = None,
        table_template: Optional[str] = None,
        file: Optional[str] = None,
        last_timestamp: Optional[datetime] = None,
) -> Union[CSVDumper, DBWriter]:
    if destination == Destination.CSV:
        return CSVDumper(symbol, timeframe, start, end, folder, include_header)
    elif destination == Destination.CSV_STORE:
        return CSVStoreDumper(symbol, folder, file, last_timestamp)
    else:
        return DBWriter(symbol, start, end, connection, cursor, table_template)
    

def days(start: date, end: date):
    if start > end:
        return
    end = end + timedelta(days=1)
    today = date.today()
    while start != end:
        if start.weekday() != SATURDAY and start != today:
            yield start
        start = start + timedelta(days=1)


def format_left_time(seconds):
    if seconds < 0:
        return "--:--:--"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)


def update_progress(done, total, avg_time_per_job, threads):
    progress = 1 if total == 0 else done / total
    progress = int((1.0 if progress > 1.0 else progress) * 100)
    remainder = 100 - progress
    estimation = (avg_time_per_job * (total - done) / threads)
    if not is_debug_mode():
        print('\r[{0}] {1}%  Left : {2}  '.format('#' * progress + '-' * remainder, progress,
                                                  format_left_time(estimation)), end='')


def how_many_days(start, end):
    return sum(1 for _ in days(start, end))


def avg(fetch_times):
    if len(fetch_times) != 0:
        return sum(fetch_times) / len(fetch_times)
    else:
        return -1


def name(symbol, timeframe, start, end):
    ext = ".csv"

    for x in dir(TimeFrame):
        if getattr(TimeFrame, x) == timeframe:
            ts_str = x

    name = symbol + "_" + ts_str + "_" + str(start)

    if start != end:
        name += "_" + str(end)

    return name + ext


def app(
        symbols: List[str],
        start: date,
        end: date,
        threads: int,
        timeframe: TimeFrame,
        destination: Destination,
        folder: Optional[str] = None,
        header: Optional[bool] = None,
        connection: Optional[psycopg2.extensions.connection] = None,
        cursor: Optional[psycopg2._psycopg.cursor] = None,
        table_template: Optional[str] = None,
        files: Optional[Dict[str, str]] = None,
        last_timestamps: Optional[Dict[str, datetime]] = None
):
    """Scrap data from Dukascopy

    Parameters
    ----------
    symbols: list[str]
        List of symbols recognized by Dukascopy.
        FX pairs are in the format CCY1CCY2 (no slash)
    start: date
        The inclusive start date for downloading data
    end: date
        The inclusive end date for downloading data
    threads: int
        Number of threads to be used. It seems that only threads=1 is valid
    timeframe: TimeFrame
        Specify whether to dump tick data or candles formed from tick data
    destination: Destination
        Specify whether to dump data in new files, in a database, or update
        existing files instead
    folder: str (optional)
        Where to dump or update files. Not needed if destination == DB
    header: str
        Only used if Destination == CSV. Specifies whether files should
        have a header
    connection: psycopg2.extensions.connection
        Only used if destination == DB.
        Opened connection to the database
    cursor: cursor
        Only used if destination == DB.
        Cursor on the opened connection
    table_template: str (optional, default=None)
        Only used if destination == DB.
        The template to be used to form the table name
    files: Dict[str, str]
        Only used if destination == CSV_STORE. These are the files containing
        the more recent data for each symbol.
    last_timestamps: Dict[str, datetime]
        Only used if destination == CSV_STORE.
        The last timestamp in each of the above files
    """
    if start > end:
        return
    lock = threading.Lock()
    global day_counter
    total_days = how_many_days(start, end)

    if total_days == 0:
        return

    last_fetch = deque([], maxlen=5)
    update_progress(day_counter, total_days, -1, threads)

    if files is None:
        files = {}
    if last_timestamps is None:
        last_timestamps = {}

    def do_work(
            symbol: str,
            day,
            writer
    ):
        global day_counter
        star_time = time.time()
        Logger.info("Fetching day {0}".format(day))
        try:
            writer.append(day, decompress(symbol, day, fetch_day(symbol, day)))
        except Exception as e:
            print("ERROR for {0}, {1} Exception : {2}".format(day, symbol, str(e)))
        elapsed_time = time.time() - star_time
        last_fetch.append(elapsed_time)
        with lock:
            day_counter += 1
        Logger.info("Day {0} fetched in {1}s".format(day, elapsed_time))
        print(f"Day {day} fetched in {elapsed_time}s for {symbol}")

    futures = []

    if threads == 1:
        Executor = FakeExecutor
    else:
        Executor = concurrent.futures.ThreadPoolExecutor

    with Executor(max_workers=threads) as executor:

        writers = {
            symbol: build_writer(
                destination, symbol, timeframe, start, end, folder, header,
                connection, cursor, table_template, files.get(symbol, None),
                last_timestamps.get(symbol, None),
            )
            for symbol in symbols
        }

        for symbol in symbols:
            for day in days(start, end):
                futures.append(executor.submit(do_work, symbol, day, writers[symbol]))

        for future in concurrent.futures.as_completed(futures):
            if future.exception() is None:
                update_progress(day_counter, total_days, avg(last_fetch), threads)
            else:
                Logger.error("An error happen when fetching data : ", future.exception())

        print("Fetching data terminated")
        Logger.info("Fetching data terminated")
        for writer in writers.values():
            writer.dump()
            print(f"Writing data for {writer.symbol} terminated")

    update_progress(day_counter, total_days, avg(last_fetch), threads)
