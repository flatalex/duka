from typing import Optional
import csv
import os
import re
import time
from os.path import join
from datetime import date
import psycopg2
import json

from .candle import Candle
from .utils import TimeFrame, stringify, Logger

TEMPLATE_FILE_NAME = "{}-{}_{:02d}_{:02d}-{}_{:02d}_{:02d}.csv"
DB_INSERTION_TITLE = "{}-{}_{:02d}_{:02d}-{}_{:02d}_{:02d}"


def format_float(number):
    return format(number, '.5f')


class CSVFormatter(object):
    COLUMN_TIME = 0
    COLUMN_ASK = 1
    COLUMN_BID = 2
    COLUMN_ASK_VOLUME = 3
    COLUMN_BID_VOLUME = 4


def write_tick(writer, tick):
    writer.writerow(
        {'time': tick[0],
         'ask': format_float(tick[1]),
         'bid': format_float(tick[2]),
         'ask_volume': tick[3],
         'bid_volume': tick[4]})


def write_candle(writer, candle):
    writer.writerow(
        {'time': stringify(candle.timestamp),
         'open': format_float(candle.open_price),
         'close': format_float(candle.close_price),
         'high': format_float(candle.high),
         'low': format_float(candle.low)})


class CSVDumper:
    def __init__(self, symbol, timeframe, start, end, folder, header=False):
        self.symbol = symbol
        self.timeframe = timeframe
        self.start = start
        self.end = end
        self.folder = folder
        self.include_header = header
        self.buffer = {}

    def get_header(self):
        if self.timeframe == TimeFrame.TICK:
            return ['time', 'ask', 'bid', 'ask_volume', 'bid_volume']
        return ['time', 'open', 'close', 'high', 'low']

    def unpack(self, tick):
        #return [tick[k] for k in self.get_header()]
        return tick

    def append(self, day, ticks):
        previous_key = None
        current_ticks = []
        self.buffer[day] = []
        for tick in ticks:
            if self.timeframe == TimeFrame.TICK:
                self.buffer[day].append(self.unpack(tick))
            else:
                ts = time.mktime(tick['time'].timetuple())
                key = int(ts - (ts % self.timeframe))
                if previous_key != key and previous_key is not None:
                    n = int((key - previous_key) / self.timeframe)
                    for i in range(0, n):
                        self.buffer[day].append(
                            Candle(self.symbol, previous_key + i * self.timeframe, self.timeframe, current_ticks))
                    current_ticks = []
                current_ticks.append(tick[1])
                previous_key = key

        if self.timeframe != TimeFrame.TICK:
            self.buffer[day].append(Candle(self.symbol, previous_key, self.timeframe, current_ticks))

    def dump(self):
        file_name = TEMPLATE_FILE_NAME.format(self.symbol,
                                              self.start.year, self.start.month, self.start.day,
                                              self.end.year, self.end.month, self.end.day)

        Logger.info("Writing {0}".format(file_name))

        with open(join(self.folder, file_name), 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.get_header())
            if self.include_header:
                writer.writeheader()
            for day in sorted(self.buffer.keys()):
                for value in self.buffer[day]:
                    if self.timeframe == TimeFrame.TICK:
                        write_tick(writer, value)
                    else:
                        write_candle(writer, value)

        Logger.info("{0} completed".format(file_name))


class CSVStoreDumper(object):

    DATETIME_FMT = '%Y.%m.%d %H:%M:%S.%f'
    FN_REGEX = re.compile(
        "((?P<fx>\D{6}))_Ticks_"
        "((?P<year_start>\d{4})).((?P<month_start>\d{2})).((?P<day_start>\d{2}))_"
        "((?P<year_end>\d{4})).((?P<month_end>\d{2})).((?P<day_end>\d{2})).csv"
    )

    def __init__(
            self,
            symbol,
            folder,
            file,
            last_timestamp,
    ):
        self.symbol = symbol
        self.folder = folder
        self.file = file
        self.last_timestamp = last_timestamp
        self.buffer = {}
        self.delimiter = ','

    def get_header(self):
        # consistent with jForex
        return ['Time (UTC)', 'Ask', 'Bid', 'AskVolume', 'BidVolume']

    def append(self, day, ticks):
        self.buffer[day] = []
        for tick in ticks:
            self.buffer[day].append(tick)

    def dump(self):

        days = list(self.buffer.keys())
        days.sort()

        def split(_days):
            all_years = [d.year for d in _days]
            years = set(all_years)
            if len(years) == 1:
                return _days, []
            elif len(years) == 2:
                split_at = all_years.index(max(years))
                return _days[:split_at], _days[split_at:]
            else:
                raise AssertionError(
                    "Days to add cannot touch more than two different years"
                )

        def write_line(file, tick):
            timestamp = tick[0]
            ask, bid = tick[1:3]
            ask_volume, bid_volume = 1e-6 * tick[3], 1e-6 * tick[4]
            line = self.delimiter.join(
                [
                    timestamp.strftime(self.DATETIME_FMT)[:-3],
                    f"{ask:.5f}",
                    f"{bid:.5f}",
                    f"{ask_volume:.1f}",
                    f"{bid_volume:.1f}",
                ]
            )
            file.write(f"{line}\n")

        rng1, rng2 = split(days)
        if len(rng1):
            res = self.FN_REGEX.match(self.file)
            used_fn = f"{self.symbol}_Ticks_{res['year_start']}.{res['month_start']}.{res['day_start']}_{rng1[-1].strftime('%Y.%m.%d')}.csv"
            full_path = os.path.join(self.folder, used_fn)
            # rename file to reflect new dates
            os.rename(
                os.path.join(self.folder, self.file),
                full_path
            )
            # update an existing file
            with open(full_path, mode='a', newline='') as f:
                for day in rng1:
                    for tick in self.buffer[day]:
                        timestamp = tick[0]
                        if timestamp <= self.last_timestamp:
                            continue
                        write_line(f, tick)

        if len(rng2):
            # new file
            fn = f"{self.symbol}_Ticks_{rng2[0].strftime('%Y.%m.%d')}_{rng2[-1].strftime('%Y.%m.%d')}.csv"
            full_path = os.path.join(self.folder, fn)
            with open(full_path, 'w', newline='') as f:
                header = self.delimiter.join(self.get_header())
                f.write(f"{header}\n")
                for day in rng2:
                    for tick in self.buffer[day]:
                        write_line(f, tick)


class DBWriter(CSVDumper):
    """Writes data into the database directly

    Paramters
    ---------
    symbol: str
        The symbol
    start: date
        Start date
    end: date
        End date
    """

    table = 'fx_tick_data_dukascopy'
    col_names = 'utc_timestamp, ticker, ask, bid, ask_volume, bid_volume'

    def __init__(
            self,
            symbol: str,
            start: date,
            end: date,
            connection: Optional[psycopg2.extensions.connection] = None,
            cursor: Optional[psycopg2._psycopg.cursor] = None,
    ):
        super(DBWriter, self).__init__(
            symbol, TimeFrame.TICK, start, end, 'unused', header=False,
        )
        self.connection = connection
        self.cursor = cursor

    def unpack(self, tick):
        return tick

    def dump(self):
        title = DB_INSERTION_TITLE.format(self.symbol,
                                          self.start.year, self.start.month,
                                          self.start.day,
                                          self.end.year, self.end.month,
                                          self.end.day)

        Logger.info("Writing {0}".format(title))

        data = []
        for day in sorted(self.buffer.keys()):
            for row in self.buffer[day]:
                data.append(
                    dict(
                        utc_timestamp=self.format_time(row[0]),
                        ticker=self.symbol,
                        ask=row[1],
                        bid=row[2],
                        ask_volume=row[3],
                        bid_volume=row[4],
                    )
                )

        query = (
            f"INSERT INTO {self.table} ({self.col_names})\n"
            "SELECT\n"
            f"    {self.col_names}\n"
            f"FROM json_populate_recordset(null::{self.table}, %s);"
        )

        self.cursor.execute(query, (json.dumps(data),))
        self.connection.commit()
        print("{0} completed".format(title))
        Logger.info("{0} completed".format(title))

    def format_time(self, time):
        return time.isoformat(sep=' ', timespec='milliseconds')
