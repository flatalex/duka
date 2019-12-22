from typing import Optional
import csv
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
        return [tick[k] for k in self.get_header()]

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
