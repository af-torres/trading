import schedule
from typing import TypedDict, Literal
from abc import ABC, abstractmethod, ab
import yfinance as yf
from pandas import DataFrame
import datetime


class JobPayload(TypedDict):
    Ticker: str
    Ctx: dict


class Signal(TypedDict):
    Ticker: str
    JobName: str
    Output: Literal['buy', 'sell', 'hold', 'not_enough_info']


class Job(ABC):
    @abstractmethod
    def _get_payload(self) -> JobPayload:
        pass

    @abstractmethod
    def build(self, schedule: schedule):
        pass

    @abstractmethod
    def execute(self) -> Signal:
        pass


class Last8MondaysOpen(Job):
    def __init__(self, ticker: str) -> None:
        self.ticker: yf.Ticker = yf.Ticker(ticker)

        self.data = self.__preload()

    def __preload(self) -> DataFrame:
        data: DataFrame = self.ticker.history(period='1mo', interval='1d')

        mondays_in_data = []

        day: datetime.date
        for day in data.index:
            if day.weekday() == 1:
                mondays_in_data.append(day)

        data.filter(items=mondays_in_data, axis=0)

        return data

    def build(self, schedule: schedule):
        schedule.every().monday.at('9:00').do(
            print_signal(self.execute))

    def __update_data(self):
        new_data = self.ticker.history(
            period='1d', interval='1d', start=datetime.date.today().strftime("%Y-%m-%d"))

        self.data = self.data.concat(self.data, new_data)

    def _get_payload(self) -> JobPayload:
        self.__update_data()

        ctx = dict(data=self.data)

        return JobPayload(self.ticker.ticker, ctx)

    def execute(self) -> Signal:
        payload = self._get_payload()

        if payload.__getitem__('context').__getattribute__('data').shape[0] < 8:
            return Signal(self.ticker.ticker, type(self).__name__, 'not_enough_info')

        # TODO: fix signal logic

        return Signal(self.ticker.ticker, type(self).__name__, 'buy')


def print_signal(execute):
    print(execute())
