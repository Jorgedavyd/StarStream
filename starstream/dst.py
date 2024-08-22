from .utils import datetime_interval
from datetime import datetime
from dateutil.relativedelta import *
import pandas as pd
import aiofiles
import aiofiles
import asyncio
import asyncio
import os
from typing import Callable, List, Tuple

__all__ = ["Dst"]


class Dst:
    def __init__(self) -> None:
        self.root: str = "./data/Dst_index"
        self.csv_path: Callable[[str], str] = (
            lambda month: f"./data/Dst_index/{month}.csv"
        )
        os.makedirs(self.root, exist_ok=True)

    def date_to_url(self, month: str) -> str:
        if datetime.strptime(month, "%Y%m") > datetime(
            2022, 12, 31
        ):  # UPDATED FOR 2023, 2024, manually update if the page has change
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{month}/dst{month[2:]}.for.request"
        elif datetime.strptime(month, "%Y%m") > datetime(
            2016, 12, 31
        ):  # UPDATED FOR 2023, 2024, manually update if the page has change
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_provisional/{month}/dst{month[2:]}.for.request"
        else:
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_final/{month}/dst{month[2:]}.for.request"

    """
    Downloader process
    """

    def get_check_tasks(self, scrap_date: Tuple[datetime, datetime]):
        new_scrap_date: List[str] = datetime_interval(
            *scrap_date, relativedelta(months=1), "%Y%m"
        )
        self.new_scrap_date_list: List[str] = [
            month
            for month in new_scrap_date
            if self.csv_path(month)
            not in [
                os.path.join(self.root, filename) for filename in os.listdir(self.root)
            ]
        ]

    def get_download_tasks(self, session):
        return [self.download_url(month, session) for month in self.new_scrap_date_list]

    async def download_url(self, month, session):
        async with session.get(self.date_to_url(month), ssl=False) as request:
            data = await request.text()
            data = data.split("\n")
            line_lambda = (
                lambda line: ",\n".join(
                    line.replace("-", " -").replace("+", " +").split()[-24:]
                )
                + "\n"
            )
            async with aiofiles.open(self.csv_path(month), "w") as f:
                for line in data:
                    await f.write(line_lambda(line))

    """Preprocessing"""

    async def single_import(self, month) -> pd.DataFrame:
        csv = await asyncio.get_event_loop().run_in_executor(
            None, pd.read_csv, self.csv_path(month)
        )
        start_date = pd.Timestamp(
            int(month[:4]), int(month[4:6]), 1, 0
        )  # Start of the month at midnight
        end_date = start_date + pd.offsets.MonthEnd(1)
        csv.index = pd.timedelta_range(
            start_date, end_date, freq="1H", inclusive="both"
        )
        return csv

    """Main object pipeline"""

    async def downloader_pipeline(self, scrap_date, session):
        self.get_check_tasks(scrap_date)
        await asyncio.gather(*self.get_download_tasks(session))

    """Prep pipeline"""

    async def data_prep(self, scrap_date: Tuple[datetime, datetime]):
        month_scrap: List[str] = datetime_interval(
            scrap_date[0], scrap_date[-1], relativedelta(months=1), "%Y%m"
        )

        init_date = pd.to_datetime(scrap_date[0])
        last_date = pd.to_datetime(scrap_date[-1])

        csvs = await asyncio.gather(
            *[self.single_import(month) for month in month_scrap]
        )
        serie = pd.concat(csvs)
        return serie[(serie.index >= init_date) & (serie.index <= last_date)]
