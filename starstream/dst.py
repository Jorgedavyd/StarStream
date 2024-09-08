from .utils import datetime_interval, handle_client_connection_error
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import aiofiles
import asyncio
import asyncio
import os
import os.path as osp
from typing import Callable, Coroutine, List, Tuple
from tqdm import tqdm

__all__ = ["Dst"]


class Dst:
    def __init__(self, download_path: str = "./data/Dst", batch_size: int = 10) -> None:
        self.batch_size: int = batch_size
        self.root: str = download_path
        self.csv_path: Callable[[str], str] = lambda month: osp.join(
            self.root, f"{month}.csv"
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
        print(f"{self.__class__.__name__}: Looking for missing data...")
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
        print(f"{self.__class__.__name__}: Downloading missing data...")
        return [self.download_url(month, session) for month in self.new_scrap_date_list]

    @handle_client_connection_error(default_cooldown=5, max_retries=3, increment="exp")
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

        downloading_tasks: List[Coroutine] = self.get_download_tasks(session)

        for i in tqdm(
            range(0, len(downloading_tasks), self.batch_size),
            desc=f"Downloading for {self.__class__.__name__}...",
        ):
            await asyncio.gather(*downloading_tasks[i : i + self.batch_size])

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
