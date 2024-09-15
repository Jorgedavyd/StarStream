from .utils import DataDownloading, datetime_interval, handle_client_connection_error, timedelta_to_freq
from typing import Callable, Coroutine, List, Tuple
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import os.path as osp
from tqdm import tqdm
import pandas as pd
import aiofiles
import asyncio
import asyncio
import os

__all__ = ["Dst"]

def last_december() -> datetime:
    return datetime(datetime.today().year - 1, 12, 31)

class Dst:
    def __init__(self, download_path: str = "./data/Dst", batch_size: int = 10) -> None:
        self.batch_size: int = batch_size
        self.root: str = download_path
        self.csv_path: Callable[[str], str] = lambda month: osp.join(
            self.root, f"{month}.csv"
        )
        os.makedirs(self.root, exist_ok=True)

    def date_to_url(self, month: str) -> str:
        date=datetime.strptime(month,"%Y%m")
        last_decem=last_december()

        if date > last_decem:
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{month}/dst{month[2:]}.for.request"

        elif date > datetime(last_decem.year - 3, 12, 31):
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
            data = data.split("\n")[:-2]
            out_list: List[str] = []

            for line in data:
                out_list.extend(
                    line.replace("-", " -").replace("+", " +").split()[-24:]
                )

            out_list.insert(0, 'dst_index')

            async with aiofiles.open(self.csv_path(month), "w") as f:
                for line in out_list:
                    await f.write(line + ',\n')

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

    def get_df_unit(self, date: str) -> pd.Series:
        df = pd.read_csv(self.csv_path(date))['dst_index']
        start_date = datetime(int(date[:4]), int(date[4:6]), 1)
        end_date = start_date + relativedelta(months=1) - timedelta(hours=1)
        full_range = pd.date_range(start=start_date, end=end_date, freq='1h')
        df.index = full_range
        return df

    def get_df(self, scrap_date: Tuple[datetime, datetime]) -> pd.Series:
        month_scrap: List[str] = datetime_interval(
            scrap_date[0], scrap_date[-1], relativedelta(months=1), "%Y%m"
        )
        return pd.concat([self.get_df_unit(date) for date in month_scrap])

    def data_prep(self, scrap_date: Tuple[datetime, datetime], step_size: timedelta):
        assert (timedelta(hours = 1) <= step_size), "Not valid step_size, must be greater than 1 hour"

        init_date = pd.to_datetime(scrap_date[0])
        last_date = pd.to_datetime(scrap_date[-1])

        serie = self.get_df(scrap_date)

        return serie[(serie.index >= init_date) & (serie.index <= last_date)] \
                .interpolate() \
                .resample(timedelta_to_freq(step_size)) \
                .mean()
