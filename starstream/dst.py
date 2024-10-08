from starstream._base import Satellite
from .utils import StarInterval, datetime_interval, handle_client_connection_error, timedelta_to_freq
from typing import Callable, Coroutine, List, Tuple, Union
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import os.path as osp
from tqdm import tqdm
import polar as pl
import aiofiles
import asyncio
import asyncio
import os

__all__ = ["Dst"]

def last_december() -> datetime:
    return datetime(datetime.today().year - 1, 12, 31)

class Dst(Satellite):
    def __init__(self, download_path: str = "./data/Dst", batch_size: int = 10) -> None:
        self.batch_size: int = batch_size
        self.root: str = download_path
        self.csv_path: Callable[[str], str] = lambda month: osp.join(
            self.root, f"{month}.csv"
        )
        os.makedirs(self.root, exist_ok=True)

    def date_to_url(self, month: str) -> str:
        date = datetime.strptime(month, "%Y%m")
        last_decem = last_december()

        if date > last_decem:
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{month}/dst{month[2:]}.for.request"

        elif date > datetime(last_decem.year - 3, 12, 31):
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_provisional/{month}/dst{month[2:]}.for.request"

        else:
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_final/{month}/dst{month[2:]}.for.request"

    def _check_tasks(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
        new_scrap_date: StarInterval = StarInterval(
            scrap_date,
            relativedelta(months = 1),
            '%Y%m'
        )
        history: List[str] = [os.path.join(self.root, filename) for filename in os.listdir(self.root)]

        for month in new_scrap_date:
            if self.csv_path(month.str()) not in history:
                self.new_scrap_date_list.append(month)

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

            out_list.insert(0, "dst_index")

            async with aiofiles.open(self.csv_path(month), "w") as f:
                for line in out_list:
                    await f.write(line + ",\n")

    async def fetch(self, scrap_date: Union[List[Tuple[datetime, datetime]],Tuple[datetime, datetime]], session):
        if isinstance(scrap_date[0], datetime):
            self._check_tasks([scrap_date])
        else:
            self._check_tasks(scrap_date)
        downloading_tasks: List[Coroutine] = self._get_download_tasks(session)
        for i in tqdm(
            range(0, len(downloading_tasks), self.batch_size),
            desc=f"Downloading for {self.__class__.__name__}...",
        ):
            await asyncio.gather(*downloading_tasks[i : i + self.batch_size])

    def _get_df_unit(self, date: str) -> pl.Series:
        df = pl.read_csv(self.csv_path(date)).get_column("dst_index")
        start_date = datetime(int(date[:4]), int(date[4:6]), 1)
        end_date = start_date + relativedelta(months=1) - timedelta(hours=1) ## todo
        full_range = pl.date_range(start=start_date, end=end_date, freq="1h")
        df.index = full_range
        return df
