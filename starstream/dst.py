from starstream._base import CSV
from .utils import (
    StarDate,
    handle_client_connection_error,
)
from typing import List, Tuple
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import os.path as osp
import polars as pl
import aiofiles

__all__ = ["Dst"]

def last_december() -> datetime:
    return datetime(datetime.today().year - 1, 12, 31)

class Dst(CSV):
    def __init__(self, download_path: str =  "./data/Dst", batch_size: int = 10) -> None:
        super().__init__(
            root_path = download_path,
            batch_size = batch_size,
            csv_path = lambda date: osp.join(download_path, f"{date}.csv"),
            date_sampling = relativedelta(months = 1),
            format = '%Y%m',
        )

    def _date_to_url(self, month: str) -> str:
        date = datetime.strptime(month, "%Y%m")
        last_decem = last_december()

        if date > last_decem:
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/{month}/dst{month[2:]}.for.request"

        elif date > datetime(last_decem.year - 3, 12, 31):
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_provisional/{month}/dst{month[2:]}.for.request"

        else:
            return f"https://wdc.kugi.kyoto-u.ac.jp/dst_final/{month}/dst{month[2:]}.for.request"

    async def _check_tasks(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
        return await super()._check_tasks(scrap_date)

    @handle_client_connection_error(default_cooldown=5, max_retries=3, increment="exp")
    async def _download_url(self, session, month: StarDate) -> None:
        async with session.get(self._date_to_url(month.str()), ssl=False) as request:
            data = await request.text()
            data = data.split("\n")[:-2]
            out_list: List[str] = []

            for line in data:
                out_list.extend(
                    line.replace("-", " -").replace("+", " +").split()[-24:]
                )

            out_list.insert(0, "dst_index")

            async with aiofiles.open(self.csv_path(month.str()), "w") as f:
                for line in out_list:
                    await f.write(line + ",\n")

    def _get_df_unit(self, date: str) -> pl.DataFrame:
        df = pl.read_csv(self.csv_path(date)).get_column("dst_index")
        start_date: datetime = datetime(int(date[:4]), int(date[4:6]), 1)
        end_date: datetime = start_date + relativedelta(months=1) - timedelta(hours=1)  ## todo
        full_range = pl.DataFrame({"date": pl.datetime_range(start=start_date, end=end_date, interval='1h', eager = True)})
        return full_range.with_columns(df)
