from starstream._base import CSV
from starstream._utils import (
    download_url_prep,
    StarDate,
    handle_client_connection_error,
)
from starstream.typing import ScrapDate
from typing import List
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import polars as pl
import aiofiles
from itertools import chain

__all__ = ["Dst"]


def last_december() -> datetime:
    return datetime(datetime.today().year - 1, 12, 31)


class Dst(CSV):
    def __init__(self, root: str = "./data/Dst", batch_size: int = 10) -> None:
        super().__init__(
            root=root,
            batch_size=batch_size,
            date_sampling=relativedelta(months=1),
            format="%Y%m",
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

    def _interval_setup(self, scrap_date: ScrapDate) -> None:
        super()._interval_setup(scrap_date)
        self.urls = [self._date_to_url(month.str()) for month in self.dates]
        self.paths = [self.filepath(month.str()) for month in self.dates]

    async def _scrap_(self, idx: int) -> None:
        _ = idx

    @handle_client_connection_error(default_cooldown=5, max_retries=3, increment="exp")
    async def _download_(self, idx: int) -> None:
        await download_url_prep(self, idx, self._on_download_prep, idx)

    async def _prep_(self, idx: int) -> None:
        _ = idx

    async def _on_download_prep(self, data, idx: int) -> None:
        date: StarDate = self.dates[idx]
        data = data.read().decode("utf-8").split("\n")
        data = list(map(lambda x: x.replace("-", " -").replace("+", " +"), data))
        data = list(map(lambda x: x.split(), data))
        value: List = list(
            map(float, chain.from_iterable([sample[3:-1] for sample in data[:-3]]))
        )
        value.insert(0, "dst_index")
        value = "\n".join(list(map(lambda x: str(x) + ",", value)))
        async with aiofiles.open(self.filepath(date.str()), "w") as f:
            await f.write(value)

    def _get_df_unit(self, date: str) -> pl.DataFrame:
        df = pl.read_csv(self.filepath(date)).get_column("dst_index")
        start_date: datetime = datetime(int(date[:4]), int(date[4:6]), 1)
        end_date: datetime = start_date + relativedelta(months=1) - timedelta(hours=1)
        full_range = pl.DataFrame(
            {
                "date": pl.datetime_range(
                    start=start_date, end=end_date, interval="1h", eager=True
                )
            }
        )
        return full_range.with_columns(df)
