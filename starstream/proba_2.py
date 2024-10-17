from typing import Callable
from numpy._typing import NDArray
from starstream._base import CSV
from starstream._utils import (
    asyncFITS,
    download_url_prep,
)
import numpy as np
from datetime import datetime
import os.path as osp
import polars as pl
from starstream.typing import ScrapDate

__all__ = ["PROBA_2"]


def min_to_datetime(data: NDArray, date: str) -> NDArray:
    def func(min: int) -> datetime:
        first_date = datetime.strptime(date, "%Y%m%d")
        hours = min // 60
        mins = min % 60
        return datetime(first_date.year, first_date.month, first_date.day, hours, mins)

    return np.array([func(int(item.item())) for item in data])


class PROBA_2:
    class LYRA(CSV):
        def __init__(
            self,
            root: str = "./data/LYRA",
            batch_size: int = 10,
        ):
            super().__init__(
                root=root,
                batch_size=batch_size,
                filepath=lambda date: osp.join(root, f"{date}.csv"),
            )
            self.url: Callable[[str], str] = (
                lambda date: f"http://proba2.oma.be/lyra/data/bsd/{date[:4]}/{date[4:6]}/{date[6:]}/lyra_{date}-000000_lev3_std.fits"
            )

        def _interval_setup(self, scrap_date: ScrapDate) -> None:
            super()._interval_setup(scrap_date)
            self.urls = [self.url(date.str()) for date in self.dates]
            self.paths = [self.filepath(date.str()) for date in self.dates]

        async def _scrap_(self, idx: int) -> None:
            _ = idx

        async def _download_(self, idx: int) -> None:
            await download_url_prep(self, idx, asyncFITS, self.preprocess, idx)

        async def _prep_(self, idx: int) -> None:
            _ = idx

        async def preprocess(self, hdul, idx: int) -> None:
            date: str = self.dates[idx].str()
            data = np.stack(hdul[1].data, axis=0)
            data[:, 1:] = data[:, 1:].astype(np.float32)
            data[:, 0] = min_to_datetime(data[:, 0], date)
            pl.from_numpy(
                data[:, :-1],
                schema=["date"] + [f"channel_{i}" for i in range(1, 5)],
            ).write_csv(self.filepath(date))
