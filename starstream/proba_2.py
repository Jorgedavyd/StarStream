from typing import Callable, Coroutine, List, Optional
from numpy._typing import NDArray
from starstream.downloader import DataDownloading
from ._base import CSV
from .utils import (
    StarDate,
    handle_client_connection_error,
    timedelta_to_freq,
)
from astropy.io import fits
from io import BytesIO
import numpy as np
import aiofiles
import os
from datetime import datetime, timedelta
import os.path as osp
import polars as pl

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
            download_path: str = "./data/LYRA",
            batch_size: int = 10,
            store_resolution: Optional[timedelta] = None,
        ):
            super().__init__(
                root_path = download_path,
                batch_size = batch_size,
                csv_path = lambda date: osp.join(download_path, f"{date}.csv")
            )
            if store_resolution is not None:
                self.resolution = timedelta_to_freq(store_resolution)
            self.url: Callable[[str], str] = (
                lambda date: f"http://proba2.oma.be/lyra/data/bsd/{date[:4]}/{date[4:6]}/{date[6:]}/lyra_{date}-000000_lev3_std.fits"
            )

            self.fits_path: Callable[[str], str] = lambda date: osp.join(
                self.root_path, f"{date}.fits"
            )

        @handle_client_connection_error(
            max_retries=3, default_cooldown=5, increment="exp"
        )
        async def _download_url(self, session, date: StarDate) -> None:
            day = date.str()
            async with session.get(self.url(day), ssl=False) as response:
                data = await response.read()
                async with aiofiles.open(self.fits_path(day), "wb") as f:
                    await f.write(data)

        def _get_preprocessing_tasks(self, session) -> List[Coroutine]:
            return [self.preprocess(date) for date in self.new_scrap_date_list]

        async def preprocess(self, date: StarDate) -> None:
            day = date.str()
            async with aiofiles.open(self.fits_path(day), "rb") as f:
                data = await f.read()
                with fits.open(BytesIO(data)) as hdul:
                    data = np.stack(hdul[1].data, axis=0)
                    print(hdul[1].header)
                    data[:, 1:] = data[:, 1:].astype(np.float32)
                    data[:, 0] = min_to_datetime(data[:, 0], day)
                    pl.from_numpy(data[:,:-1], schema = ['date'] + [f'channel_{i}' for i in range(1,5)]).write_csv(self.csv_path(day))
            os.remove(self.fits_path(day))
