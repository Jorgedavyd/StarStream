from typing import Callable, Coroutine, List, Optional, Tuple, Union
from numpy._typing import NDArray
from tqdm import tqdm
from .utils import (
    Satellite,
    handle_client_connection_error,
    timedelta_to_freq,
)
from astropy.io import fits
from io import BytesIO
import numpy as np
import aiofiles
import os
import asyncio
from datetime import datetime, timedelta
import os.path as osp

__all__ = ["PROBA_2"]


def min_to_datetime(data: NDArray, date: str) -> NDArray:
    def func(min: int) -> datetime:
        first_date = datetime.strptime(date, "%Y%m%d")
        hours = min // 60
        mins = min % 60
        return datetime(first_date.year, first_date.month, first_date.day, hours, mins)

    return np.array([func(int(item.item())) for item in data])


class PROBA_2:
    class LYRA(Satellite):
        def __init__(
            self,
            download_path: str = "./data/LYRA",
            batch_size: int = 10,
            store_resolution: Optional[timedelta] = None,
        ):
            if store_resolution is not None:
                self.resolution = timedelta_to_freq(store_resolution)
            self.root: str = download_path
            self.batch_size: int = batch_size
            self.url: Callable[[str], str] = (
                lambda date: f"http://proba2.oma.be/lyra/data/bsd/{date[:4]}/{date[4:6]}/{date[6:]}/lyra_{date}-000000_lev3_std.fits"
            )

            self.fits_path: Callable[[str], str] = lambda date: osp.join(
                self.root, f"{date}.fits"
            )
            self.csv_path: Callable[[str], str] = lambda date: osp.join(
                self.root, f"{date}.csv"
            )
            os.makedirs(self.root, exist_ok=True)

        def get_download_tasks(self, session) -> List[Coroutine]:
            return [
                self.download_url(session, date) for date in self.new_scrap_date_list
            ]

        @handle_client_connection_error(
            max_retries=3, default_cooldown=5, increment="exp"
        )
        async def _download_url(self, session, date: str) -> None:
            async with session.get(self.url(date), ssl=False) as response:
                data = await response.read()
                async with aiofiles.open(self.fits_path(date), "wb") as f:
                    await f.write(data)

        def get_preprocessing_tasks(self) -> List[Coroutine]:
            return [self.preprocessing(date) for date in self.new_scrap_date_list]

        async def preprocessing(self, date: str) -> None:
            async with aiofiles.open(self.lyra_fits_path(date), "rb") as f:
                data = await f.read()
                with fits.open(BytesIO(data)) as hdul:
                    data = np.stack(hdul[1].data, axis=0)
                    data[:, 1:] = data[:, 1:].astype(np.float32)
                    data[:, 0] = min_to_datetime(data[:, 0], date)
                    print(data)
                    np.savetxt(
                        self.lyra_csv_path(date),
                        data[:, :-1],
                        delimiter=",",
                    )
            os.remove(self.fits_path(date))

        async def fetch(
            self,
            scrap_date: Union[
                List[Tuple[datetime, datetime]], Tuple[datetime, datetime]
            ],
            session,
        ) -> None:
            if isinstance(scrap_date[0], datetime):
                self._check_tasks([scrap_date])
            else:
                self._check_tasks(scrap_date)
            if len(self.new_scrap_date_list) == 0:
                print(f"{self.__class__.__name__}: Already downloaded!")
            else:
                downloading_tasks = self.get_download_tasks(session)
                for i in tqdm(
                    range(0, len(downloading_tasks), self.batch_size),
                    desc=f"{self.__class__.__name__}: Downloading...",
                ):
                    await asyncio.gather(*downloading_tasks[i : i + self.batch_size])

                prep_tasks = self.get_preprocessing_tasks()
                for i in tqdm(
                    range(0, len(prep_tasks), self.batch_size),
                    desc=f"{self.__class__.__name__}: Preprocessing...",
                ):
                    await asyncio.gather(*prep_tasks[i : i + self.batch_size])
