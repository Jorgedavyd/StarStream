from typing import Callable, Coroutine, List, Tuple
from tqdm import tqdm
from .utils import datetime_interval, handle_client_connection_error
from astropy.io import fits
from io import BytesIO
import numpy as np
import aiofiles
import os
import asyncio
from datetime import datetime, timedelta
import os.path as osp

__all__ = ["PROBA_2"]


class PROBA_2:
    class LYRA:
        def __init__(
            self,
            download_path: str = "./data/LYRA",
            batch_size: int = 10,
            sequence_length: timedelta = timedelta(minutes=10),
        ):
            self.lyra_folder_path: str = download_path
            self.batch_size: int = batch_size
            self.url: Callable[[str], str] = (
                lambda date: f"http://proba2.oma.be/lyra/data/bsd/{date[:4]}/{date[4:6]}/{date[6:]}/lyra_{date}-000000_lev3_std.fits"
            )

            self.lyra_fits_path: Callable[[str], str] = lambda date: osp.join(
                self.lyra_folder_path, f"{date}.fits"
            )
            self.lyra_csv_path: Callable[[str], str] = lambda date: osp.join(
                self.lyra_folder_path, f"{date}.fits"
            )
            os.makedirs(self.lyra_folder_path, exist_ok=True)
            self.sl: timedelta = sequence_length

        def get_check_tasks(self, scrap_date: Tuple[datetime, datetime]) -> None:
            new_scrap_date: List[str] = datetime_interval(
                *scrap_date, timedelta(days=1)
            )
            self.new_scrap_date_list: List[str] = [
                date
                for date in new_scrap_date
                if not os.path.exists(self.lyra_csv_path(date))
            ]

        def get_download_tasks(self, session) -> List[Coroutine]:
            return [
                self.download_url(session, date) for date in self.new_scrap_date_list
            ]

        @handle_client_connection_error(
            max_retries=3, default_cooldown=5, increment="exp"
        )
        async def download_url(self, session, date: str) -> None:
            async with session.get(self.url(date), ssl=False) as response:
                data = await response.read()
                async with aiofiles.open(self.lyra_fits_path(date), "wb") as f:
                    await f.write(data)

        def get_preprocessing_tasks(self) -> List[Coroutine]:
            return [self.preprocessing(date) for date in self.new_scrap_date_list]

        async def preprocessing(self, date: str) -> None:
            async with aiofiles.open(self.lyra_fits_path(date), "rb") as f:
                data = await f.read()
                with fits.open(BytesIO(data)) as hdul:
                    np.savetxt(
                        self.lyra_csv_path(date),
                        np.array(list(hdul[1].data), dtype=np.float32),
                        delimiter=",",
                    )
            os.remove(self.lyra_fits_path(date))

        async def downloader_pipeline(
            self, scrap_date: Tuple[datetime, datetime], session
        ) -> None:
            self.get_check_tasks(scrap_date)
            if len(self.new_scrap_date_list) == 0:
                print(f"{self.__class__.__name__}: Already downloaded!")
            else:
                downloading_tasks = self.get_download_tasks(session)
                for i in tqdm(
                    range(0, len(downloading_tasks), self.batch_size),
                    desc=f"Downloading for {self.__class__.__name__}",
                ):
                    await asyncio.gather(*downloading_tasks[i : i + self.batch_size])

                prep_tasks = self.get_preprocessing_tasks()
                for i in tqdm(
                    range(0, len(prep_tasks), self.batch_size),
                    desc=f"Downloading for {self.__class__.__name__}",
                ):
                    await asyncio.gather(*prep_tasks[i : i + self.batch_size])


def save_npy(file, array) -> None:
    np.savetxt(file, array, delimiter=",")
