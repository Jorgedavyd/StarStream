from .utils import datetime_interval
from astropy.io import fits
from io import BytesIO
import numpy as np
import aiofiles
import os
import asyncio
from datetime import timedelta

__all__ = ["PROBA_2"]


class PROBA_2:
    class LYRA:
        def __init__(self, sequence_length):
            self.url = (
                lambda date: f"http://proba2.oma.be/lyra/data/bsd/{date[:4]}/{date[4:6]}/{date[6:]}/lyra_{date}-000000_lev3_std.fits"
            )
            self.lyra_folder_path = "./data/LYRA/"
            self.lyra_fits_path = lambda date: f"./data/LYRA/{date}.fits"
            self.lyra_csv_path = lambda date: f"./data/LYRA/{date}.csv"
            self.sl = sequence_length
            os.makedirs(self.lyra_folder_path, exist_ok=True)

        def get_check_tasks(self, scrap_date):
            scrap_date = datetime_interval(
                scrap_date[0], scrap_date[-1], timedelta(days=1)
            )
            self.new_scrap_date_list = [
                date
                for date in scrap_date
                if not os.path.exists(self.lyra_csv_path(date))
            ]

        def get_download_tasks(self, session):
            return [
                self.download_url(session, date) for date in self.new_scrap_date_list
            ]

        async def download_url(self, session, date):
            async with session.get(self.url(date), ssl=False) as response:
                data = await response.read()
                async with aiofiles.open(self.lyra_fits_path(date), "wb") as f:
                    await f.write(data)

        def get_preprocessing_tasks(self):
            return [self.preprocessing(date) for date in self.new_scrap_date_list]

        async def preprocessing(self, date):
            async with aiofiles.open(self.lyra_fits_path(date), "rb") as f:
                data = await f.read()
                with fits.open(BytesIO(data)) as hdul:
                    np.savetxt(
                        self.lyra_csv_path(date),
                        np.array(list(hdul[1].data), dtype=np.float32),
                        delimiter=",",
                    )
            os.remove(self.lyra_fits_path(date))

        async def downloader_pipeline(self, scrap_date, session) -> None:
            self.get_check_tasks(scrap_date)
            if len(self.new_scrap_date_list) == 0:
                print("Already downloaded!")
            else:
                await asyncio.gather(*self.get_download_tasks(session))
                await asyncio.gather(*self.get_preprocessing_tasks())


def save_npy(file, array) -> None:
    np.savetxt(file, array, delimiter=",")
