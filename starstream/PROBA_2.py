from .utils import interval_time
from astropy.io import fits
from io import BytesIO
import numpy as np
import aiofiles
import os

"""cambiar y probar no importa mucho ahora"""


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

        def get_check_tasks(self, scrap_date):
            self.scrap_date = interval_time(scrap_date[0], scrap_date[-1])
            for date in scrap_date:
                if os.path.exists(self.lyra_csv_path(date)):
                    pass
                else:
                    self.new_scrap_date_list.append(date)

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
                    np.savetxt(self.lyra_csv_path(date), hdul[1].data, delimiter=",")
