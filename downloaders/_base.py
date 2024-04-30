from utils import datetime_interval, timedelta_to_freq
from datetime import timedelta, datetime
from spacepy import pycdf
import pandas as pd
import numpy as np
import aiofiles
import asyncio
import cudf
import os


class CDAWeb:
    def default_cda_processing(self, cdf_file, date):
        data_columns = [
            (
                cdf_file[var][:].reshape(-1, 1)
                if len(cdf_file[var][:].shape) == 1
                else cdf_file[var][:]
            )
            for var in self.phy_obs
        ]
        data = np.concatenate(data_columns, axis=1).astype(np.float32)
        pd.DataFrame(
            data,
            columns=self.variables,
            index=pd.to_datetime(cdf_file["Epoch"][:].reshape(-1)),
        ).resample("1T").mean().to_csv(self.csv_path(date))

    def check_tasks(self, scrap_date: tuple[datetime, datetime]):
        scrap_date = datetime_interval(scrap_date[0], scrap_date[-1], timedelta(days=1))
        self.new_scrap_date_list = [
            date for date in scrap_date if not os.path.exists(self.csv_path(date))
        ]
        os.makedirs(self.root_path, exist_ok=True)

    def get_download_tasks(self, session):
        return [self.download_url(session, date) for date in self.new_scrap_date_list]

    async def download_url(self, session, date):
        url = self.url(date)
        async with session.get(url, ssl=False) as response:
            cdf_data = await response.read()
            async with aiofiles.open(self.cdf_path(date), "wb") as f:
                await f.write(cdf_data)

    async def preprocessing(self, date):
        with pycdf.CDF(self.cdf_path(date)) as cdf_file:
            self.default_cda_processing(cdf_file, date)
        os.remove(self.cdf_path(date))

    def get_preprocessing_tasks(self):
        return [self.preprocessing(date) for date in self.new_scrap_date_list]

    async def downloader_pipeline(self, scrap_date, session):
        self.check_tasks(scrap_date)
        await asyncio.gather(*self.get_download_tasks(session))
        await asyncio.gather(*self.get_preprocessing_tasks())

    def get_df(self, date):
        return cudf.read_csv(self.csv_path(date))

    def data_prep(self, scrap_date: tuple[datetime, datetime], step_size: timedelta):
        init, end = [pd.to_datetime(date) for date in scrap_date]
        scrap_date = datetime_interval(init, end, timedelta(days=1))
        df = cudf.concat([self.get_df(date) for date in scrap_date])
        l1 = l1[(l1.index >= init) & (l1.index <= end)]
        return (
            df[(df.index >= init) & (df.index <= end)]
            .resample(timedelta_to_freq(step_size))
            .mean()
        )
