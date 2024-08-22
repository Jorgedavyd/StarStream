from .utils import datetime_interval, timedelta_to_freq
from datetime import timedelta, datetime
from spacepy import pycdf
import pandas as pd
import numpy as np
import aiofiles
import asyncio
import os
from typing import Callable, List, Tuple


class CDAWeb:
    phy_obs: List[str]
    batch_size = 8
    variables: List[str]
    csv_path: Callable[[str], str]
    cdf_path: Callable[[str], str]
    url: Callable[[str], str]
    root_path: str

    def default_cda_processing(self, cdf_file, date):
        epoch = cdf_file["Epoch"][:].reshape(-1)
        data_columns = [
            (
                cdf_file[var][:].reshape(-1, 1)
                if len(cdf_file[var][:].shape) == 1
                else cdf_file[var][:].reshape(epoch.shape[0], -1)
            )
            for var in self.phy_obs
        ]
        data = np.concatenate(data_columns, axis=1).astype(np.float32)
        pd.DataFrame(
            data,
            columns=self.variables,
            index=pd.to_datetime(epoch),
        ).resample("1T").mean().to_csv(self.csv_path(date))

    def check_tasks(self, scrap_date: Tuple[datetime, datetime]):
        new_scrap_date: List[str] = datetime_interval(*scrap_date, timedelta(days=1))
        self.new_scrap_date_list = [
            date for date in new_scrap_date if not os.path.exists(self.csv_path(date))
        ]
        os.makedirs(self.root_path, exist_ok=True)

    def get_download_tasks(self, session):
        return [self.download_url(session, date) for date in self.new_scrap_date_list]

    async def download_url(self, session, date: str):
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
        tasks = self.get_download_tasks(session)

        for i in range(0, len(self.new_scrap_date_list), self.batch_size):
            await asyncio.gather(*tasks[i : i + self.batch_size])

        await asyncio.gather(*self.get_preprocessing_tasks())

    async def get_df_unit(self, date: str) -> pd.DataFrame:
        df = await asyncio.get_event_loop().run_in_executor(
            None, pd.read_csv, self.csv_path(date)
        )
        return df

    async def get_df(self, scrap_date: Tuple[datetime, datetime]) -> List[pd.DataFrame]:
        new_scrap_date: List[str] = datetime_interval(*scrap_date, timedelta(days=1))
        dfs = await asyncio.gather(*[self.get_df_unit(date) for date in new_scrap_date])
        return dfs

    def data_prep(
        self, scrap_date: Tuple[datetime, datetime], step_size: timedelta
    ) -> pd.DataFrame:
        dfs = asyncio.run(self.get_df(scrap_date))
        df = pd.concat(dfs)
        return (
            df[(df.index >= scrap_date[0]) & (df.index <= scrap_date[-1])]
            .resample(timedelta_to_freq(step_size))
            .mean()
        )
