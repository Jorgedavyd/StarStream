from .utils import datetime_interval, timedelta_to_freq, handle_client_connection_error
from datetime import timedelta, datetime
from spacepy import pycdf
from tqdm import tqdm
import pandas as pd
import numpy as np
import aiofiles
import asyncio
import os
from typing import Callable, Coroutine, List, Tuple
import os.path as osp


class CDAWeb:
    phy_obs: List[str]
    variables: List[str]
    csv_path: Callable[[str], str]
    cdf_path: Callable[[str], str]
    url: Callable[[str], str]
    root_path: str

    def __init__(self, download_path: str, batch_size: int = 10) -> None:
        self.batch_size: int = batch_size
        self.root_path = download_path
        self.csv_path = lambda date: osp.join(self.root_path, f"{date}.csv")
        self.cdf_path = lambda date: osp.join(self.root_path, f"{date}.cdf")
        os.makedirs(self.root_path, exist_ok=True)

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
        ).resample("1min").mean().to_csv(self.csv_path(date))

    def check_tasks(self, scrap_date: Tuple[datetime, datetime]):
        print(f"{self.__class__.__name__}: Looking for missing dates...")
        new_scrap_date: List[str] = datetime_interval(*scrap_date, timedelta(days=1))
        self.new_scrap_date_list = [
            date for date in new_scrap_date if not os.path.exists(self.csv_path(date))
        ]
        os.makedirs(self.root_path, exist_ok=True)

    def get_download_tasks(self, session):
        print(f"{self.__class__.__name__}: Looking for the links of missing dates...")
        return [self.download_url(session, date) for date in self.new_scrap_date_list]

    @handle_client_connection_error(default_cooldown=5, increment="exp", max_retries=5)
    async def download_url(self, session, date: str):
        url = self.url(date)
        async with session.get(url) as response:
            if response.status != 200:
                print(
                    f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                )
                self.new_scrap_date_list.remove(date)
            else:
                cdf_data = await response.read()
                if cdf_data.startswith(b"<html>"):
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                    return
                async with aiofiles.open(self.cdf_path(date), "wb") as f:
                    await f.write(cdf_data)

    async def preprocessing(self, date):
        with pycdf.CDF(self.cdf_path(date)) as cdf_file:
            self.default_cda_processing(cdf_file, date)
        os.remove(self.cdf_path(date))

    def get_preprocessing_tasks(self):
        print(f"{self.__class__.__name__}: Preprocessing stage...")
        return [self.preprocessing(date) for date in self.new_scrap_date_list]

    async def downloader_pipeline(self, scrap_date, session):
        self.check_tasks(scrap_date)
        downloading_tasks = self.get_download_tasks(session)

        for i in tqdm(
            range(0, len(downloading_tasks), self.batch_size),
            desc=f"Downloading for {self.__class__.__name__}...",
        ):
            await asyncio.gather(*downloading_tasks[i : i + self.batch_size])

        prep_tasks: List[Coroutine] = self.get_preprocessing_tasks()

        for i in tqdm(
            range(0, len(prep_tasks), self.batch_size),
            desc=f"Preprocessing for {self.__class__.__name__}...",
        ):
            await asyncio.gather(*prep_tasks[i : i + self.batch_size])

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
