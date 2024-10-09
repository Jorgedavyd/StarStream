from dataclasses import dataclass, field
from dateutil.relativedelta import relativedelta
from .utils import (
    StarDate,
    coroutine_handler,
    StarInterval,
    handle_client_connection_error,
    timedelta_to_freq,
)
from typing import Any, Callable, Coroutine, List, Tuple, Union
from datetime import timedelta, datetime
from numpy._typing import NDArray
from spacepy import pycdf
from torch import Tensor
from icecream import ic
from tqdm import tqdm
import os.path as osp
import polars as pl
import pandas as pd
import numpy as np
import aiofiles
import asyncio
import os

@dataclass
class Satellite:
    root_path: str = field(default = './data/')
    batch_size: int = field(default = 10)
    date_sampling: Union[timedelta, relativedelta] = field(default = timedelta(days = 1))
    format: str = field(default = '%Y%m%d')
    new_scrap_date_list: List[StarDate] = field(default = [])
    async def fetch(self, scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]], session) -> None: raise NotImplemented("Fetch not defined")

@dataclass
class CSV(Satellite):
    csv_path: Callable[[str], str] = field(default = lambda x: x)

    def _get_download_tasks(self, session) -> List[Coroutine]:
        return [
            self._download_url(session, date) for date in self.new_scrap_date_list
        ]

    async def fetch(
        self,
        scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]],
        session,
    ) -> None:
        if isinstance(scrap_date[0], datetime):
            scrap_date = [scrap_date]
        await coroutine_handler(self._check_tasks, scrap_date)
        download_tasks: List[Coroutine] = await coroutine_handler(self._get_download_tasks, session)
        for i in tqdm(
            range(0, len(download_tasks), self.batch_size),
            desc=f"{self.__class__.__name__}: Downloading...",
        ):
            await asyncio.gather(*download_tasks[i : i + self.batch_size])

        if func:=getattr(self, '_get_preprocessing_tasks', False):
            preprocessing_tasks: List[Coroutine] = await coroutine_handler(func, session)
            for i in tqdm(
                range(0, len(preprocessing_tasks), self.batch_size),
                desc=f"{self.__class__.__name__}: Preprocessing..."):
                await asyncio.gather(*preprocessing_tasks[i : i + self.batch_size])

        if func:=getattr(self, '_preprocess', False):
            for date in tqdm(
                self.new_scrap_date_list,
                desc=f"{self.__class__.__name__}: Preprocessing...",
            ):
                await coroutine_handler(func, date.str())

    def _get_df_unit(self, date: str) -> pl.DataFrame:
        return pl.read_csv(self.csv_path(date), try_parse_dates=True)

    def _get_df(self, scrap_date: StarInterval) -> pl.DataFrame:
        return pl.concat([self._get_df_unit(date.str()) for date in scrap_date])

    def _convert_to_format(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: timedelta,
        method: str,
    ) -> Tuple[Any, ...]:
        list_df: List[pl.DataFrame] = self._process_polars(scrap_date, resolution)
        return tuple([getattr(df, method)() for df in list_df])

    def get_numpy(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: timedelta,
    ) -> Tuple[NDArray, ...]:
        return self._convert_to_format(scrap_date, resolution, "to_numpy")

    def get_pandas(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: timedelta,
    ) -> Tuple[pd.DataFrame, ...]:
        return self._convert_to_format(scrap_date, resolution, "to_pandas")

    def get_torch(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: timedelta,
    ) -> Tuple[Tensor, ...]:
        return self._convert_to_format(scrap_date, resolution, "to_torch")

    def get_polars(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: timedelta,
    ) -> Tuple[pl.DataFrame, ...]:
        return tuple(self._process_polars(scrap_date, resolution))

    def _process_polars(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: timedelta,
    ) -> List[pl.DataFrame]:
        if isinstance(scrap_date[0], datetime):
            scrap_date = [scrap_date]
        out_list: List[pl.DataFrame] = []
        for tuple_date in scrap_date:
            new_scrap_date: StarInterval = StarInterval(
                [tuple_date], self.date_sampling, self.format
            )
            df: pl.DataFrame = self._get_df(new_scrap_date)
            df = df.cast({pl.Date: pl.Datetime})
            df.filter(
                (pl.col("time") > new_scrap_date.interval[0].polars())
                & (pl.col("time") < new_scrap_date.interval[-1].polars())
            ).group_by_dynamic("time", every = timedelta_to_freq(resolution)).agg(
                pl.col("*")
            ).mean()
            out_list.append(df)
        return out_list

    def _check_tasks(self, scrap_date: List[Tuple[datetime, datetime]], resolution: Union[timedelta, relativedelta] = timedelta(days = 1), format: str = '%Y%m%d') -> None:
        if func:=getattr(self, '_check_updates', False):
            func(scrap_date)

        new_scrap_date: StarInterval = StarInterval(scrap_date)

        for date in tqdm(
            new_scrap_date,
            desc=f"{self.__class__.__name__}: Looking for missing dates...",
        ):
            if not os.path.exists(self.csv_path(date.str())):
                self.new_scrap_date_list.append(date)

        if self.new_scrap_date_list:
            os.makedirs(self.root_path, exist_ok=True)

class CDAWeb(CSV):
    phy_obs: List[str]
    variables: List[str]
    url: Callable[[str], str]

    def __init__(self, download_path: str, batch_size: int = 10) -> None:
        self.batch_size: int = batch_size
        self.root_path: str = download_path
        self.csv_path: Callable[[str], str] = lambda date: osp.join(
            self.root_path, f"{date}.csv"
        )
        self.cdf_path: Callable[[str], str] = lambda date: osp.join(
            self.root_path, f"{date}.cdf"
        )

    def _preprocess(self, date: str):
        with pycdf.CDF(self.cdf_path(date)) as cdf_file:
            epoch = cdf_file["Epoch"][:]
            if epoch is not None:
                epoch = epoch.reshape(-1, 1)
            else:
                raise ValueError("Epoch is None")

            def data_func(var: str) -> NDArray:
                file = cdf_file[var][:]
                if file is not None:
                    return file.astype(np.float32)
                else:
                    raise ValueError("Data is None")

            sample =cdf_file[self.phy_obs[0]][:]
            if sample is not None:
                sample_shape: Tuple[int, ...] = sample.shape
                ic(sample_shape)
                if len(sample_shape) == 1:
                    data_columns = [data_func(var).reshape(-1, 1) for var in self.phy_obs]
                elif len(sample_shape) == 2:
                    data_columns = [data_func(var) for var in self.phy_obs]
                else:
                    raise ValueError("Found singularity")
                data_columns.extend(epoch)
                data: NDArray = np.concatenate(data_columns, axis=1)
                columns: List[str] = ["time"] + self.variables

                pl.from_numpy(data, schema=columns, orient="col").write_csv(self.csv_path(date))
                os.remove(self.cdf_path(date))
            else:
                ValueError("Data is None")

    @handle_client_connection_error(default_cooldown=5, increment="exp", max_retries=5)
    async def _download_url(self, session, date: StarDate) -> None:
        url: str = self.url(date.str())
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
                async with aiofiles.open(self.cdf_path(date.str()), "wb") as f:
                    await f.write(cdf_data)
