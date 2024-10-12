from dataclasses import dataclass, field
from dateutil.relativedelta import relativedelta
from .utils import (
    StarDate,
    coroutine_handler,
    StarInterval,
    handle_client_connection_error,
    timedelta_to_freq,
)
from PIL import Image
from typing import Any, Callable, Coroutine, List, Optional, Tuple, Union
from datetime import timedelta, datetime
from numpy._typing import NDArray
from spacepy import pycdf
from torch import Tensor
from tqdm import tqdm
import os.path as osp
import polars as pl
import pandas as pd
import numpy as np
import aiofiles
import asyncio
import torch
import os
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import torch
from astropy.io import fits
from io import BytesIO

@dataclass
class Satellite:
    root_path: str = field(default = './data/')
    batch_size: int = field(default = 10)
    date_sampling: Union[timedelta, relativedelta] = field(default = timedelta(days = 1))
    format: str = field(default = '%Y%m%d')

    def __post_init__(self) -> None:
        self.new_scrap_date_list: List[StarDate] = []
        self.urls: List[str] = []

    def _scrap_url() -> None:
        self.urls.extend(new_urls)

    def _download_url(self, session, url: str) -> None:

    def _preprocess(self, path: str) -> None:

    async def fetch(self, scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]], session) -> None: raise NotImplemented("Fetch not defined")

@dataclass
class CSV(Satellite):
    csv_path: Callable[[str], str] = lambda x: x

    def _get_download_tasks(self, session) -> List[Coroutine]:
        return [self._download_url(session, date) for date in self.new_scrap_date_list]

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
        return pl.read_csv(self.csv_path(date), try_parse_dates = True)

    def _get_df(self, scrap_date: StarInterval) -> pl.DataFrame:
        return pl.concat([self._get_df_unit(date.str()) for date in scrap_date])

    def _convert_to_format(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: Optional[timedelta] = None,
        method: str = None,
    ) -> Tuple[Any, ...]:
        list_df: List[pl.DataFrame] = self._process_polars(scrap_date, resolution)
        return tuple([getattr(df.drop("date"), method)() for df in list_df])

    def get_numpy(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: Optional[timedelta] = None,
    ) -> Tuple[NDArray, ...]:
        return self._convert_to_format(scrap_date, resolution, "to_numpy")

    def get_pandas(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: Optional[timedelta] = None,
    ) -> Tuple[pd.DataFrame, ...]:
        return self._convert_to_format(scrap_date, resolution, "to_pandas")

    def get_torch(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: Optional[timedelta] = None,
    ) -> Tuple[Tensor, ...]:
        list_df: List[pl.DataFrame] = self._process_polars(scrap_date, resolution)
        return tuple([torch.from_numpy(df.drop("date").to_numpy().astype(np.float32)) for df in list_df])

    def get_polars(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: Optional[timedelta] = None,
    ) -> Tuple[pl.DataFrame, ...]:
        return tuple(self._process_polars(scrap_date, resolution))

    def _process_polars(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: Optional[timedelta] = None,
    ) -> List[pl.DataFrame]:
        if isinstance(scrap_date[0], datetime):
            scrap_date = [scrap_date]
        out_list: List[pl.DataFrame] = []
        for tuple_date in scrap_date:
            new_scrap_date: StarInterval = StarInterval(
                [tuple_date], self.date_sampling, self.format
            )
            df: pl.DataFrame = self._get_df(new_scrap_date)
            if resolution is not None:
                df = df.filter(
                    (pl.col("date") > new_scrap_date.interval[0].polars())
                    & (pl.col("date") < new_scrap_date.interval[-1].polars())
                ).group_by_dynamic("date", every = timedelta_to_freq(resolution)).agg(
                    pl.col("*")
                ).mean()
            else:
                df = df.filter(
                    (pl.col("date") > new_scrap_date.interval[0].polars())
                    & (pl.col("date") < new_scrap_date.interval[-1].polars())
                )
            out_list.append(df)
        return out_list

    async def _check_tasks(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
        if func:=getattr(self, '_check_update', False):
            await coroutine_handler(func, scrap_date)

        new_scrap_date: StarInterval = StarInterval(scrap_date, self.date_sampling, self.format)

        for date in tqdm(
            new_scrap_date,
            desc=f"{self.__class__.__name__}: Looking for missing dates...",
        ):
            if not os.path.exists(self.csv_path(date.str())):
                self.new_scrap_date_list.append(date)

        if self.new_scrap_date_list:
            os.makedirs(self.csv_path("")[:-4], exist_ok=True)

class CDAWeb(CSV):
    phy_obs: List[str]
    variables: List[str]
    url: Callable[[str], str]

    def __init__(
            self,
            download_path: str,
            batch_size: int = 10,
            date_sampling: Union[timedelta, relativedelta] = timedelta(days = 1),
            format: str = '%Y%m%d'
    ) -> None:
        super().__init__(
            root_path = download_path,
            batch_size = batch_size,
            date_sampling = date_sampling,
            format = format,
            csv_path = lambda date: osp.join(download_path, f"{date}.csv")
        )
        self.cdf_path: Callable[[str], str] = lambda date: osp.join(
            self.root_path, f"{date}.cdf"
        )

    def _preprocess(self, date: str):
        with pycdf.CDF(self.cdf_path(date)) as cdf_file:
            epoch = cdf_file["Epoch"][:]
            if epoch is None:
                raise ValueError("Epoch is None")
            epoch = epoch.astype(np.datetime64).reshape(-1)
            def data_func(var: str) -> NDArray:
                file = cdf_file[var][:]
                if file is not None:
                    return file.astype(np.float32)
                else:
                    raise ValueError("Data is None")

            data_columns: List[NDArray] = []
            for var in self.phy_obs:
                data = cdf_file[var][:]
                shape = data.shape
                if len(shape) == 1:
                    data_columns.append(data_func(var).reshape(-1, 1))
                elif len(shape) == 2:
                    data_columns.append(data_func(var))
                else:
                    raise ValueError("Found singularity")
            data_columns = np.concatenate(data_columns, -1).astype(np.float32).T
            time = pl.from_numpy(epoch, schema = ['date'], orient = 'col').cast({"date": pl.Datetime})
            output = pl.from_numpy(data_columns, schema = self.variables, orient = 'col')
            output = output.with_columns(time)
            output.write_csv(self.csv_path(date))
            os.remove(self.cdf_path(date))

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

class StarImage(Satellite):
    def __init__(self, download_path: str, batch_size: int) -> None:
        super().__init__(download_path, batch_size)
        os.makedirs(download_path, exist_ok = True)

    def _path_prep(self, scrap_date: List[Tuple[datetime, datetime]]) -> List[str]: raise NotImplemented("_path_prep not implemented")

    def process_image(self, content: bytes):
        image = Image.open(BytesIO(content))
        return np.array(image)

    def process_fits(self, content: bytes):
        with fits.open(BytesIO(content)) as hdul:
            data = hdul[0].data
            if data is not None:
                data = hdul[0].data.astype(np.float32)
        return data

    async def load_fits(self, path: str) -> NDArray:
        async with aiofiles.open(path, mode="rb") as file:
            content = await file.read()

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            array = await loop.run_in_executor(pool, self.process_fits, content)

        return array

    async def load_img(self, path: str) -> NDArray:
        async with aiofiles.open(path, mode="rb") as file:
            content = await file.read()

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            array = await loop.run_in_executor(pool, self.process_image, content)

        return array

    async def async_numpy(self, scrap_date: List[Tuple[datetime, datetime]]) -> np.ndarray:
        paths: List[str] = self._path_prep(scrap_date)
        sample_path: str = paths[0]

        match sample_path.split('.')[-1]:
            case "fits":
                method = self.load_fits
            case "png":
                method = self.load_img
            case "jp2":
                method = self.load_img
            case "jpg":
                method = self.load_img
            case _:
                raise ValueError("Not valid path to an image")

        return np.stack(
            await asyncio.gather(
                *[method(path) for path in paths]
            ),
            axis=0,
        )

    async def async_torch(self, scrap_date: List[Tuple[datetime, datetime]]) -> Tensor:
        return torch.from_numpy(await self.async_numpy(scrap_date))

    def get_torch(self, scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]]) -> Tensor:
        if isinstance(scrap_date[0], datetime):
            scrap_date = [scrap_date]
        return asyncio.run(self.async_torch(scrap_date))

    def get_numpy(self, scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]]) -> NDArray:
        if isinstance(scrap_date[0], datetime):
            scrap_date = [scrap_date]
        return asyncio.run(self.async_numpy(scrap_date))
