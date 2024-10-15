from dataclasses import dataclass, field
from dateutil.relativedelta import relativedelta
from starstream._utils import (
    async_batch,
    asyncCDF,
    create_scrap_date,
    download_url_write,
    find_files_daily,
    find_files_glob,
    StarDate,
    coroutine_handler,
    StarInterval,
    timedelta_to_freq,
)
from starstream.typing import ScrapDate
from PIL import Image
from typing import Any, Callable, List, Optional, Tuple, Union
from datetime import timedelta, datetime
from numpy._typing import NDArray
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
import torch
from astropy.io import fits
from io import BytesIO
import os.path as osp


@dataclass
class Satellite:
    root: str = field(default="./data/")
    batch_size: int = field(default=10)
    filepath: Callable = field(default=lambda name: osp.join("./data", name))
    date_sampling: Union[timedelta, relativedelta] = field(default=timedelta(days=1))
    format: str = field(default="%Y%m%d")
    dates: List[StarDate] = field(default_factory=list)
    paths: List[str] = field(default_factory=list)
    urls: List[str] = field(default_factory=list)

    def scrap_path(self, date: str) -> str:
        return self.filepath(date)

    async def _scrap_(self, idx: int) -> None:
        """
        Defines all URLs to be downloaded in order to complete the query.
        Populates:
            local dates (List[void]) -> (List[str])
            self.urls (List[void]) -> (List[str])
            self.paths (List[void]) -> (List[str])
        """
        _ = idx
        raise NotImplementedError("_scrap_")

    async def _download_(self, idx: int) -> None:
        """
        Defines the underlying methods to find the values for each Satellite class.
        Changes:
            self.urls (List[str]) -> (List[str | empty]) (pops one element of self.urls)
        """
        _ = idx
        raise NotImplementedError("_download_")

    async def _prep_(self, idx: int) -> None:
        """
        Defines the underlying methods to find the values for each Satellite class.
        Changes:
            self.paths (List[str]) -> (List[str]) (pops one element of self.paths)
        """
        _ = idx
        raise NotImplementedError("_prep_")

    def _find_local(self, date: StarDate) -> Union[bool, int]:
        return find_files_daily(self, date.str())

    def _interval_setup(self, scrap_date: ScrapDate) -> None:
        new_scrap_date = StarInterval(
            create_scrap_date(scrap_date), self.date_sampling, self.format
        )

        for date in tqdm(
            new_scrap_date,
            desc=f"{self.__class__.__name__}: Looking for missing dates...",
        ):
            if not self._find_local(date):
                self.dates.append(date)

        if self.dates:
            os.makedirs(self.root, exist_ok=True)

    async def _scrap(self, idx: int) -> None:
        """
        Defines all URLs to be downloaded in order to complete the query.
        Populates:
            local dates (List[StarDate]) -> (List[StarDate])
            self.urls (List[str]) -> (List[str])
            self.paths (List[str]) -> (List[str])
        """
        await self._scrap_(idx)

    async def _download(self, idx: int) -> None:
        """
        Downloads the URLs defined at self._scrap_url.
        Changes:
            self.urls (List[str]) -> (List[str]) (pops one URL)
        """
        await self._download_(idx)

    async def _preprocess(self, idx: int) -> None:
        """
        Preprocess the data with asynchronous methods.
        Changes:
            self.paths (List[str]) -> (List[str | empty]) (pops the path)
        """
        await self._prep_(idx)

    async def fetch(self, scrap_date: ScrapDate, session) -> None:
        await coroutine_handler(self._interval_setup, scrap_date)
        self.session = session
        await async_batch(
            self,
            ("_scrap", "_download", "_preprocess"),
            f"{self.__class__.__name__}",
        )


class CSV(Satellite):
    def __init__(
        self,
        root: str = "./data",
        batch_size: int = 10,
        filepath: Callable = lambda date: osp.join("./data", f"{date}.csv"),
        date_sampling: Union[timedelta, relativedelta] = timedelta(days=1),
        format: str = "%Y%m%d",
    ) -> None:
        super().__init__(
            root,
            batch_size,
            filepath,
            date_sampling,
            format,
        )

    def _get_df_unit(self, date: str) -> pl.DataFrame:
        return pl.read_csv(self.filepath(date), try_parse_dates=True)

    def _get_df(self, scrap_date: StarInterval) -> pl.DataFrame:
        return pl.concat([self._get_df_unit(date.str()) for date in scrap_date])

    def _convert_to_format(
        self,
        scrap_date: ScrapDate,
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
        return tuple(
            [
                torch.from_numpy(df.drop("date").to_numpy().astype(np.float32))
                for df in list_df
            ]
        )

    def get_polars(
        self,
        scrap_date: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]],
        resolution: Optional[timedelta] = None,
    ) -> Tuple[pl.DataFrame, ...]:
        return tuple(self._process_polars(scrap_date, resolution))

    def _process_polars(
        self,
        scrap_date: ScrapDate,
        resolution: Optional[timedelta] = None,
    ) -> List[pl.DataFrame]:
        scrap_date = create_scrap_date(scrap_date)
        out_list: List[pl.DataFrame] = []
        for tuple_date in scrap_date:
            new_scrap_date: StarInterval = StarInterval(
                [tuple_date], self.date_sampling, self.format
            )
            df: pl.DataFrame = self._get_df(new_scrap_date)
            if resolution is not None:
                df = (
                    df.filter(
                        (pl.col("date") > new_scrap_date.interval[0].polars())
                        & (pl.col("date") < new_scrap_date.interval[-1].polars())
                    )
                    .group_by_dynamic("date", every=timedelta_to_freq(resolution))
                    .agg(pl.col("*"))
                    .mean()
                )
            else:
                df = df.filter(
                    (pl.col("date") > new_scrap_date.interval[0].polars())
                    & (pl.col("date") < new_scrap_date.interval[-1].polars())
                )
            out_list.append(df)
        return out_list


class CDAWeb(CSV):
    phy_obs: List[str]
    variables: List[str]
    url: Callable[[str], str]

    def __init__(
        self,
        root: str = "./data",
        batch_size: int = 10,
        date_sampling: Union[timedelta, relativedelta] = timedelta(days=1),
        format: str = "%Y%m%d",
    ) -> None:
        super().__init__(
            root,
            batch_size,
            lambda date: osp.join(self.root, f"{date}.csv"),
            date_sampling,
            format,
        )
        self.cdf_path = lambda date: osp.join(self.root, f"{date}.cdf")

    async def _scrap_(self, idx: int) -> None:
        self.paths.extend(
            [
                self.cdf_path(date.str())
                for date in self.dates[idx : idx + self.batch_size]
            ]
        )
        self.urls.extend(
            [self.url(date.str()) for date in self.dates[idx : idx + self.batch_size]]
        )

    async def _download_(self, idx: int) -> None:
        return await download_url_write(self, idx)

    async def _prep_(self, idx: int):
        try:
            date: str = self.dates[idx].str()
        except IndexError:
            return
        def processing(cdf_file) -> None:
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
            time = pl.from_numpy(epoch, schema=["date"], orient="col").cast(
                {"date": pl.Datetime}
            )
            output = pl.from_numpy(data_columns, schema=self.variables, orient="col")
            output = output.with_columns(time)
            output.write_csv(self.filepath(date))
            os.remove(self.cdf_path(date))

        try:
            path: str = self.paths[idx]
            await asyncCDF(path, processing)
        except IndexError:
            return


@dataclass
class Img(Satellite):
    def _find_local(self, date: str) -> Tuple[bool, List[str]]:
        return find_files_glob(self, date)

    def _path_prep(self, scrap_date: List[Tuple[datetime, datetime]]) -> List[str]:
        """
        Defines the path scrapping method that is used to get all files.
        """
        out: List[str] = []
        for date in StarInterval(
            create_scrap_date(scrap_date), self.date_sampling, self.format
        ):
            out.extend(self._find_local(date.str())[-1])
        return out

    def process_image(self, content: bytes) -> NDArray:
        image = Image.open(BytesIO(content))
        return np.array(image)

    def process_fits(self, content: bytes) -> NDArray:
        with fits.open(BytesIO(content)) as hdul:
            data = hdul[0].data
            if data is not None:
                data = data.astype(np.float32)
        return data

    async def load_fits(self, path: str) -> NDArray:
        async with aiofiles.open(path, mode="rb") as file:
            content = await file.read()

        loop = asyncio.get_running_loop()
        array = await loop.run_in_executor(None, self.process_fits, content)

        return array

    async def load_img(self, path: str) -> NDArray:
        async with aiofiles.open(path, mode="rb") as file:
            content = await file.read()

        loop = asyncio.get_running_loop()
        array = await loop.run_in_executor(None, self.process_image, content)

        return array

    async def async_numpy(
        self, scrap_date: List[Tuple[datetime, datetime]]
    ) -> np.ndarray:
        paths: List[str] = self._path_prep(scrap_date)
        sample_path: str = paths[0]

        match sample_path.split(".")[-1]:
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
            await asyncio.gather(*[method(path) for path in paths]),
            axis=0,
        )

    async def async_torch(self, scrap_date: List[Tuple[datetime, datetime]]) -> Tensor:
        return torch.from_numpy(await self.async_numpy(scrap_date))

    def get_torch(self, scrap_date: ScrapDate) -> Tensor:
        new_scrap_date = create_scrap_date(scrap_date)
        return asyncio.run(self.async_torch(new_scrap_date))

    def get_numpy(self, scrap_date: ScrapDate) -> NDArray:
        new_scrap_date = create_scrap_date(scrap_date)
        return asyncio.run(self.async_numpy(new_scrap_date))
