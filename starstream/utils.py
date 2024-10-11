from collections.abc import Callable
from datetime import datetime, timedelta
import functools
from numpy._typing import NDArray
from aiohttp import ClientConnectionError
from dateutil.relativedelta import relativedelta
from astropy.io import fits
from spacepy import pycdf
from io import BytesIO
import zipfile
import asyncio
import tarfile
import gzip
from inspect import iscoroutinefunction
from typing import Dict, Optional, Tuple, List, Any, Union
from dataclasses import dataclass, field
from itertools import chain
import polars as pl
import numpy as np
from PIL import Image
import aiofiles
import io
from concurrent.futures import ThreadPoolExecutor
import torch
from torch import Tensor
from inspect import iscoroutinefunction

## Asynchronous processing
async def asyncCDF(cdf_path: str, processing: Callable, *args) -> Any:
    cdf_file = await asyncio.get_event_loop().run_in_executor(None, pycdf.CDF, cdf_path)
    if iscoroutinefunction(processing):
        out = await processing(cdf_file, *args)
    else:
        out = processing(cdf_file, *args)
    cdf_file.close()
    return out


async def asyncZIP(bytes_obj: BytesIO, processing: Callable, *args) -> Any:
    zip_file = await asyncio.get_event_loop().run_in_executor(
        None, zipfile.ZipFile, bytes_obj
    )
    if iscoroutinefunction(processing):
        out = await processing(zip_file, *args)
    else:
        out = processing(zip_file, *args)
    zip_file.close()
    return out


async def asyncGZ(bytes_obj: BytesIO, processing: Callable, *args) -> Any:
    gz_file = await asyncio.get_event_loop().run_in_executor(None, syncGZ, bytes_obj)
    if iscoroutinefunction(processing):
        out = await processing(gz_file, *args)
    else:
        out = processing(gz_file, *args)
    gz_file.close()
    return out


async def asyncGZFITS(bytes_obj: BytesIO, processing, *args) -> None:
    gz_file = await asyncio.get_event_loop().run_in_executor(None, syncGZ, bytes_obj)
    await asyncFITS(BytesIO(gz_file.read()), processing, *args)
    gz_file.close()


def syncGZ(file_obj: BytesIO):
    return gzip.GzipFile(fileobj=file_obj)


async def asyncTAR(bytes_obj: BytesIO, processing: Callable, *args) -> Any:
    tar_file = await asyncio.get_event_loop().run_in_executor(None, syncTAR, bytes_obj)
    if iscoroutinefunction(processing):
        out = await processing(tar_file, *args)
    else:
        out = processing(tar_file, *args)
    tar_file.close()
    return out


def syncTAR(file_obj: BytesIO):
    return tarfile.open(fileobj=file_obj, mode="r")


async def asyncFITS(bytes_obj: BytesIO, processing: Callable, *args) -> Any:
    fits_file = await asyncio.get_event_loop().run_in_executor(
        None, fits.open, bytes_obj
    )
    if iscoroutinefunction(processing):
        out = await processing(fits_file, *args)
    else:
        out = processing(fits_file, *args)
    fits_file.close()
    return out


## Datetime manipulation
def timedelta_to_freq(timedelta_obj: timedelta) -> str:
    total_seconds = timedelta_obj.total_seconds()
    if total_seconds % 1 != 0:
        raise ValueError("Timedelta must represent a whole number of seconds")

    days = int(total_seconds // (24 * 3600))
    hours = int((total_seconds % (24 * 3600)) // 3600)
    minutes = int(((total_seconds % (24 * 3600)) % 3600) // 60)
    seconds = int(((total_seconds % (24 * 3600)) % 3600) % 60)

    freq_parts = []
    if days > 0:
        freq_parts.append(f"{days}d")
    if hours > 0:
        freq_parts.append(f"{hours}h")
    if minutes > 0:
        freq_parts.append(f"{minutes}min")
    if seconds > 0:
        freq_parts.append(f"{seconds}s")

    return "".join(freq_parts) if freq_parts else "0s"


def to_polars(obj: datetime):
    return pl.datetime(
        year=obj.year,
        month=obj.month,
        day=obj.day,
        hour=obj.hour,
        minute=obj.minute,
        second=obj.second,
    )


@dataclass(frozen=True)
class StarDate:
    date: datetime
    format: Optional[str] = field(default=None)

    def str(self, format: Optional[str] = None) -> str:
        if format is None:
            assert self.format is not None, "String format not defined"
            return self.date.strftime(self.format)
        return self.date.strftime(format)

    def polars(self):
        return to_polars(self.date)


def interval_time(
    init: StarDate, end: StarDate, resolution: Union[timedelta, relativedelta]
) -> List[StarDate]:
    current_time = init.date
    dates = []
    while current_time <= end.date:
        dates.append(StarDate(current_time, init.format))
        current_time += resolution
    return dates


@dataclass
class StarInterval:
    scrap_date_list: List[Tuple[datetime, datetime]]
    resolution: Union[timedelta, relativedelta] = field(default=timedelta(days=1))
    format: str = field(default="%Y%m%d")

    def __post_init__(self) -> None:
        self.interval: List[StarDate] = []
        for init, end in self.scrap_date_list:
            self.interval.extend(
                interval_time(
                    StarDate(init, self.format),
                    StarDate(end, self.format),
                    self.resolution,
                )
            )

    def __iter__(self):
        return iter(self.interval)


def mega_interval(*args) -> List[StarDate]:
    assert isinstance(args[0], tuple), "Not valid non-tuple argument"
    return [*chain.from_iterable([StarInterval(*arg) for arg in args])]


## Decorator for connection error
def handle_client_connection_error(
    default_cooldown: int, max_retries: int = 100, increment="exp"
) -> Callable:
    assert increment in ["exp", "linear"]

    VALID_HANDLE: Dict[str, Callable[[int], Union[int, float]]] = {
        "exp": lambda retries: 2**retries + default_cooldown,
        "linear": lambda retries: 2 * retries + default_cooldown,
    }

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except (ClientConnectionError, asyncio.TimeoutError) as e:
                    retry_in = VALID_HANDLE[increment](retries)
                    print(f"Attempt {retries} failed.")
                    print(
                        f"Connection error encountered: {e}, retrying in {retry_in} seconds.."
                    )
                    await asyncio.sleep(retry_in)
                    retries += 1
            print(f"Max retries ({max_retries}) reached. Operation failed.")
            raise ClientConnectionError("Max retries exceeded.")

        return wrapper

    return decorator


@dataclass
class StarImage:
    new_scrap_date_list: List[StarDate] = field(default_factory = list)
    root_path: str = field(default = './data')
    batch_size: int = field(default = 10)

    def _path_prep(self, scrap_date: List[Tuple[datetime, datetime]]) -> List[str]: raise NotImplemented("_path_prep not implemented")

    def process_image(self, content: bytes):
        image = Image.open(io.BytesIO(content))
        return np.array(image)

    async def load_npy_from_png(self, path: str) -> NDArray:
        async with aiofiles.open(path, mode="rb") as file:
            content = await file.read()

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            array = await loop.run_in_executor(pool, StarImage.process_image, content)

        return array

    async def async_numpy(self, scrap_date: List[Tuple[datetime, datetime]]) -> NDArray:
        paths: List[str] = self._path_prep(scrap_date)
        return np.stack(
            await asyncio.gather(
                *[self.load_npy_from_png(path) for path in paths]
            ),
            axis=0,
        )

    async def async_torch(self, scrap_date: List[Tuple[datetime, datetime]]) -> Tensor:
        return torch.from_numpy(await self.async_numpy(scrap_date))

    def get_torch(self, scrap_date: List[Tuple[datetime, datetime]]) -> Tensor:
        return asyncio.run(self.async_torch(scrap_date))

    def get_numpy(self, scrap_date: List[Tuple[datetime, datetime]]) -> NDArray:
        return asyncio.run(self.async_numpy(scrap_date))


## Async handling

async def coroutine_handler(function: Callable[..., Any], *args: Any) -> Any:
    if iscoroutinefunction(function):
        return await function(*args)
    else:
        return function(*args)

