from collections.abc import Callable
from datetime import datetime, timedelta
import functools
import aiofiles
from tqdm import tqdm
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
from typing import (
    Coroutine,
    Dict,
    Optional,
    Sequence,
    Tuple,
    List,
    Any,
    Union,
)
from dataclasses import dataclass, field
from itertools import chain
import polars as pl
from inspect import iscoroutinefunction
from concurrent.futures import ThreadPoolExecutor
from glob import glob
import os.path as osp

from starstream.typing import ScrapDate

## Asynchronous processing


def syncGZ(file_obj: BytesIO):
    return gzip.GzipFile(fileobj=file_obj)


def syncTAR(file_obj: BytesIO):
    return tarfile.open(fileobj=file_obj, mode="r")


async def asyncGeneral(
    obj: Union[str, BytesIO],
    processing: Optional[Callable],
    read_method: Callable,
    *args,
) -> None:
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as thread:
        general_file = await loop.run_in_executor(thread, read_method, obj)
    if processing is not None:
        return await coroutine_handler(processing, general_file, *args)
    else:
        return general_file


async def asyncCDF(
    obj: Union[str, BytesIO], processing: Optional[Callable] = None, *args
) -> Any:
    return await asyncGeneral(obj, processing, pycdf.CDF, *args)


async def asyncZIP(
    obj: Union[str, BytesIO], processing: Optional[Callable] = None, *args
) -> Any:
    return await asyncGeneral(obj, processing, zipfile.ZipFile, *args)


async def asyncGZIP(
    obj: Union[str, BytesIO], processing: Optional[Callable] = None, *args
) -> Any:
    return await asyncGeneral(obj, processing, syncGZ, *args)


async def asyncFITS(
    obj: Union[str, BytesIO], processing: Optional[Callable] = None, *args
) -> Any:
    return await asyncGeneral(obj, processing, fits.open, *args)


async def asyncGZFITS(obj: Union[str, BytesIO], processing: Callable, *args) -> None:
    gz_obj = await asyncGZIP(obj, lambda gzip_file: gzip_file.read())
    return await asyncFITS(BytesIO(gz_obj), processing, *args)


async def asyncTAR(obj: Union[str, BytesIO], processing: Callable, *args) -> Any:
    return await asyncGeneral(obj, processing, syncTAR, *args)


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


## Async handling


async def coroutine_handler(function: Union[Callable, Coroutine], *args: Any) -> Any:
    if iscoroutinefunction(function):
        return await function(*args)
    else:
        return function(*args)


## Utils for scrap_date
def assert_scrap_date(
    scrap_date: Union[Sequence[Sequence[datetime]], Sequence[datetime]]
):
    if isinstance(scrap_date[0], datetime):
        interval_len: int = len(scrap_date)
        assert (
            len(scrap_date) == 2
        ), f"The length of the interval must be 2, got {interval_len}"
    elif isinstance(scrap_date[0], (list, tuple, set)):
        for interval in scrap_date:
            assert_scrap_date(interval)


def create_scrap_date(scrap_date: ScrapDate):
    assert_scrap_date(scrap_date)
    if isinstance(scrap_date[0], datetime):
        return [scrap_date]
    return scrap_date


## Utils for downloading


async def async_batch(self, methods: Sequence[str], desc: str) -> None:
    for idx in tqdm(range(0, len(self.dates), self.batch_size), desc=desc):
        for method in methods:
            await asyncio.gather(
                *[
                    coroutine_handler(getattr(self, method), idx)
                    for idx in range(idx, self.batch_size + idx)
                ]
            )


# Utilities for downloading
## Decorator for connection error
def handle_client_connection_error(
    default_cooldown: int, max_retries: int = 100, increment="linear"
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


def not_valid_query(self, url: str) -> None:
    print(f"{self.__class__.__name__}: Data not available for queried url {url}")


async def check_response_bytes(self, response, url: str) -> Any:
    if response is not None:
        content = await response.read()
        if (
            response.status != 200
            or content.startswith(b"<html>")
            or b"404 Not Found" in content
        ):
            not_valid_query(self, url)
            return
        return content


async def check_response_text(self, response, url: str) -> Any:
    if response is not None:
        content = await response.text()
        if response.status != 200 or "404 Not Found" in content:
            not_valid_query(self, url)
        return content


@handle_client_connection_error(default_cooldown=5, increment="exp", max_retries=5)
async def download_url_write(self, idx: int) -> None:
    try:
        url: str = self.urls[idx]
        async with self.session.get(url, ssl=False) as response:
            content = await check_response_bytes(self, response, url)
            if content is not None:
                async with aiofiles.open(self.paths[idx], "wb") as f:
                    await f.write(content)
    except IndexError:
        return


@handle_client_connection_error(default_cooldown=5, increment="exp", max_retries=5)
async def download_url_prep(
    self, idx: int, method: Union[Callable, Coroutine], *args
) -> Any:
    try:
        url: str = self.urls[idx]
        async with self.session.get(url, ssl=False) as response:
            content = await check_response_bytes(self, response, url)
            if content is not None:
                return await coroutine_handler(method, BytesIO(content), *args)
    except IndexError:
        return


@handle_client_connection_error(default_cooldown=5, increment="exp", max_retries=5)
async def scrap_url_default(
    self, idx: int, method: Union[Callable, Coroutine], *args: Any
) -> Any:
    try:
        url: str = self.scrap_urls[idx]
        async with self.session.get(url, ssl=False) as response:
            content = await check_response_text(self, response, url)
            if content is not None:
                return await coroutine_handler(method, content, *args)
    except IndexError:
        return


def find_files_glob(self, date: str) -> Tuple[bool, List[str]]:
    out = glob(self.scrap_path(date))
    return bool(len(out)), out


def find_files_daily(self, date: str) -> bool:
    return osp.exists(self.scrap_path(date))
