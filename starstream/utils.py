from collections.abc import Callable
from datetime import datetime, timedelta
import functools
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
import aiohttp
from dataclasses import dataclass, field
from itertools import chain


__all__ = ["DataDownloading"]


def separe_interval(init: datetime, end: datetime, step_size: timedelta):
    intervals = []
    current_date = init
    while current_date <= end:
        intervals.append((current_date, current_date + step_size))
        current_date += step_size + timedelta(days=1)
    return intervals


def hour_steps(init_hour: datetime, last_hour: datetime, step_size: timedelta):
    assert step_size >= timedelta(minutes=2), "2 minutes is the highest resolution"
    hours = []
    current_hour = init_hour
    while current_hour <= last_hour:
        hours.append(current_hour)
        current_hour += step_size
    return [datetime.strftime(iters, "%Y%m%d-%H%M%S").split("-") for iters in hours]


def l1_hour_steps(
    init_hour: datetime, last_hour: datetime, seq_len: timedelta = timedelta(hours=2)
):
    init_hour = init_hour - seq_len
    return datetime.strftime(init_hour, "%Y%m%d-%H%M%S"), datetime.strftime(
        last_hour, "%Y%m%d-%H%M%S"
    )


def scrap_date_to_month(scrap_date: List[str]) -> List[str]:
    return [day[:6] for day in scrap_date]


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

def datetime_interval(
    init: datetime,
    last: datetime,
    step_size: Union[relativedelta, timedelta],
    output_format: str = "%Y%m%d",
) -> List[str]:
    current_date = init
    date_list = []
    while current_date <= last:
        date_list.append(current_date.strftime(output_format))
        current_date += step_size
    return date_list


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

@dataclass(frozen=True)
class StarDate:
    date: datetime
    format: Optional[str] = field(default = None)

    def str(self, format: Optional[str]) -> str:
        if format is None:
            assert (self.format is not None), "String format not defined"
            return self.date.strftime(self.format)
        return self.date.strftime(format)

def interval_time(init: StarDate, end: StarDate, resolution: timedelta) -> List[StarDate]:
    current_time = init.date
    dates = []
    while current_time < end.date:
        dates.append(StarDate(current_time, init.format))
        current_time += resolution
    return dates

@dataclass
class StarInterval:
    lower_boundary: StarDate
    upper_boundary: StarDate
    resolution: timedelta = field(default=timedelta(hours=1))

    def __post_init__(self) -> None:
        assert self.lower_boundary.format == self.upper_boundary.format, "Boundaries' format must be equal"
        self.interval: List[StarDate] = interval_time(self.lower_boundary, self.upper_boundary, self.resolution)

    def __iter__(self):
        return iter(self.interval)

def mega_interval(*args) -> List[StarDate]:
    assert (isinstance(args[0], tuple)), "Not valid non-tuple argument"
    return [*chain.from_iterable([StarInterval(*arg) for arg in args])]

def DataDownloading(
    sat_objs: Union[List, Any],
    scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]],
) -> None:
    asyncio.run(downloader(sat_objs, scrap_date))

async def downloader(
    sat_objs: Union[List, Any],
    scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]],
) -> None:
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[satellite(scrap_date, session) for satellite in sat_objs])

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
