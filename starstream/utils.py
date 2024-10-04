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
from typing import Dict, Tuple, List, Any, Union
from inspect import iscoroutinefunction
import aiohttp

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


def flare_hour_steps(
    init_hour: datetime, last_hour: datetime, step_size: timedelta, int_seq_len: int
):
    assert step_size >= timedelta(minutes=2), "2 minutes is the highest resolution"
    hours = []
    current_hour = init_hour
    while current_hour <= last_hour:
        hours.extend(
            [current_hour - step * step_size for step in range(int_seq_len, -1, -1)]
        )
        current_hour += timedelta(hours=1)
    return [datetime.strftime(iters, "%Y%m%d-%H%M%S").split("-") for iters in hours]


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


def interval_time(
    start_date_str,
    end_date_str,
    format="%Y%m%d",
    step_size: timedelta = timedelta(days=1),
    output_format=None,
) -> List[str]:
    if output_format is None:
        output_format = format

    start_date = datetime.strptime(start_date_str, format)
    end_date = datetime.strptime(end_date_str, format)

    current_date = start_date
    date_list = []

    while current_date <= end_date:
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

async def downloader(scrap_date, sat_objs) -> None:
    async with aiohttp.ClientSession() as session:
        if isinstance(sat_objs, (list, tuple)):
            await asyncio.gather(
                *[
                    sat_obj.downloader_pipeline(scrap_date, session)
                    for sat_obj in sat_objs
                ]
            )
        else:
            await sat_objs.downloader_pipeline(scrap_date, session)


def DataDownloading(
    sat_objs: Union[List, Any],
    scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]],
) -> None:
    if isinstance(scrap_date[0], datetime):
        asyncio.run(downloader(scrap_date, sat_objs))
    else:
        for date in scrap_date:
            asyncio.run(downloader(date, sat_objs))

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
