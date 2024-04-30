from datetime import datetime, timedelta
from astropy.io import fits
from spacepy import pycdf
from io import BytesIO
import zipfile
import asyncio
import tarfile
import gzip
from inspect import iscoroutinefunction
import aiohttp
from typing import Tuple, List, Union


def separe_interval(init, end, step_size):
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
    init_hour = datetime.strptime(init_hour, "%Y%m%d") - seq_len
    return datetime.strftime(init_hour, "%Y%m%d-%H%M%S"), datetime.strftime(
        last_hour, "%Y%m%d-%H%M%S"
    )


def scrap_date_to_month(scrap_date):
    return [(day[:4], day[4:6]) for day in scrap_date]


async def asyncCDF(cdf_path: str, processing, *args):
    cdf_file = await asyncio.get_event_loop().run_in_executor(None, pycdf.CDF, cdf_path)
    if iscoroutinefunction(processing):
        out = await processing(cdf_file, *args)
    else:
        out = processing(cdf_file, *args)
    cdf_file.close()
    return out


async def asyncZIP(bytes_obj: BytesIO, processing, *args):
    zip_file = await asyncio.get_event_loop().run_in_executor(
        None, zipfile.ZipFile, bytes_obj
    )
    if iscoroutinefunction(processing):
        out = await processing(zip_file, *args)
    else:
        out = processing(zip_file, *args)
    zip_file.close()
    return out


async def asyncGZ(bytes_obj: BytesIO, processing, *args):
    gz_file = await asyncio.get_event_loop().run_in_executor(None, syncGZ, bytes_obj)
    if iscoroutinefunction(processing):
        out = await processing(gz_file, *args)
    else:
        out = processing(gz_file, *args)
    gz_file.close()
    return out


async def asyncGZFITS(bytes_obj: BytesIO, processing, *args):
    gz_file = await asyncio.get_event_loop().run_in_executor(None, syncGZ, bytes_obj)
    await asyncFITS(BytesIO(gz_file.read()), processing, *args)
    gz_file.close()


def syncGZ(file_obj):
    return gzip.GzipFile(fileobj=file_obj)


async def asyncTAR(bytes_obj: BytesIO, processing, *args):
    tar_file = await asyncio.get_event_loop().run_in_executor(None, syncTAR, bytes_obj)
    if iscoroutinefunction(processing):
        out = await processing(tar_file, *args)
    else:
        out = processing(tar_file, *args)
    tar_file.close()
    return out


def syncTAR(file_obj):
    return tarfile.open(fileobj=file_obj, mode="r")


async def asyncFITS(bytes_obj: BytesIO, processing, *args):
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
    init_hour: datetime, last_hour: datetime, step_size, int_seq_len: int
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
    init: datetime, last: datetime, step_size: timedelta, output_format: str = "%Y%m%d"
):
    current_date = init
    date_list = []
    while current_date <= last:
        date_list.append(current_date.date().strftime(output_format))
        current_date += step_size
    return date_list


def interval_time(
    start_date_str,
    end_date_str,
    format="%Y%m%d",
    step_size: timedelta = timedelta(days=1),
    output_format=None,
):
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


def timedelta_to_freq(timedelta_obj):
    total_seconds = timedelta_obj.total_seconds()

    if total_seconds % 1 != 0:
        raise ValueError("Timedelta must represent a whole number of seconds")

    days = total_seconds // (24 * 3600)
    hours = (total_seconds % (24 * 3600)) // 3600
    minutes = ((total_seconds % (24 * 3600)) % 3600) // 60
    seconds = ((total_seconds % (24 * 3600)) % 3600) % 60

    freq_str = ""

    if days > 0:
        freq_str += str(int(days)) + "D"
    if hours > 0:
        freq_str += str(int(hours)) + "H"
    if minutes > 0:
        freq_str += str(int(minutes)) + "T"
    if seconds > 0:
        freq_str += str(int(seconds)) + "S"

    return freq_str


class DataDownloading:
    async def _download(
        self, sat_objs: list, scrap_date_list: List[Tuple[datetime, datetime]]
    ):
        for scrap_date in scrap_date_list:
            async with aiohttp.ClientSession() as session:
                await asyncio.gather(
                    *[
                        sat_obj.downloader_pipeline(scrap_date, session)
                        for sat_obj in sat_objs
                    ]
                )

    def __call__(
        self, sat_objs: list, scrap_date_list: List[Tuple[datetime, datetime]]
    ):
        asyncio.run(self._download(sat_objs, scrap_date_list))
