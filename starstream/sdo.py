from astropy.io import fits
from starstream._base import CSV
from .utils import (
    StarDate,
    datetime,
    StarInterval,
    timedelta_to_freq,
    handle_client_connection_error,
)
from ._base import StarImage
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from itertools import chain
from tqdm import tqdm
import aiofiles
import asyncio
import glob
import os
from typing import Callable, Coroutine, List, Optional, Tuple, Union
import os.path as osp
import polars as pl
import gzip

"""
http://jsoc.stanford.edu/data/aia/synoptic/mostrecent/
"""

__all__ = ["SDO"]


def date_to_day_of_year(date_string):
    date_object = datetime.strptime(date_string, "%Y%m%d")
    day_of_year = date_object.timetuple().tm_yday
    day_of_year_string = f"{day_of_year:03d}"
    return date_string[:4] + day_of_year_string


class Base(StarImage):
    async def fetch(
        self,
        scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]],
        session,
    ):
        if isinstance(scrap_date[0], datetime):
            scrap_date = [scrap_date]
        self.check_tasks(scrap_date)
        await self.batched_download(session)

    async def batched_download(self, client) -> None:
        params = []
        for i in tqdm(
            range(0, len(self.new_scrap_date_list), self.batch_size),
            desc="Getting file names...",
        ):
            params.extend(
                [
                    *chain.from_iterable(
                        await asyncio.gather(
                            *[
                                self.scrap_names(date, client)
                                for date in self.new_scrap_date_list[
                                    i : i + self.batch_size
                                ]
                            ]
                        )
                    )
                ]
            )

        params = [i for i in params if i is not None]
        for i in tqdm(range(0, len(params), self.batch_size), desc="Getting images..."):
            await asyncio.gather(
                *[
                    self.download_from_name(name, client)
                    for name in params[i : i + self.batch_size]
                ]
            )

class SDO:
    class AIA_HR(Base):
        wavelengths: List[str] = [
            "94",
            "131",
            "171",
            "193",
            "211",
            "304",
            "335",
            "1600",
            "1700",
            "4500",
        ]

        min_step_size: timedelta = timedelta(seconds = 36)

        def __init__(
            self,
            wavelength: Union[str, int],
            download_path: str = "./data/SDO_HR/",
            batch_size: int = 10,
            resolution: timedelta = timedelta(minutes = 5),
        ) -> None:
            assert (0 < batch_size <= 10), "Not valid batch_size, should be between 0 and 10"
            assert (str(wavelength) in self.wavelengths), f"Not valid wavelength: {self.wavelengths}"
            assert (resolution > self.min_step_size), "Not valid step size, extremely high resolution"
            self.resolution = resolution
            self.wavelength = wavelength
            self.url = (
                lambda date, name: f"http://jsoc2.stanford.edu/data/aia/images/{date[:4]}/{date[4:6]}/{date[6:]}/{wavelength}/{name}"
            )
            super().__init__(osp.join(download_path, str(wavelength)), batch_size)

            self.jp2_path: Callable[[str], str] = lambda date: osp.join(
                self.root_path, f"{date}*.jp2"
            )

            self.name = (
                lambda webname: "-".join(
                    [item.replace("_", "") for item in webname.split("__")[:-1]]
                )
                + ".jp2"
            )

        def check_tasks(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
            new_scrap_date: StarInterval = StarInterval(scrap_date)
            for date in new_scrap_date:
                if len(glob.glob(self.jp2_path(date.str()))) < (
                    (timedelta(days=1) / self.resolution) - 1
                ):
                    self.new_scrap_date_list.append(date)

        async def get_names(self, html):
            loop = asyncio.get_event_loop()
            soup = await loop.run_in_executor(None, BeautifulSoup, html, "html.parser")
            scrap = await loop.run_in_executor(None, self.find_all, soup)
            names = [name["href"] for name in scrap]
            return [
                names[i]
                for i in range(0, len(names), self.resolution // self.min_step_size)
            ]

        @handle_client_connection_error(
            increment="exp", default_cooldown=5, max_retries=3
        )
        async def scrap_names(self, date: StarDate, client):
            url = self.url(date, "")
            async with client.get(url) as response:
                if response.status != 200:
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                else:
                    data = await response.text()
                    if "404 not found" in data:
                        print(
                            f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                        )
                        self.new_scrap_date_list.remove(date)
                        return
                    names = await self.get_names(data)
                    return names

        def find_all(self, soup):
            return soup.find_all("a", href=lambda href: href.endswith(".jp2"))

        @handle_client_connection_error(
            increment="exp", default_cooldown=5, max_retries=3
        )
        async def download_from_name(self, name, client):
            date = name.split("_")[0]
            url = self.url(date, name)
            async with client.get(url, ssl=False) as response, aiofiles.open(
                self.jp2_path(self.name(name)), "wb"
            ) as f:
                await f.write(await response.read())

    class AIA_LR(Base):
        wavelengths: List[str] = [
            "0094",
            "0131",
            "0171",
            "0193",
            "0211",
            "0304",
            "0335",
            "1600",
            "1700",
            "4500",
        ]

        def __init__(
            self,
            wavelength: str,
            download_path: str = "./data/AIA_LR",
            batch_size: int = 256,
        ) -> None:
            assert (
                wavelength in self.wavelengths
            ), f"Not valid wavelength: {self.wavelengths}"
            super().__init__(osp.join(download_path, wavelength), batch_size)
            self.wavelength: str = wavelength
            self.url: Callable[[str, str], str] = (
                lambda date, name: f"https://sdo.gsfc.nasa.gov/assets/img/browse/{date[:4]}/{date[4:6]}/{date[6:]}/{name}"
            )

            self.scrap_path: Callable[[str], str] = lambda date: osp.join(
                self.root_path, f"{date}*.jpg"
            )
            self.jpg_path: Callable[[str], str] = lambda name: osp.join(
                self.root_path, name
            )
            self.name: Callable[[str], str] = (
                lambda webname: "-".join(webname.split("_")[:2]) + ".jpg"
            )

        def check_tasks(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
            print(
                f"{self.__class__.__name__}: Looking for the links of missing dates..."
            )
            new_scrap_date: StarInterval = StarInterval(scrap_date)
            for date in new_scrap_date:
                if len(glob.glob(self.scrap_path(date.str()))) == 0:
                    self.new_scrap_date_list.append(date)

        async def get_names(self, html) -> List[str]:
            loop = asyncio.get_event_loop()
            soup = await loop.run_in_executor(None, BeautifulSoup, html, "html.parser")
            scrap = await loop.run_in_executor(None, self.find_all, soup)
            return [name["href"] for name in scrap]

        @handle_client_connection_error(
            increment="exp", default_cooldown=5, max_retries=3
        )
        async def scrap_names(self, date: StarDate, client):
            url = self.url(date.str(), "")
            async with client.get(url) as response:
                if response.status != 200:
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                else:
                    data = await response.text()
                    if "404 not found" in data:
                        print(
                            f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                        )
                        self.new_scrap_date_list.remove(date)
                        return
                    names = await self.get_names(data)
                    return names

        def find_all(self, soup) -> List:
            return soup.find_all(
                "a", href=lambda href: href.endswith(f"512_{self.wavelength}.jpg")
            )

        async def download_from_name(self, name: str, client) -> None:
            date = name.split("_")[0]
            url = self.url(date, name)
            async with client.get(url) as response, aiofiles.open(
                self.jpg_path(self.name(name)), "wb"
            ) as f:
                await f.write(await response.read())

        def _path_prep(self, scrap_date: List[Tuple[datetime, datetime]]) -> List[str]:
            new_scrap_date: StarInterval = StarInterval(scrap_date)
            return [
                *chain.from_iterable(
                    [glob.glob(self.scrap_path(date.str())) for date in new_scrap_date]
                )
            ]

    class EVE(CSV):
        def __init__(
            self,
            download_path: str = "./data/SDO/EVE",
            batch_size: int = 10,
            store_resolution: Optional[timedelta] = None,
        ) -> None:
            if store_resolution is not None:
                self.resolution = timedelta_to_freq(store_resolution)
            self.batch_size: int = batch_size
            self.url: Callable[[str], str] = (
                lambda date: f"https://lasp.colorado.edu/eve/data_access/eve_data/products/level1/esp/{date[:4]}/esp_L1_{date_to_day_of_year(date)}_008.fit.gz"
            )
            self.csv_path: Callable[[str], str] = lambda date: osp.join(
                download_path, f"{date}.csv"
            )

            self.gz_path: Callable[[str], str] = lambda date: osp.join(
                download_path, f"{date}.fits.gz"
            )
            self.fits_path: Callable[[str], str] = lambda date: osp.join(
                download_path, f"{date}.fits"
            )
            os.makedirs(download_path, exist_ok=True)

        def to_csv(self, date: str) -> None:
            path: str = self.fits_path(date)
            with fits.open(path) as hdul:
                df: pl.DataFrame = pl.DataFrame(hdul[1].data)

            df = df.with_columns(
                [
                    pl.struct(["YEAR", "DOY", "SOD"])
                    .apply(
                        lambda row: pl.datetime(year=int(row["YEAR"]), month=1, day=1)
                        + pl.duration(
                            days=int(row["DOY"]) - 1,
                            seconds=int(row["SOD"]),
                            microseconds=int((row["SOD"] - int(row["SOD"])) * 1e6),
                        )
                    )
                    .alias("datetime")
                ]
            )

            df = df.select(["datetime", "CH_18", "CH_26", "CH_30", "Q_1", "Q_2", "Q_3"])
            df = df.set_sorted("datetime")

            if resolution:=getattr(self, "resolution", False):
                df = df.groupby_dynamic("datetime", every= timedelta_to_freq(resolution)).agg(
                    [
                        pl.col("CH_18").mean(),
                        pl.col("CH_26").mean(),
                        pl.col("CH_30").mean(),
                        pl.col("Q_1").mean(),
                        pl.col("Q_2").mean(),
                        pl.col("Q_3").mean(),
                    ]
                )

            df.write_csv(self.csv_path(date))
            os.remove(self.fits_path(date))

        async def to_fits(self, date: str) -> None:
            path: str = self.gz_path(date)
            gzip_file = gzip.open(path)
            fits_file = gzip_file.read()
            gzip_file.close()
            os.remove(path)
            async with aiofiles.open(path[:-3], "xb") as file:
                await file.write(fits_file)

        async def preprocess(self, date: StarDate) -> None:
            await self.to_fits(date.str())
            self.to_csv(date.str())

        def _get_preprocessing_tasks(self) -> List[Coroutine]:
            return [self.preprocess(date) for date in self.new_scrap_date_list]

        async def download_url(self, session, date: StarDate) -> None:
            day = date.str()
            url = self.url(day)
            async with session.get(url, ssl=False) as response:
                data = await response.read()
                async with aiofiles.open(self.gz_path(day), "wb") as file:
                    await file.write(data)
