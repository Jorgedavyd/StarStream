from io import BytesIO
from astropy.io import fits
from starstream._base import CSV
from starstream.downloader import DataDownloading
from starstream._utils import (
    StarDate,
    asyncFITS,
    asyncGZFITS,
    datetime,
    StarInterval,
    download_url_prep,
    handle_client_connection_error,
)
from starstream.typing import ScrapDate
from ._base import Img
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
import numpy as np
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

def url(date: str) -> str:
    yyyydoy: str = date_to_day_of_year(date)
    doy: str = yyyydoy[-3:]
    return f"https://lasp.colorado.edu/eve/data_access/eve_data/products/level2b/{date[:4]}/{doy}/EVL_L2B_{yyyydoy}_008_01.fit.gz"

class Base(Img):
    def __init__(self, download_path: str, batch_size: int) -> None:
        super().__init__(download_path, batch_size)

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

    def _path_prep(self, scrap_date: List[Tuple[datetime, datetime]]) -> List[str]:
        new_scrap_date: StarInterval = StarInterval(scrap_date)
        return [
            *chain.from_iterable(
                [glob.glob(self.scrap_path(date.str())) for date in new_scrap_date]
            )
        ]


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

        min_step_size: timedelta = timedelta(seconds=36)

        def __init__(
            self,
            wavelength: Union[str, int],
            download_path: str = "./data/SDO_HR/",
            batch_size: int = 10,
            resolution: timedelta = timedelta(minutes=5),
        ) -> None:
            assert (
                0 < batch_size <= 10
            ), "Not valid batch_size, should be between 0 and 10"
            assert (
                str(wavelength) in self.wavelengths
            ), f"Not valid wavelength: {self.wavelengths}"
            assert (
                resolution > self.min_step_size
            ), "Not valid step size, extremely high resolution"
            self.resolution = resolution
            self.wavelength = wavelength
            self.url = (
                lambda date, name: f"http://jsoc2.stanford.edu/data/aia/images/{date[:4]}/{date[4:6]}/{date[6:]}/{wavelength}/{name}"
            )
            super().__init__(osp.join(download_path, str(wavelength)), batch_size)

            self.scrap_path: Callable[[str], str] = lambda date: osp.join(
                self.root_path, f"{date}*.jp2"
            )

            self.name = (
                lambda webname: "-".join(
                    [item.replace("_", "") for item in webname.split("__")[:-1]]
                )
                + ".jp2"
            )

            self.path: Callable[[str], str] = lambda name: osp.join(
                self.root_path, self.name(name)
            )

        def check_tasks(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
            new_scrap_date: StarInterval = StarInterval(scrap_date)
            for date in new_scrap_date:
                if len(glob.glob(self.scrap_path(date.str()))) < (
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
            day = date.str()
            url = self.url(day, "")
            async with client.get(url) as response:
                if response.status != 200:
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {day}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                else:
                    data = await response.text()
                    if "404 not found" in data:
                        print(
                            f"{self.__class__.__name__}: Data not available for date: {day}, queried url: {url}"
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
                self.path(name), "wb"
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
            super().__init__(
                download_path=osp.join(download_path, wavelength), batch_size=batch_size
            )
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

    class EVE(CSV):
        def __init__(
            self,
            root: str = "./data/SDO/EVE",
            batch_size: int = 1,
        ) -> None:
            super().__init__(
                root=root,
                batch_size=batch_size,
            )
            self.url: Callable[[str], str] = lambda date: f"https://lasp.colorado.edu/eve/data_access/eve_data/products/level3/{date[:4]}/EVE_L3_{date_to_day_of_year(date)}_008_01.fit"

        def _interval_setup(self, scrap_date: ScrapDate) -> None:
            super()._interval_setup(scrap_date)
            self.paths = [self.filepath(date.str()) for date in self.dates]
            self.urls = [self.url(date.str()) for date in self.dates]

        async def _scrap_(self, idx: int) -> None:
            _ = idx

        async def _download_(self, idx: int) -> None:
            await download_url_prep(self, idx, asyncFITS, self.preprocessing, idx)

        async def _prep_(self, idx: int) -> None:
            _ = idx

        def preprocessing(self, hdul, idx: int) -> None:
            date: str = self.dates[idx].str()
            data = hdul[8].data
            columns: List[str] = ['MEGSB_LINE_IRRADIANCE', 'MEGSB_LINE_PRECISION', 'MEGSB_LINE_ACCURACY', 'MEGSB_LINE_STDEV']
            if data is not None:
                data = data[columns].astype(np.float32)
            df: pl.DataFrame = pl.from_numpy(data, schema = columns)

            df = df.with_columns(
                [
                    pl.struct(["YEAR", "DOY", ""])
                    .apply(
                        lambda row: pl.datetime(year=int(row["YEAR"]), month=1, day=1)
                        + pl.duration(
                            days=int(row["DOY"]) - 1,
                            seconds=int(row["SOD"]),
                            microseconds=int((row["SOD"] - int(row["SOD"])) * 1e6),
                        )
                    )
                    .alias("date")
                ]
            )

            df = df.select(["date"])
            df = df.set_sorted("date")
            df.write_csv(self.filepath(date))
