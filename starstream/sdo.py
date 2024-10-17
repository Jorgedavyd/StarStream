from starstream._base import CSV
from starstream._utils import (
    asyncFITS,
    datetime,
    download_url_prep,
    download_url_write,
    scrap_url_default,
)
from starstream.typing import ScrapDate
from ._base import Img
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import asyncio
from typing import Callable, List
import os.path as osp
import polars as pl
import numpy as np

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
    def __init__(
        self,
        wavelength: str,
        root: str,
        batch_size: int,
        filepath: Callable,
        url: Callable,
    ) -> None:
        super().__init__(
            root=root,
            batch_size=batch_size,
            filepath=filepath,
        )
        self.url = url
        self.wavelength = wavelength
        self.scrap_url: Callable[[str], str] = lambda date: self.url(date, "")

    def find_all(self, soup) -> List[str]:
        _ = soup
        raise NotImplemented("find_all")

    def _interval_setup(self, scrap_date: ScrapDate) -> None:
        super()._interval_setup(scrap_date)
        self.scrap_urls: List[str] = [self.scrap_url(date.str()) for date in self.dates]

    async def _scrap_(self, idx: int):
        await scrap_url_default(self, idx, self.manipulate_html, idx)

    async def _download_(self, idx: int) -> None:
        await download_url_write(self, idx)

    async def _prep_(self, idx: int) -> None:
        _ = idx


class SDO:
    class AIA_HR(Base):
        valid_wavelengths: List[str] = [
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
            wavelength: int,
            root: str = "./data/SDO/AIA_HR/",
            batch_size: int = 10,
            resolution: timedelta = timedelta(minutes=5),
        ) -> None:
            assert (
                0 < batch_size <= 10
            ), "Not valid batch_size, should be between 0 and 10"
            assert (
                resolution > self.min_step_size
            ), "Not valid step size, extremely high resolution"
            super().__init__(
                str(wavelength),
                osp.join(root, str(wavelength)),
                batch_size,
                lambda name: osp.join(root, str(wavelength), self.name(name)),
                lambda date, name: f"http://jsoc2.stanford.edu/data/aia/images/{date[:4]}/{date[4:6]}/{date[6:]}/{wavelength}/{name}",
            )
            self.resolution = resolution
            self.scrap_path: Callable[[str], str] = lambda date: osp.join(
                self.root, f"{date}*.jp2"
            )

            self.name = (
                lambda webname: "-".join(
                    [item.replace("_", "") for item in webname.split("__")[:-1]]
                )
                + ".jp2"
            )

        async def manipulate_html(self, html, idx: int):
            date: str = self.dates[idx].str()
            loop = asyncio.get_event_loop()
            soup = await loop.run_in_executor(None, BeautifulSoup, html, "html.parser")
            scrap = await loop.run_in_executor(None, self.find_all, soup)
            names = [name["href"] for name in scrap]
            names = [
                names[i]
                for i in range(0, len(names), self.resolution // self.min_step_size)
            ]
            self.urls.extend([self.url(date, name) for name in names])
            self.paths.extend([self.filepath(name) for name in names])

        def find_all(self, soup):
            return soup.find_all("a", href=lambda href: href.endswith(".jp2"))

    class AIA_LR(Base):
        valid_wavelengths: List[str] = [
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
            wavelength: int,
            root: str = "./data/AIA_LR",
            batch_size: int = 256,
        ) -> None:
            assert (
                f"{wavelength:04}" in self.valid_wavelengths
            ), f"Not valid wavelength: {self.valid_wavelengths}"
            super().__init__(
                f"{wavelength:04}",
                root=osp.join(root, str(wavelength)),
                batch_size=batch_size,
                filepath=lambda name: osp.join(root, str(wavelength), self.name(name)),
                url=lambda date, name: f"https://sdo.gsfc.nasa.gov/assets/img/browse/{date[:4]}/{date[4:6]}/{date[6:]}/{name}",
            )

            self.name: Callable[[str], str] = (
                lambda webname: "-".join(webname.split("_")[:2]) + ".jpg"
            )

            self.scrap_path: Callable[[str], str] = lambda date: osp.join(
                self.root, f"{date}*.jpg"
            )

        def find_all(self, soup) -> List:
            return soup.find_all(
                "a", href=lambda href: href.endswith(f"512_{self.wavelength}.jpg")
            )

        async def manipulate_html(self, html, idx: int) -> None:
            date: str = self.dates[idx].str()
            loop = asyncio.get_event_loop()
            soup = await loop.run_in_executor(None, BeautifulSoup, html, "html.parser")
            scrap = await loop.run_in_executor(None, self.find_all, soup)
            names = [name["href"] for name in scrap]
            self.urls.extend([self.url(date, name) for name in names])
            self.paths.extend([self.filepath(name) for name in names])

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
            self.url: Callable[[str], str] = (
                lambda date: f"https://lasp.colorado.edu/eve/data_access/eve_data/products/level3/{date[:4]}/EVE_L3_{date_to_day_of_year(date)}_008_01.fit"
            )

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
            columns: List[str] = [
                "MEGSB_LINE_IRRADIANCE",
                "MEGSB_LINE_PRECISION",
                "MEGSB_LINE_ACCURACY",
                "MEGSB_LINE_STDEV",
            ]
            data = np.stack([data[column] for column in columns], axis=-1)
            if data is not None:
                data = data.astype(np.float32).squeeze(0)
                df: pl.DataFrame = pl.from_numpy(data, schema=columns)
                df.write_csv(self.filepath(date))
