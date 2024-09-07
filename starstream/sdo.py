from .utils import (
    datetime_interval,
    timedelta_to_freq,
    asyncGZFITS,
    handle_client_connection_error,
)
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from itertools import chain
from io import BytesIO
from tqdm import tqdm
import pandas as pd
import aiofiles
import asyncio
import glob
import os
from typing import Callable, List, Tuple, Union
import os.path as osp

"""
http://jsoc.stanford.edu/data/aia/synoptic/mostrecent/
"""

__all__ = ["SDO"]


def date_to_day_of_year(date_string):
    date_object = datetime.strptime(date_string, "%Y%m%d")
    day_of_year = date_object.timetuple().tm_yday
    day_of_year_string = f"{day_of_year:03d}"
    return date_string[:4] + day_of_year_string


class SDO:
    class AIA_HR:
        min_step_size: timedelta = timedelta(seconds=36)
        batch_size = 10
        wavelengths: list = [
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

        def __init__(
            self,
            step_size: timedelta,
            wavelength: Union[str, int],
            download_path: str = "./data/SDO_HR/",
            batch_size: int = 10,
        ) -> None:
            assert 0 < batch_size <= 10
            assert (
                str(wavelength) in self.wavelengths
            ), f"Not valid wavelength: {self.wavelengths}"
            assert (
                step_size > self.min_step_size
            ), "Not valid step size, extremely high resolution"
            self.step_size = step_size
            self.wavelength = wavelength
            self.url = (
                lambda date, name: f"http://jsoc2.stanford.edu/data/aia/images/{date[:4]}/{date[4:6]}/{date[6:]}/{wavelength}/{name}"
            )
            self.root_path: str = osp.join(download_path, str(wavelength))
            self.scrap_path: Callable[[str], str] = lambda date: osp.join(
                self.root_path, f"{date}*.jp2"
            )
            self.jp2_path: Callable[[str], str] = lambda name: osp.join(
                self.root_path, f"{name}*.jp2"
            )
            self.name = (
                lambda webname: "-".join(
                    [item.replace("_", "") for item in webname.split("__")[:-1]]
                )
                + ".jp2"
            )
            os.makedirs(self.jp2_path(""), exist_ok=True)

        def check_tasks(self, scrap_date: Tuple[datetime, datetime]) -> None:
            print(f"{self.__class__.__name__}: Looking for missing dates...")
            new_scrap_date: List[str] = datetime_interval(
                *scrap_date, timedelta(days=1)
            )
            self.new_scrap_date_list: List[str] = [
                date
                for date in new_scrap_date
                if len(glob.glob(self.scrap_path(date)))
                < ((timedelta(days=1) / self.step_size) - 1)
            ]

        async def get_names(self, html):
            loop = asyncio.get_event_loop()
            soup = await loop.run_in_executor(None, BeautifulSoup, html, "html.parser")
            scrap = await loop.run_in_executor(None, self.find_all, soup)
            names = [name["href"] for name in scrap]
            return [
                names[i]
                for i in range(0, len(names), self.step_size // self.min_step_size)
            ]

        @handle_client_connection_error(
            increment="exp", default_cooldown=5, max_retries=3
        )
        async def scrap_names(self, date, client):
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

        async def batched_download(self, session):
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
                                    self.scrap_names(date, session)
                                    for date in self.new_scrap_date_list[
                                        i : i + self.batch_size
                                    ]
                                ]
                            )
                        )
                    ]
                )

            params = [i for i in params if i is not None]

            for i in tqdm(
                range(0, len(params), self.batch_size), desc="Getting images..."
            ):
                await asyncio.gather(
                    *[
                        self.download_from_name(name, session)
                        for name in params[i : i + self.batch_size]
                    ]
                )

        def data_prep(self, scrap_date: tuple[datetime, datetime]):
            new_scrap_date: List[str] = datetime_interval(
                *scrap_date, timedelta(days=1)
            )
            return [
                *chain.from_iterable(
                    [glob.glob(self.scrap_path(date)) for date in new_scrap_date]
                )
            ]

        async def downloader_pipeline(
            self, scrap_date: Tuple[datetime, datetime], session
        ):
            self.check_tasks(scrap_date)
            await self.batched_download(session)

    class AIA_LR:
        def __init__(
            self,
            wavelength: str,
            download_path: str = "./data/AIA_LR",
            batch_size: int = 256,
        ) -> None:
            self.root_path: str = osp.join(download_path, wavelength)
            self.batch_size: int = batch_size
            self.wavelengths: list = [
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
            assert (
                wavelength in self.wavelengths
            ), f"Not valid wavelength: {self.wavelengths}"
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
            os.makedirs(self.jpg_path(""), exist_ok=True)

        def check_tasks(self, scrap_date: Tuple[datetime, datetime]) -> None:
            print(
                f"{self.__class__.__name__}: Looking for the links of missing dates..."
            )
            new_scrap_date: List[str] = datetime_interval(
                *scrap_date, timedelta(days=1)
            )
            self.new_scrap_date_list = [
                date
                for date in new_scrap_date
                if len(glob.glob(self.scrap_path(date))) == 0
            ]

        async def get_names(self, html) -> List[str]:
            loop = asyncio.get_event_loop()
            soup = await loop.run_in_executor(None, BeautifulSoup, html, "html.parser")
            scrap = await loop.run_in_executor(None, self.find_all, soup)
            return [name["href"] for name in scrap]

        @handle_client_connection_error(
            increment="exp", default_cooldown=5, max_retries=3
        )
        async def scrap_names(self, date: str, client):
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
            for i in tqdm(
                range(0, len(params), self.batch_size), desc="Getting images..."
            ):
                await asyncio.gather(
                    *[
                        self.download_from_name(name, client)
                        for name in params[i : i + self.batch_size]
                    ]
                )

        def data_prep(self, scrap_date: Tuple[datetime, datetime]) -> List[str]:
            new_scrap_date = datetime_interval(*scrap_date, timedelta(days=1))
            return [
                *chain.from_iterable(
                    [glob.glob(self.scrap_path(date)) for date in new_scrap_date]
                )
            ]

        async def downloader_pipeline(
            self, scrap_date: tuple[datetime, datetime], session
        ):
            self.check_tasks(scrap_date)
            await self.batched_download(session)

    class EVE:
        def __init__(self) -> None:
            self.batch_size: int = 10
            self.url: Callable[[str], str] = (
                lambda date: f"https://lasp.colorado.edu/eve/data_access/eve_data/products/level1/esp/{date[:4]}/esp_L1_{date_to_day_of_year(date)}_007.fit.gz"
            )
            self.eve_csv_path: Callable[[str], str] = (
                lambda date: f"./data/SDO/EVE/{date}.csv"
            )
            self.eve_fits_gz_path: Callable[[str], str] = (
                lambda date: f"./data/SDO/EVE/{date}.fits.gz"
            )
            self.eve_path: str = "./data/SDO/EVE/"
            os.makedirs(self.eve_path, exist_ok=True)

        async def to_csv(self, fits_file, day) -> None:
            df: pd.DataFrame = await asyncio.get_event_loop().run_in_executor(
                None, pd.DataFrame, fits_file[1].data
            )
            df.index = await asyncio.get_event_loop().run_in_executor(
                None,
                df.apply,
                lambda row: datetime(int(row["YEAR"]), 1, 1)
                + timedelta(
                    days=int(row["DOY"]) - 1,
                    seconds=int(row["SOD"]),
                    microseconds=int((row["SOD"] - int(row["SOD"])) * 1e6),
                ),
                1,
            )
            df[["CH_18", "CH_26", "CH_30", "Q_1", "Q_2", "Q_3"]].resample(
                "1min"
            ).mean().to_csv(self.eve_csv_path(day))

        def get_check_tasks(self, scrap_date: Tuple[datetime, datetime]) -> None:
            new_scrap_date: List[str] = datetime_interval(
                *scrap_date, timedelta(days=1)
            )
            self.new_scrap_date_list = [
                date
                for date in new_scrap_date
                if not os.path.exists(self.eve_fits_gz_path(date))
            ]

        async def download_task(self, session) -> None:
            for i in range(0, len(self.new_scrap_date_list), self.batch_size):
                await asyncio.gather(
                    *[
                        self.download_url(session, day)
                        for day in self.new_scrap_date_list[i : i + self.batch_size]
                    ]
                )

        async def download_url(self, session, day: str) -> None:
            url = self.url(day)
            async with session.get(url, ssl=False) as response:
                data = await response.read()
                await asyncGZFITS(BytesIO(data), self.to_csv, day)

        """prep pipeline"""

        def data_prep(
            self, scrap_date: Tuple[datetime, datetime], step_size
        ) -> pd.DataFrame:
            init, end = scrap_date
            new_scrap_date = datetime_interval(init, end, step_size)
            dfs = [pd.read_csv(self.eve_csv_path(date)) for date in new_scrap_date]
            return (
                pd.concat(dfs)
                .resample(timedelta_to_freq(step_size))
                .mean()
                .loc[init:end]
            )

        async def downloader_pipeline(
            self, scrap_date: Tuple[datetime, datetime], session
        ):
            self.get_check_tasks(scrap_date)
            await self.download_task(session)
