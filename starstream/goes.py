from typing import Callable, List, Tuple, Coroutine
import asyncio
from datetime import datetime, timedelta
import glob
from bs4 import BeautifulSoup
from tqdm import tqdm
from starstream.utils import (
    datetime_interval,
    handle_client_connection_error,
)
from itertools import chain
import gzip
import aiofiles
import asyncio
import os

VALID_INSTRUMENTS = ["fe094", "fe131", "fe171", "fe195", "fe284", "he304"]


class GOES16:
    def __init__(
        self,
        instrument: str,
        path: str = "./data/GOES16/",
        granularity: float = 1.0,
        batch_size: int = 10,
    ) -> None:
        assert 0 <= granularity <= 1
        assert instrument in VALID_INSTRUMENTS
        self.batch_size: int = batch_size
        instrument = f"suvi-l1b-{instrument}"
        root: str = os.path.join(path, instrument)
        self.granularity: float = granularity
        os.makedirs(root, exist_ok=True)
        self.path: Callable[[str], str] = lambda name: os.path.join(root, name)
        self.url: Callable[[str, str], str] = (
            lambda name, date: f"https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l1b/{instrument}/{date[:4]}/{date[4:6]}/{date[6:]}/{name}"
        )

    def check_data(self, scrap_date: Tuple[datetime, datetime]) -> None:
        print(f"{self.__class__.__name__}: Checking for missing data...")
        new_scrap_date: List[str] = datetime_interval(*scrap_date, timedelta(days=1))
        self.new_scrap_date_list: List[str] = [
            date
            for date in new_scrap_date
            if len(glob.glob(self.path(date + "*"))) == 0
        ]

    def get_scrap_tasks(self, session) -> List[Coroutine]:
        print(f"{self.__class__.__name__}: Getting the URLs for missing data...")
        return [self.scrap_url(session, day) for day in self.new_scrap_date_list]

    @handle_client_connection_error(max_retries=3, increment="exp", default_cooldown=5)
    async def scrap_url(self, session, date: str) -> List[Tuple[str, str]] | None:
        url: str = self.url("", date)
        async with session.get(url) as request:
            if request.status != 200:
                print(
                    f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                )
                self.new_scrap_date_list.remove(date)
            else:
                html = await request.text()
                if "404 Not Found" in html:
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                    return
                soup = BeautifulSoup(html, "html.parser")
                href = lambda x: x and x.endswith("fits.gz")
                fits_links = soup.find_all("a", href=href)
                names = [(link["href"], date) for link in fits_links]
                names = [
                    name
                    for idx, name in enumerate(names)
                    if idx % round(1 / self.granularity) == 0
                ]
                return names

    @handle_client_connection_error(max_retries=3, increment="exp", default_cooldown=5)
    async def download_url(self, session, date: str, name: str) -> None:
        url: str = self.url(name, date)
        path: str = self.path(name)
        async with session.get(url) as request:
            if request.status != 200:
                print(
                    f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                )
                self.new_scrap_date_list.remove(date)
            else:
                data = await request.read()
                if data.startswith(b"<html>"):
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                    return
                async with aiofiles.open(path, "wb") as file:
                    await file.write(data)

    def get_download_tasks(
        self, session, fits_names: List[Tuple[str, str]]
    ) -> List[Coroutine]:
        print(f"{self.__class__.__name__}: Downloading...")
        return [self.download_url(session, date, name) for name, date in fits_names]

    def get_preprocessing_tasks(
        self, fits_names: List[Tuple[str, str]]
    ) -> List[Coroutine]:
        print(f"{self.__class__.__name__}: Preprocessing...")
        return [self.preprocess(name) for name, _ in fits_names]

    async def preprocess(self, name: str) -> None:
        path: str = self.path(name)
        gzip_file = gzip.GzipFile(path)
        fits_file = gzip_file.read()
        gzip_file.close()
        os.remove(path)
        async with aiofiles.open(path[:-3], "xb") as file:
            await file.write(fits_file)

    async def downloader_pipeline(self, scrap_date: Tuple[datetime, datetime], session):
        self.check_data(scrap_date)
        if len(self.new_scrap_date_list) == 0:
            print("Already downloaded!")
        else:
            fixed_fits_links = []
            scrap_tasks = self.get_scrap_tasks(session)

            for i in tqdm(
                range(0, len(scrap_tasks), self.batch_size),
                desc=f"Scrapping URLs for {self.__class__.__name__}",
            ):
                fits_links = await asyncio.gather(*scrap_tasks[i : i + self.batch_size])
                fits_links = [*chain.from_iterable(fits_links)]
                fixed_fits_links.extend(fits_links)
            fixed_fits_links = [i for i in fixed_fits_links if i is not None]

            down_tasks = self.get_download_tasks(session, fixed_fits_links)

            for i in tqdm(
                range(0, len(down_tasks), self.batch_size),
                desc=f"Downloading URLs for {self.__class__.__name__}",
            ):
                await asyncio.gather(*down_tasks[i : i + self.batch_size])

            prep_tasks = self.get_preprocessing_tasks(fixed_fits_links)
            for i in tqdm(
                range(0, len(prep_tasks), self.batch_size),
                desc=f"Preprocessing for {self.__class__.__name__}",
            ):
                await asyncio.gather(*prep_tasks[i : i + self.batch_size])
