from starstream.utils import StarDate, StarInterval, handle_client_connection_error
from typing import Callable, List, Tuple, Coroutine, Union
from datetime import datetime, timedelta
from starstream._base import StarImage
from bs4 import BeautifulSoup
from itertools import chain
from tqdm import tqdm
import os.path as osp
import aiofiles
import asyncio
import asyncio
import glob
import gzip
import os

VALID_INSTRUMENTS = ["fe094", "fe131", "fe171", "fe195", "fe284", "he304"]


class GOES16(StarImage):
    def __init__(
        self,
        instrument: str,
        granularity: float = 1.0,
        download_path: str = "./data/GOES16/",
        batch_size: int = 10,
    ) -> None:
        self.intrument = f"suvi-l1b-{instrument}"
        super().__init__(
            os.path.join(download_path, self.intrument),
            batch_size,
        )
        assert 0 <= granularity <= 1, "Not valid granularity, must be < |1|"
        assert (
            instrument in VALID_INSTRUMENTS
        ), f"Not valid instrument: {instrument}, must be {VALID_INSTRUMENTS}"
        self.url: Callable[[str, str], str] = (
            lambda name, date: f"https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l1b/{instrument}/{date[:4]}/{date[4:6]}/{date[6:]}/{name}"
        )
        self.granularity: float = granularity
        self.img_path: Callable[[str], str] = lambda name: os.path.join(self.root_path, name)

    def _check_data(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
        new_scrap_date: StarInterval = StarInterval(scrap_date)
        for date in tqdm(new_scrap_date, desc = f'{self.__class__.__name__}: Looking for missing dates...'):
            if len(glob.glob(self.img_path(date.str() + "*"))) == 0:
                self.new_scrap_date_list.append(date)
        if self.new_scrap_date_list:
            os.makedirs(self.root_path, exist_ok = True)

    def _get_scrap_tasks(self, session) -> List[Coroutine]:
        return [self.scrap_url(session, date) for date in self.new_scrap_date_list]

    @handle_client_connection_error(max_retries=3, increment="exp", default_cooldown=5)
    async def scrap_url(self, session, date: StarDate) -> Union[List[Tuple[Union[List[str], None], StarDate]], None]:
        url: str = self.url("", date.str())
        async with session.get(url) as request:
            if request.status != 200:
                print(
                    f"{self.__class__.__name__}: Data not available for date: {date.str()}, queried url: {url}"
                )
                self.new_scrap_date_list.remove(date)
            else:
                html = await request.text()
                if "404 Not Found" in html:
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date.str()}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                    return
                soup = BeautifulSoup(html, "html.parser")
                href = lambda x: x and x.endswith("fits.gz")
                fits_links = soup.find_all("a", href=href)
                names = [(link["href"], date.str()) for link in fits_links]
                names = [
                    name
                    for idx, name in enumerate(names)
                    if idx % round(1 / self.granularity) == 0
                ]
                return names

    @handle_client_connection_error(max_retries=3, increment="exp", default_cooldown=5)
    async def _download_url(self, session, date: str, name: str) -> None:
        url: str = self.url(name, date)
        path: str = self.img_path(name)
        async with session.get(url, ssl = False) as request:
            data = await request.read()
            async with aiofiles.open(path, "wb") as file:
                await file.write(data)

    def _get_download_tasks(
        self, session, fits_names: List[Tuple[str, str]]
    ) -> List[Coroutine]:
        return [self._download_url(session, date, name) for name, date in fits_names]

    def _get_preprocessing_tasks(
        self, fits_names: List[Tuple[str, str]]
    ) -> List[Coroutine]:
        print(f"{self.__class__.__name__}: Preprocessing...")
        return [self.preprocess(name) for name, _ in fits_names]

    async def preprocess(self, name: str) -> None:
        path: str = self.img_path(name)
        gzip_file = gzip.GzipFile(path)
        fits_file = gzip_file.read()
        gzip_file.close()
        os.remove(path)
        async with aiofiles.open(path[:-3], "xb") as file:
            await file.write(fits_file)

    async def fetch(self, scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime,datetime]], session) -> None:
        if isinstance(scrap_date[0], datetime):
            scrap_date = [scrap_date]
        self._check_data(scrap_date)

        fixed_fits_links = []
        scrap_tasks = self._get_scrap_tasks(session)

        for i in tqdm(
            range(0, len(scrap_tasks), self.batch_size),
            desc=f"Scrapping URLs for {self.__class__.__name__}",
        ):
            fits_links = await asyncio.gather(*scrap_tasks[i : i + self.batch_size])
            fits_links = [*chain.from_iterable(fits_links)]
            fixed_fits_links.extend(fits_links)
        fixed_fits_links = [i for i in fixed_fits_links if i is not None]

        down_tasks = self._get_download_tasks(session, fixed_fits_links)

        for i in tqdm(
            range(0, len(down_tasks), self.batch_size),
            desc=f"Downloading URLs for {self.__class__.__name__}",
        ):
            await asyncio.gather(*down_tasks[i : i + self.batch_size])

        prep_tasks = self._get_preprocessing_tasks(fixed_fits_links)
        for i in tqdm(
            range(0, len(prep_tasks), self.batch_size),
            desc=f"Preprocessing for {self.__class__.__name__}",
        ):
            await asyncio.gather(*prep_tasks[i : i + self.batch_size])


    def _path_prep(self, scrap_date: List[Tuple[datetime, datetime]]) -> List[str]:
        new_scrap_date: StarInterval = StarInterval(
            scrap_date,
            timedelta(days=1),
        )

        scrap_files: List[str] = []

        for date in new_scrap_date:
            for path in os.listdir(self.root_path):
                if date.str() in path:
                    scrap_files.append(path)

        scrap_files = list(map(lambda x: osp.join(self.root_path, x), scrap_files))

        return scrap_files
