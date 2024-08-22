from typing import Callable, List, Tuple, Coroutine
import asyncio
from datetime import datetime, timedelta
import glob
from bs4 import BeautifulSoup
from starstream.utils import datetime_interval, interval_time, syncGZ
import aiofiles
import asyncio
import os
import io

VALID_INSTRUMENTS = ["fe094", "fe131", "fe171", "fe195", "fe284", "he304"]


class GOES16:
    batch_size: int = 10

    def __init__(self, instrument: str, path: str = "./data/GOES16/") -> None:
        assert instrument in VALID_INSTRUMENTS
        instrument = f"subi-11b-{instrument}"
        self.path: Callable[[str], str] = lambda name: os.path.join(
            path, instrument, name
        )
        self.url: Callable[[str, str], str] = (
            lambda name, date: f"https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l1b/{instrument}/{date[:2]}/{date[2:4]}/{date[4:6]}/{name}"
        )

    def check_data(self, scrap_date: Tuple[datetime, datetime]) -> None:
        new_scrap_date: List[str] = datetime_interval(*scrap_date, timedelta(days=1))
        self.new_scrap_date_list: List[str] = [
            date
            for date in new_scrap_date
            if len(glob.glob(self.path(date + "*"))) == 0
        ]

    def get_scrap_tasks(self, session) -> List[Coroutine]:
        return [self.scrap_url(session, day) for day in self.new_scrap_date_list]

    async def scrap_url(self, session, date: str) -> List[Tuple[str, str]]:
        async with session.get(self.url("", date)) as request:
            html = await request.html()
            soup = BeautifulSoup(html, "html.parser")
            href = lambda x: x and x.endswith("fits.gz")
            fits_links = soup.find_all("a", href=href)
            return [(link, date) for link in fits_links]

    async def download_url(self, session, date: str, name: str) -> None:
        url: str = self.url(date, name)
        async with session.get(url) as request:
            data = await request.read()
            gz_file = await asyncio.get_event_loop().run_in_executor(
                None, syncGZ, io.BytesIO(data)
            )
            data = gz_file.read()
            async with aiofiles.open(self.path(name), "wb") as file:
                await file.write(data)

    def get_download_tasks(self, session, fits_names: List[str]) -> List[Coroutine]:
        return [self.download_url(session, name, date) for name, date in fits_names]

    async def downloader_pipeline(self, scrap_date: Tuple[datetime, datetime], session):
        self.check_data(scrap_date)
        if len(self.new_scrap_date_list) == 0:
            print("Already downloaded!")
        else:
            fixed_fits_links = []
            scrap_tasks = self.get_scrap_tasks(session)

            for i in range(0, len(scrap_tasks), self.batch_size):
                fits_links = await asyncio.gather(*scrap_tasks[i : i + self.batch_size])
                fixed_fits_links.extend(fits_links)

            down_tasks = self.get_download_tasks(session, fixed_fits_links)

            for i in range(0, len(down_tasks), self.batch_size):
                await asyncio.gather(*down_tasks[i : i + self.batch_size])
