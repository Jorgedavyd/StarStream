from tqdm import tqdm
from starstream._base import CSV
from starstream._utils import (
    asyncGZIP,
    create_scrap_date,
    download_url_prep,
    handle_client_connection_error,
)
from starstream.typing import ScrapDate
from datetime import timedelta, datetime
import xarray as xr
import os
import time
from typing import Optional, Tuple, List, Union, Callable
import os.path as osp
from bs4 import BeautifulSoup
import aiofiles
from playwright.async_api import async_playwright
from dateutil.relativedelta import relativedelta

__all__ = ["DSCOVR"]


class DSCOVR:
    class __Base(CSV):
        def __init__(
            self,
            root: str = "./data",
            batch_size: int = 10,
            filepath: Optional[Callable] = None,
            date_sampling: Union[timedelta, relativedelta] = timedelta(days=1),
            format: str = "%Y%m%d",
            level: str = "l2",
            achronym: Optional[str] = None,
        ) -> None:
            super().__init__(root, batch_size, filepath, date_sampling, format)
            assert level == "l2" or level == "l1", "Not valid data product level"
            assert achronym is not None, "Achronym not passed"
            self.level = level
            self.achronym = achronym

        async def _get_urls(self) -> None:
            async with aiofiles.open(
                osp.join(osp.dirname(__file__), "trivials/url.txt"), "r"
            ) as file:
                lines = await file.readlines()
            for url in tqdm(
                lines, desc=f"{self.__class__.__name__}: Getting the URLs..."
            ):
                for date in self.dates:
                    if date.str() + "000000" in url and self.achronym in url:
                        self.urls.append(url[:-1])

        async def _check_update(
            self, scrap_date: List[Tuple[datetime, datetime]]
        ) -> None:
            update_path: str = osp.join(
                osp.dirname(__file__), f"trivials/last_update.txt"
            )
            scrap_date = sorted(
                scrap_date,
                key=lambda key: key[-1],
            )
            try:
                async with aiofiles.open(update_path, "r") as file:
                    lines = await file.readlines()
                    date = datetime.strptime(lines[0], "%Y%m%d")
                    if scrap_date[-1][-1] > date:
                        await self._scrap_links(
                            (date + timedelta(days=1), scrap_date[-1][-1])
                        )
            except FileNotFoundError:
                os.makedirs(osp.dirname(update_path), exist_ok=True)
                await self._scrap_links(
                    (datetime(2016, 7, 26), datetime.today() - timedelta(days=1))
                )

        @staticmethod
        def _to_unix(scrap_date: Tuple[datetime, datetime]) -> List[int]:
            timestamp = [
                int(time.mktime(datetime(*date.timetuple()[:3]).timetuple())) * 1000
                for date in scrap_date
            ]
            return timestamp

        async def _interval_setup(self, scrap_date: ScrapDate) -> None:
            await self._check_update(create_scrap_date(scrap_date))
            super()._interval_setup(scrap_date)
            await self._get_urls()
            self.paths = [self.filepath(date.str()) for date in self.dates]

        async def _scrap_(self, idx: int) -> None:
            _ = idx

        async def _scrap_links(self, scrap_date: Tuple[datetime, datetime]) -> None:
            print("Updating url dataset...")
            unix = self._to_unix(scrap_date)
            url = f"https://www.ngdc.noaa.gov/dscovr/portal/index.html#/download/{unix[0]};{unix[-1]}/f1m;fc1;m1m;mg1"

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(url)
                await page.wait_for_timeout(20000)
                html = await page.content()
                await browser.close()
            soup = BeautifulSoup(html, "html.parser")
            try:
                value: str = soup.find(
                    "input", class_="form-control input-sm cursor-text"
                )["value"]
            except AttributeError:
                print(
                    "Failed to find the input element. Check if the page structure has changed."
                )
                return
            basepath: str = osp.join(osp.dirname(__file__), "trivials")
            url_path: str = osp.join(basepath, "url.txt")
            os.makedirs(basepath, exist_ok=True)
            output = value.split()[1:]
            async with aiofiles.open(url_path, "w") as file:
                await file.write("\n".join(output))
            update_path = osp.join(basepath, "last_update.txt")
            async with aiofiles.open(update_path, "w") as file:
                await file.write(scrap_date[-1].strftime("%Y%m%d"))

        def _gz_processing(self, gz_file, date: str) -> None:
            dataset = xr.open_dataset(gz_file.read())
            df = dataset.to_dataframe()
            dataset.close()
            df = df.reset_index(drop=False)
            df = df.rename(columns={"time": "date"})
            df.to_csv(self.filepath(date), index=False)

        @handle_client_connection_error(
            default_cooldown=5, max_retries=3, increment="exp"
        )
        async def _download_(self, idx: int) -> None:
            async def anonymous(gzip_file):
                await asyncGZIP(gzip_file, self._gz_processing, self.dates[idx].str())

            await download_url_prep(self, idx, anonymous)

        async def _prep_(self, idx: int) -> None:
            _ = idx

    class FaradayCup(__Base):
        def __init__(
            self,
            root: str = "./data/DSCOVR/FaradayCup",
            batch_size: int = 15,
            level: str = "l2",
        ) -> None:
            super().__init__(
                root=osp.join(root, level),
                batch_size=batch_size,
                level=level,
                achronym="fc1" if level == "l1" else "f1m",
            )

    class Magnetometer(__Base):
        def __init__(
            self,
            root: str = "./data/DSCOVR/Magnetometer",
            batch_size: int = 10,
            level: str = "l2",
        ) -> None:
            super().__init__(
                root=osp.join(root, level),
                batch_size=batch_size,
                level=level,
                achronym="mg1" if level == "l1" else "m1m",
            )
