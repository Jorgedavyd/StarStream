from dataclasses import dataclass
from tqdm import tqdm
from starstream._base import CSV
from starstream._utils import asyncGZIP, create_scrap_date, download_url_prep
from starstream.typing import ScrapDate
from starstream.utils import handle_client_connection_error
from datetime import timedelta, datetime
from io import BytesIO
import xarray as xr
import os
import time
from typing import Tuple, List
from selenium.webdriver.chrome.options import Options
import os.path as osp
from bs4 import BeautifulSoup
from selenium import webdriver
import chromedriver_binary
import aiofiles

__all__ = ["DSCOVR"]


class DSCOVR:
    @dataclass
    class __Base(CSV):
        level: str = "l1"
        achronym: str = None

        def __post_init__(self) -> None:
            assert (
                self.level == "l2" or self.level == "l1"
            ), "Not valid data product level"
            assert self.achronym is not None, "Achronym not passed"

        async def _get_urls(self) -> None:
            async with aiofiles.open(
                osp.join(osp.dirname(__file__), "trivials/url.txt"), "r"
            ) as file:
                lines = await file.readlines()
            url_list = []
            for url in tqdm(
                lines, desc=f"{self.__class__.__name__}: Getting the URLs..."
            ):
                for date in self.dates:
                    if date.str() + "000000" in url and self.achronym in url:
                        url_list.append(url)
            self.urls = url_list

        async def _check_update(
            self, scrap_date: List[Tuple[datetime, datetime]]
        ) -> None:
            update_path: str = osp.join(osp.dirname(__file__), f"trivials/update.txt")
            scrap_date = sorted(
                scrap_date,
                key=lambda key: key[-1],
            )
            try:
                async with aiofiles.open(update_path, "r") as file:
                    lines = await file.readlines()
                    date = datetime.strptime(lines[0], "%Y%m%d")
                    if scrap_date[-1][-1] > date:
                        self._scrap_links(
                            (date + timedelta(days=1), scrap_date[-1][-1])
                        )
            except FileNotFoundError:
                os.makedirs(osp.dirname(update_path), exist_ok=True)
                self._scrap_links(
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
            pass

        def _scrap_links(self, scrap_date: Tuple[datetime, datetime]) -> None:
            print("Updating url dataset...")
            unix = self._to_unix(scrap_date)
            url = f"https://www.ngdc.noaa.gov/dscovr/portal/index.html#/download/{unix[0]};{unix[-1]}/f1m;fc1;m1m;mg1"
            op = Options()
            op.add_argument("headless")
            driver = webdriver.Chrome(options=op)
            driver.get(url)
            time.sleep(10)
            html = driver.page_source
            driver.quit()
            soup = BeautifulSoup(html, "html.parser")
            value = soup.find("input", class_="form-control input-sm cursor-text")[
                "value"
            ]
            url_path = osp.join(osp.dirname(__file__), "trivials/url.txt")
            with open(url_path, "a") as file:
                file.write(value[5:].replace(" ", "\n") + "\n")
            update_path = osp.join(osp.dirname(__file__), "trivials/last_update.txt")
            os.remove(update_path)
            with open(update_path, "x") as file:
                file.write(scrap_date[-1].strftime("%Y%m%d"))

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
            return await download_url_prep(
                self,
                idx,
                lambda gzip_file: asyncGZIP(
                    BytesIO(gzip_file), self._gz_processing, self.dates[idx].str()
                ),
            )

    class FaradayCup(__Base):
        def __init__(
            self,
            download_path: str = "./data/DSCOVR",
            batch_size: int = 15,
            level: str = "l1",
        ) -> None:
            super().__init__(
                root=osp.join(download_path, level, "faraday"),
                batch_size=batch_size,
                level=level,
                achronym="fc1" if level == "l1" else "fm1",
            )

    class Magnetometer(__Base):
        def __init__(
            self,
            download_path: str = "./data/DSCOVR",
            batch_size: int = 10,
            level: str = "l2",
        ) -> None:
            super().__init__(
                root=osp.join(download_path, level, "magnetometer"),
                batch_size=batch_size,
                level=level,
                achronym="mg1" if level == "l1" else "m1m",
            )
