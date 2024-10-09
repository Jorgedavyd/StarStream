from dataclasses import dataclass
from tqdm import tqdm

from starstream._base import Satellite
from .utils import (
    StarDate,
    StarInterval,
    handle_client_connection_error,
    asyncGZ,
)
from datetime import timedelta, datetime
from io import BytesIO
import xarray as xr
import asyncio
import os
import time
from typing import Coroutine, Tuple, Callable, List
from selenium.webdriver.chrome.options import Options
import os.path as osp
from bs4 import BeautifulSoup
from selenium import webdriver
import chromedriver_binary
import aiofiles

__all__ = ["DSCOVR"]

__path_func: Callable[[str, str, str, str], str] = (
    lambda root, arg1, arg2, date: osp.join(root, arg1, arg2, f"{date}.csv")
)

class DSCOVR:
    @dataclass
    class __Base(Satellite):
        batch_size: int
        root: str
        level: str
        csv_path: Callable[[str], str]
        var: List[str]
        achronym: str

        def __post_init__(self) -> None:
            assert self.level == "l2" or self.level == "l1", "Not valid data product level"

        @staticmethod
        def _to_unix(scrap_date: Tuple[datetime, datetime]) -> List[int]:
            timestamp = [
                int(time.mktime(datetime(*date.timetuple()[:3]).timetuple())) * 1000
                for date in scrap_date
            ]
            return timestamp

        async def _check_update(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
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
                        os.remove(update_path)
                        self._scrap_links((date + timedelta(days=1), scrap_date[-1][-1]))
            except FileNotFoundError:
                os.makedirs(osp.dirname(update_path), exist_ok=True)
                self._scrap_links(
                    (datetime(2016, 7, 26), datetime.today() - timedelta(days=1))
                )

        def _scrap_links(self, scrap_date: Tuple[datetime, datetime]) -> None:
            print("Updating url dataset...")
            unix = self._to_unix(scrap_date)
            url = (
                lambda unix: f"https://www.ngdc.noaa.gov/dscovr/portal/index.html#/download/{unix[0]};{unix[-1]}/f1m;fc1;m1m;mg1"
            )
            op = Options()
            op.add_argument("headless")
            driver = webdriver.Chrome(options=op)
            driver.get(url(unix))
            time.sleep(10)
            html = driver.page_source
            driver.quit()
            soup = BeautifulSoup(html, "html.parser")
            value = soup.find("input", class_="form-control input-sm cursor-text")["value"]
            url_path = osp.join(osp.dirname(__file__), "trivials/url.txt")
            with open(url_path, "a") as file:
                file.write(value[5:].replace(" ", "\n") + "\n")
            update_path = osp.join(osp.dirname(__file__), "trivials/last_update.txt")
            with open(update_path, "x") as file:
                file.write(scrap_date[-1].strftime("%Y%m%d"))

        async def _check_tasks(self, scrap_date_list: List[Tuple[datetime, datetime]]) -> None:
            await self._check_update(scrap_date_list)
            new_scrap_date: StarInterval = StarInterval(
                scrap_date_list, timedelta(days=1), "%Y%m%d"
            )

            self.new_scrap_date_list = [
                date
                for date in new_scrap_date
                if not os.path.exists(self.csv_path(date.str()))
            ]

        def _gz_processing(self, gz_file, date: str) -> None:
            dataset = xr.open_dataset(gz_file.read())
            df = dataset.to_dataframe()
            dataset.close()
            df.to_csv(self.csv_path(date))

        @handle_client_connection_error(default_cooldown=5, max_retries=3, increment="exp")
        async def _download_url(self, url: str, date: StarDate, session) -> None:
            async with session.get(url, ssl=False) as response:
                if response.status != 200:
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                else:
                    data = await response.read()
                    if data.startswith(b"<html>"):
                        print(
                            f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                        )
                        self.new_scrap_date_list.remove(date)
                        return
                    await asyncGZ(BytesIO(data), self._gz_processing, url, date)

        async def _get_urls(self) -> List[Tuple[str, StarDate]]:
            async with aiofiles.open(
                osp.join(osp.dirname(__file__), "trivials/url.txt"), "r"
            ) as file:
                lines = await file.readlines()
            url_list = []
            for url in tqdm(lines, desc=f"{self.__class__.__name__}: Getting the URLs..."):
                for date in self.new_scrap_date_list:
                    if date.str() + "000000" in url and self.achronym in url:
                        url_list.append((url, date))
            return url_list

        def _get_download_tasks(self, session) -> List[Coroutine]:
            urls_dates = asyncio.run(self._get_urls())
            return [self._download_url(url, date, session) for url, date in urls_dates]

    class FaradayCup(__Base):
        def __init__(
            self,
            download_path: str = "./data/DSCOVR",
            batch_size: int = 15,
            level: str = "l1",
        ) -> None:
            super().__init__(
                batch_size=batch_size,
                root=download_path,
                level=level,
                csv_path=lambda date: __path_func(
                    download_path, "faraday", level, date
                ),
                var=["proton_density", "proton_speed", "proton_temperature"],
                achronym="fc1" if level == "l1" else "fm1",
            )

    class Magnetometer(__Base):
        def __init__(
            self,
            download_path: str = "./data/DSCOVR",
            batch_size: int = 15,
            level: str = "l1",
        ) -> None:
            super().__init__(
                batch_size=batch_size,
                root=download_path,
                level=level,
                csv_path=lambda date: __path_func(
                    download_path, level, "magnetometer", date
                ),
                var=["bx_gsm", "by_gsm", "bz_gsm", "bt"],
                achronym="mg1" if level == "l1" else "m1m",
            )
