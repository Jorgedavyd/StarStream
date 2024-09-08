from collections.abc import Coroutine
from typing import Callable, List, Tuple, Union
from tqdm import tqdm
from .utils import (
    DataDownloading,
    asyncFITS,
    datetime_interval,
    handle_client_connection_error,
)
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from io import BytesIO
import numpy as np
import asyncio
import glob
import aiofiles
from itertools import chain
import os
import os.path as osp

__all__ = ["Hinode"]


class Hinode:
    class XRT:
        def __init__(
            self,
            download_path: str = "./data/Hinode/XRT",
            filetype: str = "fits",
            batch_size: int = 1,
        ) -> None:
            self.filetype: str = filetype
            self.batch_size: int = batch_size
            self.xrt_folder_path: str = download_path
            self.path: Callable[[str], str] = lambda name: osp.join(
                self.xrt_folder_path, f"{name}.{filetype}"
            )
            self.download_urls: List[str] = []
            self.url: Callable[[str, str], str] = (
                lambda date, hour: f"https://xrt.cfa.harvard.edu/level1/{date[:4]}/{date[4:6]}/{date[6:]}/H{hour[:2]}00/"
            )
            os.makedirs(self.xrt_folder_path, exist_ok=True)

        def check_tasks(self, scrap_date: Tuple[datetime, datetime]) -> None:
            print(f"{self.__class__.__name__}: Looking for missing data...")
            new_scrap_date: List[str] = datetime_interval(
                *scrap_date, timedelta(hours=1), "%Y%m%d-%H%M"
            )
            self.new_scrap_date_list = [
                date.split("-")
                for date in new_scrap_date
                if len(glob.glob(self.path(f'{date.split("-")[0]}*'))) == 0
            ]

        def get_scrap_tasks(self, session) -> List[Coroutine]:
            print(f"{self.__class__.__name__}: Scrapping URLs and downloading...")
            return [
                self.scrap_names(session, date, hour)
                for date, hour in self.new_scrap_date_list
            ]

        @handle_client_connection_error(
            max_retries=3, increment="exp", default_cooldown=5
        )
        async def scrap_names(self, session, date, hour) -> Union[List[str], None]:
            url: str = self.url(date, hour)
            async with session.get(url, ssl=False) as response:
                if response.status != 200:
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                    )
                    self.new_scrap_date_list.remove(date)
                else:
                    html = await response.text()
                    if "404 Not Found" in html:
                        print(
                            f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                        )
                        self.new_scrap_date_list.remove(date)
                        return
                    soup = BeautifulSoup(html, "html.parser")
                    fits_links = soup.find_all(
                        "a", href=lambda href: href and href.endswith(".fits")
                    )
                    download_urls = [
                        self.url(date, hour) + link["href"] for link in fits_links
                    ]

                    return download_urls

        def get_downloading_tasks(
            self, download_urls: List[str], session
        ) -> List[Coroutine]:
            return [self.download_url(session, url) for url in download_urls]

        def fits_processing(self, fits_file, path):
            image = fits_file[0].data
            np.save(path, image)

        @handle_client_connection_error(
            max_retries=3, increment="exp", default_cooldown=5
        )
        async def download_url(self, session, url):
            async with session.get(url, ssl=False) as response:
                data = await response.read()
                if self.filetype != "fits":
                    await asyncFITS(
                        BytesIO(data), self.fits_processing, self.path(url[-22:-9])
                    )
                else:
                    async with aiofiles.open(self.path(url[-22:-9]), "wb") as file:
                        await file.write(data)

        async def downloader_pipeline(
            self, scrap_date: Tuple[datetime, datetime], session
        ) -> None:
            self.check_tasks(scrap_date)
            if len(self.new_scrap_date_list) == 0:
                print(f"{self.__class__.__name__} Already downloaded!")
            else:
                params: List = []
                scrap_tasks = self.get_scrap_tasks(session)
                for i in tqdm(
                    range(0, len(scrap_tasks), self.batch_size),
                    desc=f"Scrapping and downloading for {self.__class__.__name__}",
                ):
                    download_urls: List[Union[List[str], None]] = await asyncio.gather(
                        *scrap_tasks[i : i + self.batch_size]
                    )
                    download_urls = [*chain.from_iterable(download_urls)]
                    params.extend(download_urls)

                params = [param for param in params if param is not None]

                downloading_tasks: List[Coroutine] = self.get_downloading_tasks(
                    params, session
                )

                for i in tqdm(
                    range(0, len(downloading_tasks), self.batch_size),
                    desc=f"Downloading for {self.__class__.__name__}",
                ):
                    await asyncio.gather(*downloading_tasks[i : i + self.batch_size])

        def get_hour_images(self, date: str):
            query_c = "*" + "_".join(date.split("-"))[:-4] + "**"
            return glob.glob(self.path(query_c))

        async def data_prep(self, scrap_date: Tuple[datetime, datetime]):
            new_scrap_date: List[str] = datetime_interval(
                *scrap_date,
                timedelta(days=1),
            )
            return [
                *chain.from_iterable(
                    [glob.glob(self.path(f"{date}*")) for date in new_scrap_date]
                )
            ]


if __name__ == "__main__":
    sample_date: Tuple[datetime, datetime] = (
        datetime(2020, 10, 10),
        datetime(2020, 10, 11),
    )
    DataDownloading(Hinode.XRT(), sample_date)
