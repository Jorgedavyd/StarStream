from collections.abc import Coroutine
from typing import Callable, List, Tuple, Union
from numpy._typing import NDArray
from tqdm import tqdm
from .utils import (
    StarDate,
    StarImage,
    StarInterval,
    handle_client_connection_error,
)
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from torch import Tensor
import asyncio
import glob
import aiofiles
from itertools import chain
import os
import os.path as osp

__all__ = ["Hinode"]


class Hinode:
    class XRT:
        def __init__(self, download_path: str = "./data/Hinode/XRT", batch_size: int = 1) -> None:
            self.batch_size: int = batch_size
            self.root: str = download_path
            self.path: Callable[[str], str] = lambda name: osp.join(
                self.root, f"{name}.fits"
            )
            self.scrap_path: Callable[[str], str] = lambda name: osp.join(
                self.root, f"{name}.fits"
            ) ## rvisar

            self.url: Callable[[str, str], str] = (
                lambda date, hour: f"https://xrt.cfa.harvard.edu/level1/{date[:4]}/{date[4:6]}/{date[6:]}/H{hour[:2]}00/"
            )
            os.makedirs(self.root, exist_ok=True)

        def _check_tasks(self, scrap_date: List[Tuple[datetime, datetime]]) -> None:
            new_scrap_date: StarInterval = StarInterval(
                scrap_date, timedelta(days=1), "%Y%m%d-%H%M"
            )

            for date in tqdm(new_scrap_date, desc = f'{self.__class__.__name__}: Looking for URLs'):
                if len(glob.glob(self.path(f'{date.str().split("-")[0]}*'))) == 0:
                    self.new_scrap_date_list.append(date)

        def _get_scrap_tasks(self, session) -> List[Coroutine]:
            return [
                self._scrap_names(session, *date.str().split("-"))
                for date in self.new_scrap_date_list
            ]

        @handle_client_connection_error(
            max_retries=3, increment="exp", default_cooldown=5
        )
        async def _scrap_names(
            self, session, date: str, hour: str
        ) -> Union[List[str], None]:
            url: str = self.url(date, hour)
            async with session.get(url, ssl=False) as response:
                if response.status != 200:
                    print(
                        f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                    )
                    self.new_scrap_date_list = [
                        item
                        for item in self.new_scrap_date_list
                        if item != (date, hour)
                    ]
                else:
                    html = await response.text()
                    if "404 Not Found" in html:
                        print(
                            f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                        )
                        self.new_scrap_date_list = [
                            item
                            for item in self.new_scrap_date_list
                            if item != (date, hour)
                        ]
                        return

                    soup = BeautifulSoup(html, "html.parser")
                    fits_links = soup.find_all(
                        "a", href=lambda href: href and href.endswith(".fits")
                    )
                    download_urls = [
                        self.url(date, hour) + link["href"] for link in fits_links
                    ]

                    return download_urls

        def _get_downloading_tasks(
            self, download_urls: List[str], session
        ) -> List[Coroutine]:
            return [self._download_url(session, url) for url in download_urls]

        @handle_client_connection_error(
            max_retries=3, increment="exp", default_cooldown=5
        )
        async def _download_url(self, session, url: str) -> None:
            async with session.get(url, ssl=False) as response:
                data = await response.read()
                async with aiofiles.open(self.path(url[-22:-9]), "wb") as file:
                    await file.write(data)

        async def fetch(self, scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]], session) -> None:
            if isinstance(scrap_date[0], datetime):
                self._check_tasks([scrap_date])
            else:
                self._check_tasks(scrap_date)

            if len(self.new_scrap_date_list) == 0:
                print(f"{self.__class__.__name__} Already downloaded!")
            else:
                scrap_tasks = self._get_scrap_tasks(session)
                for i in tqdm(range(0, len(scrap_tasks), self.batch_size), desc=f"{self.__class__.__name__}: Downloading..."):
                    download_urls = await asyncio.gather(
                        *scrap_tasks[i : i + self.batch_size]
                    )
                    download_urls = [*chain.from_iterable(download_urls)]
                    download_urls = [param for param in download_urls if param is not None]
                    asyncio.gather(*self._get_downloading_tasks(download_urls, session))

        def get_hour_images(self, date: StarDate) -> List[str]:
            query_c = "*" + "_".join(date.str().split("-"))[:-4] + "**"
            return glob.glob(self.path(query_c))

        def get_numpy(self, scrap_date: List[Tuple[datetime, datetime]]) -> NDArray:
            paths: List[str] = self._path_prep(scrap_date)
            return asyncio.run(StarImage.get_numpy(paths))

        def get_torch(self, scrap_date: List[Tuple[datetime, datetime]]) -> Tensor:
            paths: List[str] = self._path_prep(scrap_date)
            return asyncio.run(StarImage.get_torch(paths))

        def _path_prep(self, scrap_date: List[Tuple[datetime, datetime]]):
            new_scrap_date: StarInterval = StarInterval(
                scrap_date, timedelta(hours=1), "%Y%m%d-%H%M"
            )
            return [*chain.from_iterable([self.get_hour_images(date) for date in new_scrap_date])]
