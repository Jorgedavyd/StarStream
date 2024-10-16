from collections.abc import Coroutine
from typing import Callable, List, Tuple, Union
from spacepy import os
from tqdm import tqdm
from starstream._base import Img
from starstream._utils import (
    StarDate,
    StarInterval,
    download_url_write,
    handle_client_connection_error,
    scrap_url_default,
)
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import asyncio
import glob
import aiofiles
from itertools import chain
import os.path as osp

from starstream.typing import ScrapDate

__all__ = ["Hinode"]


class Hinode:
    class XRT(Img):
        def __init__(
            self, root: str = "./data/Hinode/XRT", batch_size: int = 1
        ) -> None:
            super().__init__(
                root,
                batch_size,
                lambda name: osp.join(root, name)
            )
            self.url: Callable[[str, str, str], str] = (
                lambda date, hour, name: f"https://xrt.cfa.harvard.edu/level1/{date[:4]}/{date[4:6]}/{date[6:]}/H{hour[:2]}00/{name}"
            )

        def _interval_setup(self, scrap_date: ScrapDate) -> None:
            return super()._interval_setup(scrap_date)

        async def _scrap_(self, idx: int) -> None:
            try:
                date: str = self.dates[idx].str()
                await scrap_url_default(self, idx, self.scrap, date, hour)
                await super()._scrap_(idx)
            except IndexError:
                return

        def scrap(self, html, date: str, hour: str) -> None:
            soup = BeautifulSoup(html, "html.parser")
            names = soup.find_all("a", href=lambda href: href and href.endswith(".fits"))
            names: List[str] = [name["href"] for name in names]

            self.urls.extend([self.url(date, hour, name) for name in names])
            self.paths.extend([self.filepath(name) for name in names])

        async def _download_(self, idx: int) -> None:
            await download_url_write(self, idx)

        async def _prep_(self, idx: int) -> None:
            _ = idx

        def get_hour_images(self, date: StarDate) -> List[str]:
            query_c = "*" + "_".join(date.str().split("-"))[:-4] + "**"
            return glob.glob(self.filepath(query_c))

        def _path_prep(self, scrap_date: List[Tuple[datetime, datetime]]):
            new_scrap_date: StarInterval = StarInterval(
                scrap_date, timedelta(days=1), "%Y%m%d"
            )
            return [
                *chain.from_iterable(
                    [self.get_hour_images(date) for date in new_scrap_date]
                )
            ]
