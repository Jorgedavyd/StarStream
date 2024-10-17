from typing import Callable, List, Tuple
from starstream._base import Img
from starstream._utils import (
    StarDate,
    download_url_write,
    scrap_url_default,
)
from datetime import timedelta
from bs4 import BeautifulSoup
import glob
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
                lambda name: osp.join(root, name),
                timedelta(hours=1),
                "%Y%m%d-%H%M",
            )
            self.url: Callable[[str, str, str], str] = (
                lambda date, hour, name: f"https://xrt.cfa.harvard.edu/level1/{date[:4]}/{date[4:6]}/{date[6:]}/H{hour[:2]}00/{name}"
            )

        def _find_local(self, date: StarDate) -> Tuple[bool, List[str]]:
            query_c = "*" + "_".join(date.str().split("-"))[:-4] + "**"
            out = glob.glob(self.filepath(query_c))
            return bool(len(out)), out

        def _interval_setup(self, scrap_date: ScrapDate) -> None:
            super()._interval_setup(scrap_date)
            self.scrap_urls: List[str] = [
                self.url(*date.str().split("-"), "") for date in self.dates
            ]

        async def _scrap_(self, idx: int) -> None:
            try:
                date: str = self.dates[idx].str()
                await scrap_url_default(self, idx, self.scrap, *date.split("-"))
            except IndexError:
                return

        def scrap(self, html, date: str, hour: str) -> None:
            soup = BeautifulSoup(html, "html.parser")
            names = soup.find_all(
                "a", href=lambda href: href and href.endswith(".fits")
            )
            names: List[str] = [name["href"] for name in names]
            self.urls.extend([self.url(date, hour, name) for name in names])
            self.paths.extend([self.filepath(name) for name in names])

        async def _download_(self, idx: int) -> None:
            await download_url_write(self, idx)

        async def _prep_(self, idx: int) -> None:
            _ = idx
