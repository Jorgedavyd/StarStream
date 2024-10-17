from starstream._utils import (
    StarDate,
    asyncGZIP,
    download_url_prep,
    handle_client_connection_error,
    scrap_url_default,
)
from typing import Callable, List, Tuple, Union
from starstream.typing import ScrapDate
from starstream._base import Img
from bs4 import BeautifulSoup
import aiofiles
import os
from datetime import datetime

VALID_INSTRUMENTS = ["fe094", "fe131", "fe171", "fe195", "fe284", "he304"]


def to_doy_year(date: str) -> str:
    return date[:4] + datetime.strptime(date, "%Y%m%d").strftime("%j")


class GOES16(Img):
    def __init__(
        self,
        instrument: str,
        granularity: float = 1.0,
        root: str = "./data/GOES16/",
        batch_size: int = 10,
    ) -> None:
        self.instrument = f"suvi-l1b-{instrument}"
        super().__init__(
            root=os.path.join(root, self.instrument),
            batch_size=batch_size,
            filepath=lambda name: os.path.join(root, self.instrument, name),
        )
        assert 0 <= granularity <= 1, "Not valid granularity, must be < |1|"
        assert (
            instrument in VALID_INSTRUMENTS
        ), f"Not valid instrument: {instrument}, must be {VALID_INSTRUMENTS}"
        self.url: Callable[[str, str], str] = (
            lambda name, date: f"https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l1b/{self.instrument}/{date[:4]}/{date[4:6]}/{date[6:]}/{name}"
        )
        self.granularity: float = granularity

    def _find_local(self, date: StarDate) -> Tuple[bool, Union[List[str], None]]:
        try:
            files: List[str] = os.listdir(self.root)
            filepaths: List[str] = list(
                map(
                    self.filepath, filter(lambda y: to_doy_year(date.str()) in y, files)
                )
            )
            return bool(len(filepaths)), filepaths
        except FileNotFoundError:
            return False, None

    def _interval_setup(self, scrap_date: ScrapDate) -> None:
        super()._interval_setup(scrap_date)
        self.scrap_urls = [self.url("", date.str()) for date in self.dates]

    @handle_client_connection_error(max_retries=3, increment="exp", default_cooldown=5)
    async def _scrap_(self, idx: int) -> None:
        try:
            date: StarDate = self.dates[idx]
            names: List[str] = await scrap_url_default(self, idx, self.manipulate_html)
            self.urls.extend(list(map(lambda y: self.url(y, date.str()), names)))
            self.paths.extend(
                list(map(lambda y: self.filepath(y)[:-8] + ".fits", names))
            )
        except IndexError:
            return

    async def _download_(self, idx: int) -> None:
        await download_url_prep(self, idx, asyncGZIP, self._to_fits, idx)

    async def _to_fits(self, gzip_file, idx: int) -> None:
        async with aiofiles.open(self.paths[idx], "wb") as file:
            await file.write(gzip_file.read())

    async def manipulate_html(self, html) -> List[Union[str, None]]:
        soup = BeautifulSoup(html, "html.parser")
        href = lambda x: x and x.endswith("fits.gz")
        fits_links = soup.find_all("a", href=href)
        names = list(
            map(
                lambda y: y["href"],
                filter(
                    lambda x: (x is not None) and (x["href"] is not None), fits_links
                ),
            )
        )
        names = [
            name
            for idx, name in enumerate(names)
            if idx % round(1 / self.granularity) == 0
        ]
        return names

    async def _prep_(self, idx: int) -> None:
        _ = idx
