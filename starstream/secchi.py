from typing import List, Callable
from starstream._utils import download_url_write, scrap_url_default
from starstream._base import Img
from bs4 import BeautifulSoup
import os.path as osp
from datetime import datetime
from starstream.downloader import DataDownloading
from starstream.typing import ScrapDate

__all__ = ["STEREO_A"]


class STEREO_A:
    class SECCHI:
        class EUVI(Img):
            def __init__(
                self,
                wavelength: int,
                root: str = "./data/STEREO_A/SECCHI/EUVI",
                batch_size: int = 10,
            ) -> None:
                super().__init__(
                    root=osp.join(root, str(wavelength)),
                    batch_size=batch_size,
                    filepath=lambda name: osp.join(root, str(wavelength), name),
                )
                self.wavelength: str = str(wavelength)
                self.url: Callable[[str, str], str] = (
                    lambda date, name: f"https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/wavelets/pngs/{date[:6]}/{date[6:]}/{self.wavelength}_A/{name}"
                )
                self.scrap_url: Callable[[str], str] = (
                    lambda date: f"https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/wavelets/pngs/{date[:6]}/{date[6:]}/{self.wavelength}_A"
                )
                self.scrap_path: Callable[[str], str] = lambda date: osp.join(
                    self.root, f"{date}*"
                )

            def _interval_setup(self, scrap_date: ScrapDate) -> None:
                super()._interval_setup(scrap_date)
                self.scrap_urls: List[str] = [
                    self.scrap_url(date.str()) for date in self.dates
                ]

            async def _scrap_(self, idx: int) -> None:
                try:
                    date: str = self.dates[idx].str()
                    await scrap_url_default(self, idx, self.scrap, date)
                except IndexError:
                    return

            def scrap(self, html, date: str) -> None:
                soup = BeautifulSoup(html, "html.parser")
                names = [
                    name["href"]
                    for name in soup.find_all(
                        "a", href=lambda key: key.endswith("R.png")
                    )
                ]
                self.paths.extend(
                    [self.filepath(name) for name in names if name is not None]
                )
                self.urls.extend(
                    [self.url(date, name) for name in names if name is not None]
                )

            async def _download_(self, idx: int) -> None:
                await download_url_write(self, idx)

            async def _prep_(self, idx: int) -> None:
                _ = idx


if __name__ == "__main__":
    DataDownloading(
        STEREO_A.SECCHI.EUVI(171), [(datetime(2014, 10, 1), datetime(2014, 10, 30))]
    )
