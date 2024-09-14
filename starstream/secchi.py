import asyncio
from typing import Coroutine, List, Callable, Sequence, Tuple, Union
from .utils import datetime_interval, handle_client_connection_error

from tqdm import tqdm
import aiofiles
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import glob
from itertools import chain
import os
import os.path as osp
import re

__all__ = ["STEREO_A"]


def url(name: str) -> str:
    m = re.search(r"(\d{6})_(\d{3})(\d{3})eu_R\.png$", name)
    if m is None:
        raise ValueError("Invalid name format")

    date = m.group(1)
    wavelength = m.group(3)

    url = f"https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/wavelets/pngs/{date[:6]}/{date[6:]}/{wavelength}/{name}"
    return url


def parseUrl(url: str) -> Tuple[str, str]:
    url_match = re.search(r"(\d{8})_(\d{6})_(\d{3})eu_R.png", url)
    date: str = url_match.group(1)
    wavelength: str = url_match.group(3)
    return date, wavelength


class STEREO_A:
    class SECCHI:
        class EUVI:
            def __init__(
                self,
                wavelength: str | Sequence[str],
                download_path: str = "./data/STEREO_A/SECCHI/EUVI",
                batch_size: int = 10,
            ) -> None:
                self.root_path: str = download_path
                self.batch_size: int = batch_size
                self.wavelength: str | Sequence[str] = (
                    wavelength if not isinstance(wavelength, str) else [wavelength]
                )
                self.url: Callable[[str, str, str], str] = (
                    lambda date, wavelength, name: f"https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/wavelets/pngs/{date[:6]}/{date[6:]}/{wavelength}_A/{name}"
                )
                self.scrap_url: Callable[[str, str], str] = (
                    lambda date, wavelength: f"https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/wavelets/pngs/{date[:6]}/{date[6:]}/{wavelength}_A"
                )
                self.euvi_png_path: Callable[[str], str] = lambda name: osp.join(
                    self.root_path, f"{name.split('_')[-2][:3]}", name
                )
                self.root_path_png_scrap: Callable[[str, str], str] = (
                    lambda date, wavelength: osp.join(
                        self.root_path, wavelength, f"{date}*"
                    )
                )
                self.wavelengths: List[str] = ["171", "195", "284", "304"]
                for wavelength in self.wavelength:
                    os.makedirs(osp.join(self.root_path, wavelength), exist_ok=True)

            async def scrap_date_names(
                self, session, date: str, wavelength: str
            ) -> Union[List[str], None]:
                url: str = self.scrap_url(date, wavelength)
                async with session.get(url, ssl=False) as response:
                    if response.status != 200:
                        print(
                            f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                        )
                        self.new_scrap_date_list.remove(date)
                    else:
                        html = await response.text()
                        if "404 not found" in html:
                            print(
                                f"{self.__class__.__name__}: Data not available for date: {date}, queried url: {url}"
                            )
                            self.new_scrap_date_list.remove(date)
                            return
                        soup = BeautifulSoup(html, "html.parser")

                        names = [
                            name["href"]
                            for name in soup.find_all(
                                "a", href=lambda href: href.endswith("R.png")
                            )
                        ]

                        return names

            @handle_client_connection_error(
                increment="exp", default_cooldown=5, max_retries=3
            )
            async def download_url(self, session, name: str) -> None:
                date, wavelength = parseUrl(name)
                url: str = self.url(date, wavelength, name)
                async with session.get(url, ssl=False) as response:
                    if response.status != 200:
                        print(
                            f"{self.__class__.__name__}: Data not available for {name}, queried url: {url}"
                        )
                    else:
                        async with aiofiles.open(
                            self.euvi_png_path(name), "wb"
                        ) as file:
                            await file.write(await response.read())

            def get_scrap_names_tasks(self, session) -> List[Coroutine]:
                return [
                    self.scrap_date_names(session, date, wavelength)
                    for date in self.new_scrap_date_list
                    for wavelength in self.wavelength
                ]

            def check_tasks(self, scrap_date: Tuple[datetime, datetime]) -> None:
                new_scrap_date: List[str] = datetime_interval(
                    *scrap_date, timedelta(days=1)
                )
                self.new_scrap_date_list = [
                    date
                    for date in new_scrap_date
                    for wavelength in self.wavelength
                    if len(glob.glob(self.root_path_png_scrap(date, wavelength))) == 0
                ]

            def data_prep(self, scrap_date: Tuple[datetime, datetime]):
                new_scrap_date = datetime_interval(
                    scrap_date[0], scrap_date[-1], timedelta(days=1)
                )
                out = [
                    glob.glob(self.root_path_png_scrap(date, wavelength))
                    for date in new_scrap_date
                    for wavelength in self.wavelength
                ]
                out = [*chain.from_iterable(out)]

                return out

            def get_download_tasks(
                self, session, name_list: List[str]
            ) -> List[Coroutine]:
                return [self.download_url(session, name) for name in name_list]

            async def downloader_pipeline(
                self, scrap_date: Tuple[datetime, datetime], session
            ) -> None:
                self.check_tasks(scrap_date)
                if len(self.new_scrap_date_list) == 0:
                    print(f"{self.__class__.__name__}: Already downloaded")
                else:
                    name_list: List = []
                    scrap_tasks: List[Coroutine] = self.get_scrap_names_tasks(session)

                    for i in tqdm(
                        range(0, len(scrap_tasks), self.batch_size),
                        desc=f"Preprocessing for {self.__class__.__name__}...",
                    ):
                        name_batch = await asyncio.gather(
                            *scrap_tasks[i : i + self.batch_size]
                        )
                        name_batch = [*chain.from_iterable(name_batch)]
                        name_list.extend(name_batch)

                    name_list = [name for name in name_list if name is not None]

                    downloading_tasks = self.get_download_tasks(session, name_list)

                    for i in tqdm(
                        range(0, len(downloading_tasks), self.batch_size),
                        desc=f"Downloading for {self.__class__.__name__}...",
                    ):
                        await asyncio.gather(
                            *downloading_tasks[i : i + self.batch_size]
                        )
