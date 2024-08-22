import asyncio
from typing import Coroutine, List
from .utils import datetime_interval
from datetime import timedelta
from io import BytesIO
from bs4 import BeautifulSoup
import glob
from itertools import chain
import os
from PIL import Image

__all__ = ["STEREO"]


class STEREO:
    class SECCHI:
        class EUVI:
            def __init__(self) -> None:
                self.url: Callable[[str, str, str], str] = (
                    lambda date, wavelength, name: f"https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/wavelets/pngs/{date[:6]}/{date[6:]}/{wavelength}_A/{name}"
                )
                self.euvi_png_path: Callable[[str], str] = (
                    lambda name: f'./data/SECCHI/EUVI/{name.split("_")[-2][:3]}/{name}'
                )
                self.wavelengths: List[str] = ["171", "195", "284", "304"]

                for wavelength in self.wavelengths:
                    os.makedirs(f"./data/SECCHI/EUVI/{wavelength}", exist_ok=True)

            async def scrap_date_names(self, session, date: str) -> List[str]:
                async with session.get(self.url(date, ""), ssl=False) as response:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    sizes = [
                        size.text.strip()
                        for size in soup.find_all(
                            "td",
                            align="right",
                            text=lambda text: text.endswith("M") or text.endswith("K"),
                        )
                    ]
                    names = [
                        name["href"]
                        for name in soup.find_all(
                            "a", href=lambda href: href.endswith("R.png")
                        )
                    ]
                    return [
                        name for size, name in zip(sizes, names) if size.endswith("M")
                    ]

            async def download_url(self, session, name: str) -> None:
                async with session.get(
                    self.url(name.split("_")[0], name), ssl=False
                ) as response:
                    data = await response.read()
                    img = await asyncio.get_event_loop().run_in_executor(
                        None, Image.open, BytesIO(data)
                    )
                    await asyncio.get_event_loop().run_in_executor(
                        None, img.save, self.euvi_png_path(name), "PNG"
                    )

            def get_scrap_names_tasks(self, scrap_date, session) -> List[Coroutine]:
                return [self.scrap_date_names(session, date) for date in scrap_date]

            def check_tasks(self, scrap_date) -> None:
                self.new_scrap_date_list = [
                    date
                    for date in scrap_date
                    if len(glob.glob(self.euvi_png_path(date, date + "*"))) == 0
                ]

            def get_days(self, scrap_date):
                return [
                    *chain.from_iterable(
                        [
                            glob.glob(self.euvi_png_path(date, "*"))
                            for date in scrap_date
                        ]
                    )
                ]

            def data_prep(self, scrap_date):
                scrap_date = datetime_interval(
                    scrap_date[0], scrap_date[-1], timedelta(days=1)
                )
                return self.get_days(scrap_date)

            def get_slice(self, idx: slice):
                pass
