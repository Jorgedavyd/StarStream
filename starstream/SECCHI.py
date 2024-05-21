import asyncio
import aiohttp
import aiofiles
from .utils import datetime_interval
from datetime import timedelta
from io import BytesIO
from bs4 import BeautifulSoup
import numpy as np
import glob
from itertools import chain
import os
import time
from PIL import Image

"""
volver a probar cor2
"""


class STEREO:
    class SECCHI:
        class COR2:
            url = (
                lambda self, date, name: f"https://secchi.nrl.navy.mil/postflight/cor2/L0/a/img/{date}/{name}"
            )
            png_path = lambda self, date, hour: f"./data/SECCHI/COR2/{date}_{hour}.png"
            fits_path = (
                lambda self, date, hour: f"./data/SECCHI/COR2/{date}_{hour}.fits"
            )
            os.makedirs("./data/SECCHI/COR2/", exist_ok=True)

            def check_tasks(self, scrap_date):
                self.new_scrap_date_list = [
                    date
                    for date in scrap_date
                    if len(glob.glob(self.png_path(date, "*"))) == 0
                ]
                print(self.new_scrap_date_list)

            async def scrap_date_names(self, date):
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url(date, ""), ssl=False) as response:
                        html = await response.text()
                        soup = BeautifulSoup(html, "html.parser")
                        names = [
                            name["href"]
                            for name in soup.find_all(
                                "a", href=lambda href: href.endswith("d4c2A.fts")
                            )
                        ]
                        print(names)

            async def download_url(self, name):
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.url(name.split("_")[0], name), ssl=False
                    ) as response, aiofiles.open(
                        self.fits_path(*name.split("_")[:2]), "wb"
                    ) as f:
                        await f.write(await response.read())

            def get_days(self, scrap_date):
                return [
                    *chain.from_iterable(
                        [glob.glob(self.png_path(date, "*")) for date in scrap_date]
                    )
                ]

            def get_scrap_names_tasks(self, scrap_date):
                return [self.scrap_date_names(date) for date in scrap_date]

            async def downloader_pipeline(self, scrap_date):
                self.check_tasks(scrap_date)
                names = await asyncio.gather(*self.get_scrap_names_tasks(scrap_date))
                names = [*chain.from_iterable(names)]
                await asyncio.gather(*[self.download_url(name) for name in names])

            def data_prep(self, scrap_date):
                scrap_date = datetime_interval(
                    scrap_date[0], scrap_date[-1], timedelta(days=1)
                )
                return self.get_days(scrap_date)

            def calibration(self):
                """
                Calibration steps:

                1. divide by exposure duration,
                2. correcting for onboard image processing,
                3. subtracting the CCD bias,
                4. multiplying by the calibration factor (converts the image values from digital number (DN) to the physical units of mean solar brightness (MSB)),
                5. vignetting and flat-field correction, and
                6. optical distortion correction (COR2 only).
                """

        class EUVI:
            url = (
                lambda self, date, wavelength, name: f"https://stereo-ssc.nascom.nasa.gov/data/ins_data/secchi/wavelets/pngs/{date[:6]}/{date[6:]}/{wavelength}_A/{name}"
            )
            euvi_png_path = (
                lambda self, wavelength, name: f'./data/SECCHI/EUVI/{name.split("_")[-2][:3]}/{name}'
            )
            wavelengths = ["171", "195", "284", "304"]
            for wavelength in wavelengths:
                os.makedirs(f"./data/SECCHI/EUVI/{wavelength}", exist_ok=True)

            async def scrap_date_names(self, session, date):
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

            async def download_url(self, session, name):
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

            def get_scrap_names_tasks(self, scrap_date, session):
                return [self.scrap_date_names(session, date) for date in scrap_date]

            def check_tasks(self, scrap_date):
                self.new_scrap_date_list = [
                    date
                    for date in scrap_date
                    if len(glob.glob(self.euvi_png_path(date, "171", date + "*"))) == 0
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
