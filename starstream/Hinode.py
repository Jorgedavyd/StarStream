from .utils import asyncFITS, datetime_interval
from datetime import timedelta
from datetime import datetime
from bs4 import BeautifulSoup
from io import BytesIO
import numpy as np
import asyncio
import os
import glob
from PIL import Image
import aiofiles
from itertools import chain
class Hinode:
    class XRT:
        xrt_npy_path = lambda self, name: f"./data/Hinode/XRT/{name}.png"
        xrt_fits_path = lambda self, name: f"./data/Hinode/XRT/{name}.fits"
        xrt_npy_glob_query = lambda self, name: f"./data/Hinode/XRT/{name}.png"
        xrt_folder_path = "./data/Hinode/XRT/"
        download_urls = []
        url = (
            lambda self, date, hour: f"https://xrt.cfa.harvard.edu/level1/{date[:4]}/{date[4:6]}/{date[6:]}/H{hour[:2]}00/"
        )
        def __init__(self, filetype: str = 'png') -> None:
            self.filetype = filetype
        def get_check_tasks(self, scrap_date):
            scrap_date = datetime_interval(scrap_date[0], scrap_date[-1], timedelta(hours = 1), '%Y%m%d-%H%M')
            self.new_scrap_date_list = [
                date.split('-')
                for date in scrap_date
                if len(glob.glob(self.xrt_npy_path(date, '')[:-5] + '*.png')) == 0
            ]

        def get_scrap_tasks(self, session):
            return [
                self.scrap_names(session, date, hour) for date, hour in self.new_scrap_date_list
            ]

        async def scrap_names(self, session, date, hour):
            async with session.get(self.url(date, hour)) as response:
                html = await response.text
                soup = BeautifulSoup(html, "html.parser")
                fits_links = soup.find_all(
                    "a", href=lambda href: href and href.endswith(".fits")
                )
                self.download_urls.append(
                    [
                        self.url(date, hour) + link["href"]
                        for link in fits_links
                    ]
                )

        def get_download_tasks(self, session):
            return [
                self.download_url(session, url)
                for url in self.download_urls
            ]

        def fits_processing(self, fits_file, path):
            image = fits_file[0].data
            np.save(path, image)

        async def download_url(self, session, url):
            async with session.get(url) as response:
                data = await response.read()
                if self.filetype == 'png':
                    await asyncFITS(
                        BytesIO(data), self.fits_processing, self.xrt_npy_path(url.split('/')[-1][7:-8] + '.png')
                    )
                elif self.filetype == 'fits':
                    async with aiofiles.open(self.xrt_fits_path(url.split('/')[-1][7:-8] + '.fits')) as file:
                        await file.write(data)

        async def downloader_pipeline(self, scrap_date, session):
            await asyncio.gather(*self.get_check_tasks(scrap_date))
            await asyncio.gather(*self.get_download_tasks(session))

        def get_hour_images(self, date):
            query_c = "*" + "_".join(date.split("-"))[:-4] + "**"
            return glob.glob(self.xrt_npy_path(query_c))

        async def data_prep(self, scrap_date):
            scrap_date = datetime_interval(
                scrap_date[0],
                scrap_date[-1],
                step_size=timedelta(days=1),
            )
            
            return list(*chain.from_iterable([glob.glob(self.xrt_folder_path + date + '*') for date in scrap_date]))
