from utils import asyncFITS, interval_time
from datetime import timedelta
from datetime import datetime
from bs4 import BeautifulSoup
from io import BytesIO
import numpy as np
import asyncio
import os
import glob


class Hinode:
    class XRT:
        xrt_npy_path = lambda date: f"./data/Hinode/XRT/{date}.npy"
        xrt_fits_path = lambda date: f"./data/Hinode/XRT/{date}.fits"
        xrt_npy_glob_query = lambda date: f"./data/Hinode/XRT/{date}.npy"
        xrt_folder_path = "./data/Hinode/XRT/"
        download_urls = []
        url = (
            lambda date, hour: f"https://xrt.cfa.harvard.edu/level1/{date[:4]}/{date[4:6]}/{date[6:]}/H{hour[:2]}00/"
        )

        def __init__(self, sequence_length):
            self.sl = sequence_length

        def get_check_tasks(self, scrap_date):
            scrap_date = interval_time(
                scrap_date[0],
                scrap_date[-1],
                step_size=timedelta(hours=1),
                output_format="%Y%m%d-%H%M%S",
            )
            self.new_scrap_date_list = [
                date
                for date in scrap_date
                if not os.path.exists(self.xrt_npy_path(date))
            ]

        def get_scrap_tasks(self, session):
            return [
                self.scrap_names(session, *date) for date in self.new_scrap_date_list
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
                        ((date, hour), self.url(date, hour) + link["href"])
                        for link in fits_links
                        if link
                        if timedelta(
                            minutes=datetime.strptime(
                                fits_links.split("_")[-1][:-7], "%H%M%S"
                            ).minute
                        )
                        > timedelta(minutes=45)
                    ]
                )

        def get_download_tasks(self, session):
            return [
                self.download_url(session, date, url)
                for date, url in self.download_urls
            ]

        def fits_processing(self, fits_file, path):
            image = fits_file[0].data
            np.save(path, image)

        async def download_url(self, session, date, url):
            async with session.get(url) as response:
                data = await response.read()
                await asyncFITS(
                    BytesIO(data), self.fits_processing, self.xrt_npy_path(date)
                )

        async def downloader_pipeline(self, scrap_date, session):
            await asyncio.gather(*self.get_check_tasks(scrap_date))
            await asyncio.gather(*self.get_download_tasks(session))

        def get_hour_images(self, date):
            query_c = "*" + "_".join(date.split("-"))[:-4] + "**"
            return glob.glob(self.xrt_npy_path(query_c))

        async def data_prep(self, scrap_date):
            scrap_date = interval_time(
                scrap_date[0],
                scrap_date[-1],
                step_size=timedelta(hours=1),
                output_format="%Y%m%d-%H%M%S",
            )
            return [self.xrt_npy_path(date) for date in scrap_date]
