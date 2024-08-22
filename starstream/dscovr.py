from types import coroutine
from .utils import datetime_interval, timedelta_to_freq, asyncGZ
from datetime import timedelta, datetime
from .utils import MHD
from io import BytesIO
import xarray as xr
import asyncio
import os
import time
from typing import Coroutine, Dict, Sequence, Tuple, Callable, List
import os.path as osp
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import chromedriver_binary  # Adds chromedriver binary to path

__all__ = ["DSCOVR"]


class DSCOVR(MHD):
    def __init__(self) -> None:
        super().__init__()

        self.fc1_root: Callable[[str], str] = lambda date: f"./data/DSCOVR/L1/faraday/{date}.csv"
        self.mg1_root: Callable[[str], str] = lambda date: f"./data/DSCOVR/L1/magnetometer/{date}.csv"
        self.f1m_root: Callable[[str], str] = lambda date: f"./data/DSCOVR/L2/faraday/{date}.csv"
        self.m1m_root: Callable[[str], str] = lambda date: f"./data/DSCOVR/L2/magnetometer/{date}.csv"
        self.mg_var: List[str] = ["bx_gsm", "by_gsm", "bz_gsm", "bt"]
        self.fc_var: List[str] = ["proton_density", "proton_speed", "proton_temperature"]
        self.roots: List[Callable[[str], str]] = [self.fc1_root, self.mg1_root, self.f1m_root, self.m1m_root]
        self.var_meta: Dict[str, List[Callable[[str], str]]] = {
            "fc1": [self.fc1_root, self.fc_var],
            "mg1": [self.mg1_root, self.mg_var],
            "f1m": [self.f1m_root, self.fc_var],
            "m1m": [self.m1m_root, self.mg_var],
        }
        os.makedirs("./data/DSCOVR/L1/faraday/", exist_ok=True)
        os.makedirs("./data/DSCOVR/L1/magnetometer/", exist_ok=True)
        os.makedirs("./data/DSCOVR/L2/faraday/", exist_ok=True)
        os.makedirs("./data/DSCOVR/L2/magnetometer/", exist_ok=True)

    def to_unix(self, scrap_date: Sequence[datetime]) -> List[int]:
        timestamp = [
            int(time.mktime(datetime(*date.timetuple()[:3]).timetuple())) * 1000
            for date in scrap_date
        ]
        return timestamp

    def check_update(self, scrap_date: Tuple[datetime, datetime]) -> None:
        update_path = osp.join(osp.dirname(__file__), "trivials/last_update.txt")
        try:
            with open(update_path, "r") as file:
                date = datetime.strptime(file.readlines()[0], "%Y%m%d")
            if scrap_date[-1] > date:
                os.remove(update_path)
                self.scrap_links((date + timedelta(days=1), scrap_date[-1]))
        except FileNotFoundError:
            # create the folder where the urls will be stored
            os.makedirs(osp.dirname(update_path), exist_ok=False)
            # scrap links from the page
            self.scrap_links(
                (datetime(2016, 7, 26), datetime.today() - timedelta(days=1))
            )

    def scrap_links(self, scrap_date: Tuple[datetime, datetime]) -> None:
        print("Updating url dataset...")
        unix = self.to_unix(scrap_date)
        url = (
            lambda unix: f"https://www.ngdc.noaa.gov/dscovr/portal/index.html#/download/{unix[0]};{unix[-1]}/f1m;fc1;m1m;mg1"
        )
        # Render a chrome like browser to enter the url
        op = webdriver.ChromeOptions()
        op.add_argument("headless")
        driver = webdriver.Chrome(options=op)
        driver.get(url(unix))
        # Wait for rendering
        time.sleep(10)
        # Render the html
        html = driver.page_source
        driver.quit()
        # Scrap the page up to the last day
        soup = BeautifulSoup(html, "html.parser")
        value = soup.find("input", class_="form-control input-sm cursor-text")["value"]
        url_path = osp.join(osp.dirname(__file__), "trivials/url.txt")
        with open(url_path, "a") as file:
            file.write(value[5:].replace(" ", "\n") + "\n")
        update_path = osp.join(osp.dirname(__file__), "trivials/last_update.txt")
        with open(update_path, "x") as file:
            file.write(scrap_date[-1].strftime("%Y%m%d"))

    def check_tasks(self, scrap_date: Tuple[datetime, datetime]) -> None:
        self.check_update(scrap_date)
        new_scrap_date: List[str] = datetime_interval(*scrap_date, timedelta(days=1))
        self.new_scrap_date_list = [
            date for date in new_scrap_date if not os.path.exists(self.mg1_root(date))
        ]

    def gz_processing(self, gz_file, url: str, date: str) -> None:
        tool = url.split("_")[1]
        dataset = xr.open_dataset(gz_file.read())
        df = dataset.to_dataframe()
        dataset.close()
        faraday_cup = df[self.var_meta[tool][1]]
        faraday_cup = faraday_cup.resample("1T").mean()
        faraday_cup.to_csv(self.var_meta[tool][0](date))

    async def download_url(self, url: str, date: str, session) -> None:
        async with session.get(url, ssl=True) as response:
            data = await response.read()
            await asyncGZ(BytesIO(data), self.gz_processing, url, date)

    def get_urls(self) -> List[str]:
        with open(osp.join(osp.dirname(__file__), "trivials/url.txt"), "r") as file:
            lines = file.readlines()
        url_list = []
        for url in lines:
            for date in self.new_scrap_date_list:
                if date + "000000" in url:
                    url_list.append((url, date))
        return url_list

    def get_download_tasks(self, session) -> List[Coroutine]:
        self.urls_dates = self.get_urls()
        return [self.download_url(url, date, session) for url, date in self.urls_dates]

    """Prep pipeline"""

    def get_df(self, path: Callable[[str], str], date: str, obs: List[str]) -> pd.DataFrame:
        df = pd.read_csv(self.path(date), index_col=0)
        return df[obs]

    def get_dfs(self, path: Callable[[str], str], scrap_date: List[str], obs: List[str]):
        dfs = [self.get_df(path, date, obs) for date in scrap_date]
        return pd.concat(dfs)

    def data_prep(self, scrap_date: Tuple[datetime, datetime], step_size: timedelta):
        init, end = [pd.to_datetime(date) for date in scrap_date]
        new_scrap_date: List[str] = datetime_interval(*scrap_date, timedelta(days=1))

        fc1 = self.get_dfs(
            self.fc1_root,
            new_scrap_date,
            ["proton_density", "proton_speed", "proton_temperature"],
        )
        mg1 = self.get_dfs(
            self.mg1_root, new_scrap_date, ["bx_gsm", "by_gsm", "bz_gsm", "bt"]
        )
        f1m = self.get_dfs(
            self.f1m_root,
            new_scrap_date,
            ["proton_density", "proton_speed", "proton_temperature"],
        )
        m1m = self.get_dfs(
            self.m1m_root, new_scrap_date, ["bx_gsm", "by_gsm", "bz_gsm", "bt"]
        )

        l1 = pd.concat(
            [
                fc1.resample(timedelta_to_freq(step_size)).mean(),
                mg1.resample(timedelta_to_freq(step_size)).mean(),
            ],
            axis=1,
        )
        l2 = pd.concat(
            [
                f1m.resample(timedelta_to_freq(step_size)).mean(),
                m1m.resample(timedelta_to_freq(step_size)).mean(),
            ],
            axis=1,
        )

        # sampling
        l1 = l1[(l1.index >= init) & (l1.index <= end)]
        l2 = l2[(l2.index >= init) & (l2.index <= end)]

        return {
            "l1": self.apply_features(
                l1, "bt", "proton_density", "proton_speed", "proton_temperature"
            ),
            "l2": self.apply_features(
                l2, "bt", "proton_density", "proton_speed", "proton_temperature"
            ),
        }

    """Downloader pipeline"""

    async def downloader_pipeline(self, scrap_date: tuple[datetime, datetime], session):
        self.check_tasks(scrap_date)
        if self.new_scrap_date_list == []:
            print("Already downloaded")
        else:
            print(
                f'Got all urls for: {scrap_date[0].strftime("%Y%m%d")} to {scrap_date[-1].strftime("%Y%m%d")}'
            )
            await asyncio.gather(*self.get_download_tasks(session))
