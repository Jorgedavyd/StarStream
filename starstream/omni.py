from .utils import asyncCDF, datetime_interval
from ._base import CDAWeb
import pandas as pd
import aiofiles
import asyncio
import os
from dateutil.relativedelta import relativedelta

__all__ = ["OMNI"]


class OMNI(CDAWeb):
    def __init__(self) -> None:
        super().__init__()
        self.url = (
            lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/omni/hro2_5min/{date[:4]}/omni_hro2_5min_{date}01_v01.cdf"
        )
        self.phy_obs = [
            "BX_GSE",
            "BY_GSE",
            "BZ_GSE",
            "Mach_num",
            "Mgs_mach_num",
            "PR-FLX_10",
            "PR-FLX_30",
            "PR-FLX_60",
            "proton_density",
            "flow_speed",
            "Vx",
            "Vy",
            "Vz",
        ]
        self.variables = self.phy_obs
        self.csv_path = lambda date: f"./data/OMNI/HRO2/{date}.csv"
        self.cdf_path = lambda date: f"./data/OMNI/HRO2/{date}.cdf"
        os.makedirs("./data/OMNI/HRO2/", exist_ok=True)

    def check_tasks(self, scrap_date):
        scrap_date = datetime_interval(*scrap_date, relativedelta(months=1), "%Y%m")
        self.new_scrap_date_list = [
            date for date in scrap_date if not os.path.exists(self.csv_path(date))
        ]

    async def download_url(self, session, date):
        async with session.get(self.url(date), ssl=False) as response:
            cdf_data = await response.read()
            async with aiofiles.open(self.cdf_path(date), mode="wb") as f:
                await f.write(cdf_data)

    def get_download_tasks(self, session):
        return [self.download_url(session, date) for date in self.new_scrap_date_list]

    async def preprocessing(self, date):
        await asyncCDF(self.cdf_path(date), self.default_cda_processing, date)
        os.remove(self.cdf_path(date))

    def get_preprocessing_tasks(self):
        return [self.preprocessing(date) for date in self.new_scrap_date_list]

    async def downloader_pipeline(self, scrap_date, session):
        self.check_tasks(scrap_date)
        await asyncio.gather(*self.get_download_tasks(session))
        await asyncio.gather(*self.get_preprocessing_tasks())

    async def get_df(self, date):
        df = await asyncio.get_event_loop().run_in_executor(
            None, pd.read_csv, self.csv_path(date)
        )
        return pd.from_pandas(df)

    def get_dfs(self, scrap_date):
        return [self.get_df(date) for date in scrap_date]

    async def data_prep(self, scrap_date):
        dfs = await asyncio.gather(*self.get_dfs(scrap_date))
        return pd.concat(dfs)
