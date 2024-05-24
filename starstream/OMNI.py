from .utils import asyncCDF, scrap_date_to_month, datetime_interval
from ._base import CDAWeb
import pandas as pd
import aiofiles
import asyncio
import os
from datetime import timedelta
from dateutil.relativedelta import relativedelta



class OMNI(CDAWeb):
    url = (
        lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/omni/hro2_5min/{date[0]}/omni_hro2_5min_{date[1]}01_v01.cdf"
    )
    phy_obs = [
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
    variables = ["datetime"] + phy_obs  # GSE
    csv_path = lambda date: f'./data/OMNI/HRO2/{"-".join(date)}.csv'
    cdf_path = lambda date: f'./data/OMNI/HRO2/{"-".join(date)}.cdf'

    def check_tasks(self, scrap_date):
        scrap_date = datetime_interval(*scrap_date, relativedelta(months = 1))
        scrap_date = scrap_date_to_month(scrap_date)
        self.new_scrap_date_list = [
            date for date in scrap_date if not os.path.exists(self.csv_file(date))
        ]

    async def download_url(self, session, date):
        async with session.get(self.url(date), ssl=False) as response:
            cdf_data = await response.read()
            async with aiofiles.open(self.temp_file(date), mode="wb") as f:
                await f.write(cdf_data)

    def get_download_tasks(self, session):
        return [self.download_url(session, date) for date in self.new_scrap_date_list]

    async def preprocessing(self, date):
        await asyncCDF(self.cdf_path(date), self.default_cda_processing, "-".join(date))
        os.remove(self.cdf_path(date))

    def get_preprocessing_tasks(self):
        return [self.preprocessing(date) for date in self.new_scrap_date_list]

    def get_data_tasks(self):
        data = [
            pd.read_csv(
                self.csv_file(date), parse_dates=["datetime"], index_col="datetime"
            )
            for date in self.scrap_date
        ]
        df = pd.concat(data, axis=0).loc[
            pd.Timestamp(self.scrap_date[0]) : pd.Timestamp(self.scrap_date[-1])
        ]
        return df

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
