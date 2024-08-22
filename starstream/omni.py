from datetime import datetime
from typing import Coroutine, List, Tuple
from .utils import asyncCDF, datetime_interval
from ._base import CDAWeb
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

    def check_tasks(self, scrap_date: Tuple[datetime, datetime]):
        new_scrap_date: List[str] = datetime_interval(
            *scrap_date, relativedelta(months=1), "%Y%m"
        )
        self.new_scrap_date_list: List[str] = [
            date for date in new_scrap_date if not os.path.exists(self.csv_path(date))
        ]

    async def download_url(self, session, date: str):
        async with session.get(self.url(date), ssl=False) as response:
            cdf_data = await response.read()
            async with aiofiles.open(self.cdf_path(date), mode="wb") as f:
                await f.write(cdf_data)

    def get_download_tasks(self, session) -> List[Coroutine]:
        return [self.download_url(session, date) for date in self.new_scrap_date_list]

    async def preprocessing(self, date: str) -> None:
        await asyncCDF(self.cdf_path(date), self.default_cda_processing, date)
        os.remove(self.cdf_path(date))

    def get_preprocessing_tasks(self) -> List[Coroutine]:
        return [self.preprocessing(date) for date in self.new_scrap_date_list]

    async def downloader_pipeline(self, scrap_date: Tuple[datetime, datetime], session):
        self.check_tasks(scrap_date)
        await asyncio.gather(*self.get_download_tasks(session))
        await asyncio.gather(*self.get_preprocessing_tasks())
