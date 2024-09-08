from datetime import datetime
from typing import Coroutine, List, Tuple

from tqdm import tqdm
from .utils import asyncCDF, datetime_interval
from ._base import CDAWeb
import aiofiles
import asyncio
import os
from dateutil.relativedelta import relativedelta

__all__ = ["OMNI"]


class OMNI(CDAWeb):
    def __init__(
        self, download_path: str = "./data/OMNI/HRO2/", batch_size: int = 10
    ) -> None:
        super().__init__(download_path, batch_size)
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

    def check_tasks(self, scrap_date: Tuple[datetime, datetime]):
        print(f"{self.__class__.__name__}: Looking for missing dates...")
        new_scrap_date: List[str] = datetime_interval(
            *scrap_date, relativedelta(months=1), "%Y%m"
        )
        self.new_scrap_date_list: List[str] = [
            date for date in new_scrap_date if not os.path.exists(self.csv_path(date))
        ]

    def get_download_tasks(self, session) -> List[Coroutine]:
        return [self.download_url(session, date) for date in self.new_scrap_date_list]

    async def preprocessing(self, date: str) -> None:
        await asyncCDF(self.cdf_path(date), self.default_cda_processing, date)
        os.remove(self.cdf_path(date))

    def get_preprocessing_tasks(self) -> List[Coroutine]:
        return [self.preprocessing(date) for date in self.new_scrap_date_list]

    async def downloader_pipeline(self, scrap_date: Tuple[datetime, datetime], session):
        self.check_tasks(scrap_date)

        downloading_tasks = self.get_download_tasks(session)
        for i in tqdm(
            range(0, len(downloading_tasks), self.batch_size),
            desc=f"Downloading for {self.__class__.__name__}",
        ):
            await asyncio.gather(*downloading_tasks[i : i + self.batch_size])

        prep_tasks = self.get_preprocessing_tasks()
        for i in tqdm(
            range(0, len(prep_tasks), self.batch_size),
            desc=f"Downloading for {self.__class__.__name__}",
        ):
            await asyncio.gather(*prep_tasks[i : i + self.batch_size])
