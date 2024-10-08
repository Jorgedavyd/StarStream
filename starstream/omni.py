from dateutil.relativedelta import relativedelta
from .utils import StarInterval
from typing import List, Tuple
from datetime import datetime
from ._base import CDAWeb
from tqdm import tqdm
import os

__all__ = ["OMNI"]


class OMNI(CDAWeb):
    def __init__(self, download_path: str = "./data/OMNI/HRO2/", batch_size: int = 10) -> None:
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

    def _check_tasks(self, scrap_date_list: List[Tuple[datetime, datetime]]):
        new_scrap_date: StarInterval = StarInterval(scrap_date_list, relativedelta(months = 1), '%Y%m')

        for date in tqdm(
            new_scrap_date,
            desc=f"{self.__class__.__name__}: Looking for missing dates...",
        ):
            if not os.path.exists(self.csv_path(date.str())):
                self.new_scrap_date_list.append(date)

        if self.new_scrap_date_list:
            os.makedirs(self.root_path, exist_ok=True)
