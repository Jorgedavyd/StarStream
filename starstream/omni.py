from dateutil.relativedelta import relativedelta
from ._base import CDAWeb

__all__ = ["OMNI"]


class OMNI(CDAWeb):
    def __init__(self, root: str = "./data/OMNI/HRO2/", batch_size: int = 10) -> None:
        super().__init__(
            root=root,
            batch_size=batch_size,
            format="%Y%m",
            date_sampling=relativedelta(months=1),
        )
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
