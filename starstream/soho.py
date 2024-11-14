from collections.abc import Callable
from typing import List, Union
from dateutil.relativedelta import relativedelta
from starstream._utils import StarInterval, asyncTAR, handle_client_connection_error
from datetime import timedelta
from starstream.typing import ScrapDate
from ._base import CDAWeb, CSV
from io import BytesIO
import polars as pl
import aiofiles
import asyncio
import glob
import os
import os.path as osp

__all__ = ["SOHO"]


class SOHO:
    class CELIAS_SEM(CDAWeb):
        def __init__(
            self, download_path: str = "./data/SOHO/CELIAS_SEM", batch_size: int = 10
        ) -> None:
            super().__init__(download_path, batch_size)
            self.phy_obs: List[str] = [
                "CH1",
                "CH2",
                "CH3",
                "first_order_flux",
                "central_order_flux",
            ]
            self.variables: List[str] = self.phy_obs
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/soho/celias/sem_15s/{date[:4]}/soho_celias-sem_15s_{date}_v04.cdf"
            )

    class CELIAS_PM(CDAWeb):
        def __init__(
            self, download_path: str = "./data/SOHO/CELIAS_PM", batch_size: int = 10
        ) -> None:
            super().__init__(download_path, batch_size)
            self.phy_obs: List[str] = [
                "N_p",
                "V_p",
                "V_He",
                "NS_angle",
                "Vth_p",
            ]
            self.variables: List[str] = self.phy_obs  # variables#change
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/soho/celias/pm_30s/{date[:4]}/soho_celias-pm_30s_{date}_v02.cdf"
            )

    class ERNE(CDAWeb):
        def __init__(
            self, download_path: str = "./data/SOHO/ERNE/", batch_size: int = 10
        ) -> None:
            super().__init__(download_path, batch_size)
            self.phy_obs: List[str] = [
                "PH",
                "PHC",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/soho_erne-hed_l2-1min_00000000_v01.skt
            energy_channels: List[str] = [
                "13  - 16  MeV",
                "16  - 20  MeV",
                "20  - 25  MeV",
                "25  - 32  MeV",
                "32  - 40  MeV",
                "40  - 50  MeV",
                "50  - 64  MeV",
                "64  - 80  MeV",
                "80  - 100 MeV",
                "100 - 130 MeV",
            ]
            self.variables: List[str] = [
                f"PH_{energy}" for energy in energy_channels
            ] + [f"PHC_{energy}" for energy in energy_channels]

            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/soho/erne/hed_l2-1min/{date[:4]}/soho_erne-hed_l2-1min_{date}_v01.cdf"
            )

    class COSTEP_EPHIN(CSV):
        date_sampling: Union[timedelta, relativedelta] = relativedelta(years=1)
        format: str = "%Y"

        def __init__(self, root: str = "./data/SOHO/COSTEP_EPHIN") -> None:
            super().__init__(
                root=root,
                batch_size=1,
                filepath=lambda date: osp.join(root, f"{date}.csv"),
            )
            self.root: str = "./data/SOHO/COSTEP_EPHIN"
            self.l3i_path: Callable[[str], str] = lambda date: osp.join(
                root, f"{date}.l3i"
            )
            self.url: str = (
                "https://soho.nascom.nasa.gov/data/EntireMissionBundles/COSTEP_EPHIN_L3_l3i_5min-EntireMission-ByYear.tar.gz"
            )
            self.name: str = "COSTEP_EPHIN_L3_l3i_5min-EntireMission-ByYear.tar.gz"
            self.columns: List[str] = [
                "year",
                "month",
                "day",
                "hour",
                "minute",
                "int_p4",
                "int_p8",
                "int_p25",
                "int_p41",
                "int_h4",
                "int_h8",
                "int_h25",
                "int_h41",
            ]

        def _interval_setup(self, scrap_date: ScrapDate) -> None:
            self.downloaded = len(glob.glob(self.root + "/*")) == 30
            if self.downloaded:
                pass
            else:
                self.dates = [
                    date
                    for date in StarInterval(scrap_date, relativedelta(years=1), "%Y")
                ]

            if self.dates:
                os.makedirs(self.root, exist_ok=True)

        async def _scrap_(self, idx: int) -> None:
            _ = idx

        @handle_client_connection_error(
            max_retries=5, default_cooldown=5, increment="exp"
        )
        async def _download_(self, idx: int):
            _ = idx
            async with self.session.get(self.url) as response:
                if response.status == 200:
                    data = await response.read()
                    await asyncTAR(BytesIO(data), self.get_processing)
            await asyncio.gather(*self.get_preprocessing_tasks())

        async def _prep_(self, idx: int) -> None:
            _ = idx

        def get_processing(self, tar_file):
            tar_file.extractall(self.root)

        def get_preprocessing_tasks(self):
            return [
                self.preprocessing(year_path)
                for year_path in glob.glob(self.root + "/5min/*")
            ]

        async def preprocessing(self, year_path: str):
            async with aiofiles.open(
                year_path[:-3] + "csv", "w"
            ) as csv_file, aiofiles.open(year_path, "r") as l3i:
                await csv_file.write(",".join(self.columns) + "\n")
                lines = await l3i.readlines()
                for line in lines[3:]:
                    data = line.split()
                    await csv_file.write(
                        ",".join(data[:3] + data[4:6] + data[8:12] + data[20:24]) + "\n"
                    )

            os.remove(year_path)
            df = pl.read_csv(year_path[:-3] + "csv")
            df = df.with_columns(
                pl.datetime(
                    year="year", month="month", day="day", hour="hour", minute="minute"
                ).alias("datetime")
            )
            df = df.drop(["year", "month", "day", "hour", "minute"])
            df = df.set_sorted("datetime")
            df.write_csv(year_path[:-3] + "csv")
