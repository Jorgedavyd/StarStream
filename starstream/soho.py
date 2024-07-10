from .utils import datetime_interval, asyncTAR
from datetime import timedelta, datetime
from ._base import CDAWeb
from io import BytesIO
import pandas as pd
import aiofiles
import asyncio
import glob
import os

__all__ = ["SOHO"]


class SOHO:
    class CELIAS_SEM(CDAWeb):
        csv_path = lambda self, date: f"./data/SOHO/CELIAS_SEM/{date}.csv"
        cdf_path = lambda self, date: f"./data/SOHO/CELIAS_SEM/{date}.cdf"
        root_path = cdf_path("", "")[:-4]
        phy_obs = ["CH1", "CH2", "CH3", "first_order_flux", "central_order_flux"]
        variables = phy_obs
        url = (
            lambda self, date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/soho/celias/sem_15s/{date[:4]}/soho_celias-sem_15s_{date}_v04.cdf"
        )

    class CELIAS_PM(CDAWeb):
        csv_path = lambda self, date: f"./data/SOHO/CELIAS_PM/{date}.csv"
        cdf_path = lambda self, date: f"./data/SOHO/CELIAS_PM/{date}.cdf"
        root_path = cdf_path("", "")[:-4]
        phy_obs = [
            "N_p",
            "V_p",
            "V_He",
            "NS_angle",
            "Vth_p",
        ]
        variables = phy_obs  # variables#change
        url = (
            lambda self, date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/soho/celias/pm_30s/{date[:4]}/soho_celias-pm_30s_{date}_v02.cdf"
        )

    class ERNE(CDAWeb):
        csv_path = lambda self, date: f"./data/SOHO/ERNE/{date}.csv"
        cdf_path = lambda self, date: f"./data/SOHO/ERNE/{date}.cdf"
        root_path = cdf_path("", "")[:-4]
        phy_obs = [
            "C_intensity",
            "N_intensity",
            "O_intensity",
            "Ne_intensity",
            "Mg_intensity",
            "Si_intensity",
            "CNO_intensity",
            "SiAr_intensity",
            "FeCoNi_intensity",
        ]
        variables = [f"{isotop}_{i}" for isotop in phy_obs for i in range(10)]
        url = (
            lambda self, date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/soho/erne/hed_l2-1min/{date[:4]}/soho_erne-hed_l2-1min_{date}_v01.cdf"
        )

    class COSTEP_EPHIN:
        root = "./data/SOHO/COSTEP_EPHIN"
        l3i_path = lambda self, date: f"./data/SOHO/COSTEP_EPHIN/{date}.l3i"
        csv_path = lambda self, date: f"./data/SOHO/COSTEP_EPHIN/{date}.csv"
        url = "https://soho.nascom.nasa.gov/data/EntireMissionBundles/COSTEP_EPHIN_L3_l3i_5min-EntireMission-ByYear.tar.gz"
        name = "COSTEP_EPHIN_L3_l3i_5min-EntireMission-ByYear.tar.gz"
        columns = [
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

        async def downloader_pipeline(
            self, scrap_date: tuple[datetime, datetime], session
        ):
            self.check_if_downloaded(scrap_date)
            if self.new_scrap_date_list is None:
                print("Dataset downloaded")
            else:
                await asyncio.gather(*self.download_url(session))

        def check_if_downloaded(self, scrap_date: tuple[datetime, datetime]):
            self.downloaded = len(glob.glob("./data/SOHO/COSTEP_EPHIN/*.csv")) == 30
            if self.downloaded:
                pass
            else:
                self.new_scrap_date_list = scrap_date

        async def download_url(self, session):
            os.makedirs(self.root)
            async with session.get(self.url, ssl=True) as response:
                if response.status == 200:
                    data = await response.read()
                    await asyncTAR(BytesIO(data), self.get_processing, self.root)
                    await asyncio.gather(*self.get_preprocessing_tasks())

        def get_processing(self, tar_file, root):
            tar_file.extractall(root)

        def get_preprocessing_tasks(self):
            return [
                self.preprocessing(year_path)
                for year_path in glob.glob("./data/SOHO/COSTEP_EPHIN/*.l3i")
            ]

        async def preprocessing(self, year_path):
            async with aiofiles.open(
                year_path[:-3] + "csv", "w"
            ) as csv_file, aiofiles.open(year_path, "r") as l3i:
                csv_file.writelines(",".join(self.columns) + "\n")
                lines = await l3i.readlines()
                for line in lines[3:]:
                    data = line.split()
                    await csv_file.write(
                        ",".join(data[:3] + data[4:6] + data[8:12] + data[20:24]) + "\n"
                    )

            os.remove(year_path)
            df = pd.read_csv(year_path[:-3] + "csv")
            df["datetime"] = pd.to_datetime(
                df[["year", "month", "day", "hour", "minute"]]
            )
            df = df.drop(["year", "month", "day", "hour", "minute"], axis=1)
            df.set_index("datetime", inplace=True, drop=True)
            df.resample("1T").mean().to_csv(year_path[:-3] + "csv")

        def sync_read_csv(
            self, path: str, parse_dates: list[str], index_col: str, date_format: str
        ):
            return pd.read_csv(
                path,
                parse_dates=parse_dates,
                index_col=index_col,
                date_format=date_format,
            )

        async def get_df(self, year):
            df = await asyncio.get_event_loop().run_in_executor(
                None,
                self.sync_read_csv,
                self.csv_path(year),
                ["datetime"],
                "datetime",
                "%Y-%m-%d %H:%M:%S",
            )
            return pd.from_pandas(df)

        async def data_prep(
            self, scrap_date: tuple[datetime, datetime], step_size: timedelta
        ):
            init_date = pd.to_datetime(scrap_date[0])
            last_date = pd.to_datetime(scrap_date[-1])
            years = sorted(
                list(
                    set(
                        [
                            date[:4]
                            for date in datetime_interval(
                                scrap_date[0], scrap_date[-1], timedelta(days=1)
                            )
                        ]
                    )
                )
            )
            df = pd.concat(await asyncio.gather(*[self.get_df(year) for year in years]))
            return df[(df.index >= init_date) & (df.index <= last_date)]
