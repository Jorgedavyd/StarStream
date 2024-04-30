from utils import datetime_interval, timedelta_to_freq
from datetime import datetime, timedelta
from viresclient import SwarmRequest
import pandas as pd
import asyncio
import time
import cudf
import os
import joblib


def separate_intervals(date_list):
    intervals = []
    start_date = None
    end_date = None

    for date in date_list:
        if start_date is None:
            start_date = date
            end_date = date
        elif date == end_date + timedelta(days=1):
            end_date = date
        else:
            intervals.append((start_date, end_date))
            start_date = date
            end_date = date
    if start_date is not None:
        intervals.append((start_date, end_date))
    return intervals


def mag_ion_transform(df: cudf.DataFrame):
    "Bx", "By", "Bz", "Ehx", "Ehy", "Ehz", "Evx", "Evy", "Evz", "Vicrx", "Vicry", "Vicrz", "Vixv", "Vixh", "Viy", "Viz"
    nec_transform = joblib.load("2nec.pkl")
    df[["Bx", "By", "Bz"]] = nec_transform(df[["Bx", "By", "Bz"]].values)
    df[["Ehx", "Ehy", "Ehz"]] = nec_transform(df[["Ehx", "Ehy", "Ehz"]].values)
    df[["Vicrx", "Vicry", "Vicrz"]] = nec_transform(
        df[["Vicrx", "Vicry", "Vicrz"]].values
    )
    df[["Vixh", "Viy", "Viz"]] = nec_transform(df[["Vixh", "Viy", "Viz"]].values)
    return df


class SwarmUtils:
    spacecrafts = ["A", "B", "C"]
    base_url = "https://swarm-diss.eo.esa.int/"

    def check_tasks(self, scrap_date: tuple[datetime, datetime]):
        self.new_scrap_date_list = [
            datetime.strptime(date, "%Y%m%d")
            for date in datetime_interval(
                scrap_date[0], scrap_date[-1], timedelta(days=1)
            )
            if not os.path.exists(self.csv_path(date, "A"))
        ]

    def data_prep(self, scrap_date: tuple[datetime, datetime], step_size: timedelta):
        scrap_date = datetime_interval(scrap_date[0], scrap_date[-1], timedelta(days=1))
        dfs = [
            cudf.read_csv(self.csv_path(date, X))
            for date in scrap_date
            for X in self.spacecrafts
        ]
        if isinstance(self, SWARM.MAG_ION):
            # perform transformation
            dfs = [mag_ion_transform(df) for df in dfs]
        return cudf.concat(dfs).resample(timedelta_to_freq(step_size)).mean()

    def downloader_pipeline(self, scrap_date):
        self.check_tasks(scrap_date)
        if self.new_scrap_date_list == []:
            print("Datasets already downloaded")
        else:
            self.download_url("A", scrap_date)
            self.download_url("B", scrap_date)
            self.download_url("C", scrap_date)

    def download_url(self, X, date_interval):
        request = SwarmRequest()
        request.set_collection(self.collection(X))
        request.set_products(measurements=self.measurements, sampling_step="PT1M")
        data = request.get_between(
            date_interval[0].replace(hour=0, minute=0, second=0),
            date_interval[-1].replace(hour=23, minute=59, second=59),
        )
        df = data.as_dataframe(expand=True)
        for date in self.new_scrap_date_list:
            df[df.index.date == pd.Timestamp(date).date()].to_csv(
                self.csv_path(datetime.strftime(date, "%Y%m%d"), X)
            )

    def create_folders(self):
        for x in self.spacecrafts:
            os.makedirs(self.root_path(x), exist_ok=True)


class SWARM:
    class MAG(SwarmUtils):
        collection = lambda self, X: f"SW_OPER_MAG{X}_LR_1B"
        csv_path = lambda self, date, X: f"./data/SWARM/MAG/Sat_{X}/{date}.csv"
        root_path = lambda self, X: f"./data/SWARM/MAG/Sat_{X}"
        measurements = ["F", "B_NEC", "dF_Sun", "dB_Sun"]

        def __init__(self):
            self.create_folders()

    class EFI(SwarmUtils):
        collection = lambda self, X: f"SW_OPER_EFI{X}_LP_1B"
        csv_path = lambda self, date, X: f"./data/SWARM/EFI/Sat_{X}/{date}.csv"
        root_path = lambda self, X: f"./data/SWARM/EFI/Sat_{X}"
        measurements = ["Ne", "Te", "U_orbit", "Vs"]

        def __init__(self):
            self.create_folders()

    class MAG_ION(SwarmUtils):
        collection = lambda self, X: f"SW_EXPT_EFI{X}_TCT16"
        csv_path = lambda self, date, X: f"./data/SWARM/MAG_ION/Sat_{X}/{date}.csv"
        root_path = lambda self, X: f"./data/SWARM/MAG_ION/Sat_{X}/"
        measurements = [
            "Bx",
            "By",
            "Bz",
            "Ehx",
            "Ehy",
            "Ehz",
            "Evx",
            "Evy",
            "Evz",
            "Vicrx",
            "Vicry",
            "Vicrz",
            "Vixv",
            "Vixh",
            "Viy",
            "Viz",
        ]

        def __init__(self):
            self.create_folders()

    class ION(SwarmUtils):
        collection = lambda self, X: f"SW_PREL_EFI{X}IDM_2_"
        csv_path = lambda self, date, X: f"./data/SWARM/ION/Sat_{X}/{date}.csv"
        root_path = lambda self, X: f"./data/SWARM/ION/Sat_{X}/"
        measurements = ["M_i_eff", "N_i", "V_i"]

        def __init__(self):
            self.create_folders()

    class FAC(SwarmUtils):
        collection = lambda self, X: f"SW_OPER_FAC{X}TMS_2F"
        csv_path = lambda self, date, X: f"./data/SWARM/FAC/Sat_{X}/{date}.csv"
        root_path = lambda self, X: f"./data/SWARM/FAC/Sat_{X}/"
        measurements = ["IRC", "FAC"]

        def __init__(self):
            self.create_folders()

    class EEF(SwarmUtils):
        collection = lambda self, X: f"SW_OPER_EEF{X}TMS_2F"
        csv_path = lambda self, date, X: f"./data/SWARM/EEF/Sat_{X}/{date}.csv"
        root_path = lambda self, X: f"./data/SWARM/EEF/Sat_{X}/"
        measurements = ["EEF"]

        def __init__(self):
            self.create_folders()
