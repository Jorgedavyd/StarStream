from typing import List, Tuple
from starstream._base import CSV
from starstream._utils import StarDate, StarInterval
from datetime import datetime, timedelta
from viresclient import SwarmRequest
import pandas as pd
import os
import joblib

__all__ = ["SWARM"]


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


def mag_ion_transform(df: pd.DataFrame):
    nec_transform = joblib.load("2nec.pkl")
    df[["Bx", "By", "Bz"]] = nec_transform(df[["Bx", "By", "Bz"]].values)
    df[["Ehx", "Ehy", "Ehz"]] = nec_transform(df[["Ehx", "Ehy", "Ehz"]].values)
    df[["Vicrx", "Vicry", "Vicrz"]] = nec_transform(
        df[["Vicrx", "Vicry", "Vicrz"]].values
    )
    df[["Vixh", "Viy", "Viz"]] = nec_transform(df[["Vixh", "Viy", "Viz"]].values)
    return df


class SwarmUtils(CSV):
    spacecrafts = ["A", "B", "C"]
    base_url = "https://swarm-diss.eo.esa.int/"
    new_scrap_date_list: List[StarDate] = []

    def check_tasks(self, scrap_date: List[Tuple[datetime, datetime]]):
        new_scrap_date: StarInterval = StarInterval(scrap_date)

        for date in new_scrap_date:
            if not os.path.exists(self.csv_path(date.str())):
                self.new_scrap_date_list.append(date)

    def downloader_pipeline(self, scrap_date, session):
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

        def __init__(self):
            self.collection = lambda X: f"SW_OPER_MAG{X}_LR_1B"
            self.csv_path = lambda date, X: f"./data/SWARM/MAG/Sat_{X}/{date}.csv"
            self.root_path = lambda X: f"./data/SWARM/MAG/Sat_{X}"
            self.measurements = ["F", "B_NEC", "dF_Sun", "dB_Sun"]
            self.create_folders()

    class EFI(SwarmUtils):

        def __init__(self):
            self.collection = lambda X: f"SW_OPER_EFI{X}_LP_1B"
            self.csv_path = lambda date, X: f"./data/SWARM/EFI/Sat_{X}/{date}.csv"
            self.root_path = lambda X: f"./data/SWARM/EFI/Sat_{X}"
            self.measurements = ["Ne", "Te", "U_orbit", "Vs"]
            self.create_folders()

    class MAG_ION(SwarmUtils):

        def __init__(self):
            self.collection = lambda X: f"SW_EXPT_EFI{X}_TCT16"
            self.csv_path = lambda date, X: f"./data/SWARM/MAG_ION/Sat_{X}/{date}.csv"
            self.root_path = lambda X: f"./data/SWARM/MAG_ION/Sat_{X}/"
            self.measurements = [
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
            self.create_folders()

    class ION(SwarmUtils):

        def __init__(self):
            self.collection = lambda X: f"SW_PREL_EFI{X}IDM_2_"
            self.csv_path = lambda date, X: f"./data/SWARM/ION/Sat_{X}/{date}.csv"
            self.root_path = lambda X: f"./data/SWARM/ION/Sat_{X}/"
            self.measurements = ["M_i_eff", "N_i", "V_i"]
            self.create_folders()

    class FAC(SwarmUtils):

        def __init__(self):
            self.collection = lambda X: f"SW_OPER_FAC{X}TMS_2F"
            self.csv_path = lambda date, X: f"./data/SWARM/FAC/Sat_{X}/{date}.csv"
            self.root_path = lambda X: f"./data/SWARM/FAC/Sat_{X}/"
            self.measurements = ["IRC", "FAC"]
            self.create_folders()

    class EEF(SwarmUtils):

        def __init__(self):
            self.collection = lambda X: f"SW_OPER_EEF{X}TMS_2F"
            self.csv_path = lambda date, X: f"./data/SWARM/EEF/Sat_{X}/{date}.csv"
            self.root_path = lambda X: f"./data/SWARM/EEF/Sat_{X}/"
            self.measurements = ["EEF"]
            self.create_folders()
