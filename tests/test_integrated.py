from datetime import timedelta
from .input_data import scrap_date_list
from starstream import *

def test_fits() -> None:
    DataDownloading(
        [
            Hinode.XRT("fits"),
            PROBA_2.LYRA(timedelta(minutes=5)),
            SDO.AIA_HR(timedelta(minutes=2), "171"),
        ],
        scrap_date_list,
    )


def test_cdf() -> None:
    DataDownloading(
        [SOHO.CELIAS_PM(), ACE.SWEPAM(), Dst(), OMNI(), WIND.SMS()], scrap_date_list
    )


def test_goes() -> None:
    DataDownloading(GOES16("he304", granularity = 0.1), scrap_date_list)
