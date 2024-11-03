from datetime import timedelta
from .input_data import scrap_date_list
from starstream import *


def test_fits() -> None:
    DataDownloading(
        [
            Hinode.XRT(batch_size=15),
            PROBA_2.LYRA(),
            SDO.AIA_HR(
                resolution=timedelta(minutes=2),
                wavelength=171
            ),
        ],
        scrap_date_list,
    )


def test_cdf() -> None:
    DataDownloading(
        [SOHO.CELIAS_PM(), ACE.SWEPAM(), Dst(), OMNI(), WIND.SMS()], scrap_date_list
    )


def test_goes() -> None:
    DataDownloading(GOES16(instrument="he304", granularity=0.1), scrap_date_list)
