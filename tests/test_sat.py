from datetime import datetime
from starstream import * 
import asyncio
import os
from datetime import timedelta

# Defining default values for tests
scrap_date_list = [
    (datetime(2017, 10, 3), datetime(2017, 10, 15)),
    (datetime(2018, 10, 3), datetime(2018, 3, 15))
    ]

def test_ncei() -> None:
    asyncio.run(DataDownloading(
        [
            DSCOVR()
        ],
        scrap_date_list
    ))

def test_fits() -> None:
    asyncio.run(DataDownloading(
        [
            Hinode.XRT('fits'), PROBA_2.LYRA(timedelta(minutes = 5)), SDO.AIA_HR(timedelta(minutes = 2), '171')
        ],
        scrap_date_list
    ))

def test_cdf() -> None:
    asyncio.run(DataDownloading(
        [
            SOHO.CELIAS_PM(), ACE.SWEPAM(), Dst(), OMNI(), WIND.SMS()
        ],
        scrap_date_list
    ))


