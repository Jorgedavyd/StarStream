from datetime import datetime
from ..starstream.utils import DataDownloading
from ..starstream import *
import asyncio
import os
from datetime import timedelta


# Defining default values for tests
scrap_date_list = [
    (datetime(2017, 10, 3), datetime(2017, 10, 15)),
    (datetime(2018, 10, 3), datetime(2018, 3, 15))
    ]

async def ncei() -> None:
    await DataDownloading(
        [
            DSCOVR()
        ],
        scrap_date_list
    )

async def fits() -> None:
    await DataDownloading(
        [
            Hinode.XRT('fits'), PROBA_2.LYRA(timedelta(minutes = 5)), SDO.AIA_HR(timedelta(minutes = 2), '0171')
        ],
        scrap_date_list
    )

async def cdf() -> None:
    await DataDownloading(
        [
            SOHO.CELIAS_PM(), ACE.SWEPAM(), Dst(), OMNI(), WIND.SMS()
        ]
    )

def test() -> None:
    asyncio.run(ncei())
    asyncio.run(fits())
    asyncio.run(cdf())
    os.remove('./data')


