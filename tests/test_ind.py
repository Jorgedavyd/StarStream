from .input_data import scrap_date_list
from typing import Tuple
from starstream import *


def runtime(object) -> None:
    obj = object()
    DataDownloading(obj, scrap_date_list)
    scrap_date: Tuple[datetime, datetime] = scrap_date_list[0]
    try:
        obj.data_prep(scrap_date, timedelta(hours=1))
    except TypeError:
        obj.data_prep(scrap_date)


def test_dscovr() -> None:
    runtime(DSCOVR)


def test_ace_mag() -> None:
    runtime(ACE.MAG)


def test_ace_swepam() -> None:
    runtime(ACE.SWEPAM)


def test_ace_swics() -> None:
    runtime(ACE.SWICS)


def test_ace_sis() -> None:
    runtime(ACE.SIS)


def test_dst() -> None:
    runtime(Dst)


def test_omni() -> None:
    runtime(OMNI)


def test_goes() -> None:
    def object():
        return GOES16("fe094", granularity=1 / 60, batch_size=15)

    runtime(object)


def test_hinode() -> None:
    def object():
        return Hinode.XRT(batch_size=15)

    runtime(object)


def test_proba() -> None:
    def object():
        return PROBA_2.LYRA(batch_size=15)

    runtime(object)


def test_aia_lr() -> None:
    def object():
        return SDO.AIA_LR(wavelength="0131")

    runtime(object)


def test_aia_hr() -> None:
    def object():
        return SDO.AIA_HR(step_size=timedelta(minutes=5), wavelength="94")

    runtime(object)


def test_secchi() -> None:
    def object():
        return STEREO_A.SECCHI.EUVI("171")

    runtime(object)


def test_celias_sem() -> None:
    runtime(SOHO.CELIAS_SEM)


def test_celias_pm() -> None:
    runtime(SOHO.CELIAS_PM)


def test_soho_erne() -> None:
    runtime(SOHO.ERNE)


def test_soho_costep_ephin() -> None:
    runtime(SOHO.COSTEP_EPHIN)


def test_wind() -> None:
    runtime(WIND.MAG)
