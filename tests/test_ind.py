from .input_data import scrap_date_list
from starstream import *


def test_dscovr() -> None:
    DataDownloading(DSCOVR(), scrap_date_list)
