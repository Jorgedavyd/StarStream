from ._base import CDAWeb
from datetime import datetime

__all__ = ["ACE"]


def SIS_version(date, mode="%Y%m%d"):
    date = datetime.strptime(date, mode)
    v5 = datetime.strptime("20141104", "%Y%m%d")
    v6 = datetime.strptime("20171019", "%Y%m%d")
    if date < v5:
        return "v04"
    elif date < v6:
        return "v05"
    else:
        return "v06"


def EPAM_version(date, mode="%Y%m%d"):
    date = datetime.strptime(date, mode)
    v5 = datetime.strptime("20150101", "%Y%m%d")
    if date < v5:
        return "v04"
    else:
        return "v05"


def MAG_version(date, mode="%Y%m%d"):
    date = datetime.strptime(date, mode)
    v5 = datetime.strptime("20030328", "%Y%m%d")
    v6 = datetime.strptime("20120630", "%Y%m%d")
    v7 = datetime.strptime("20180130", "%Y%m%d")
    if date < v5:
        return "v04"
    elif date < v6:
        return "v05"
    elif date < v7:
        if date == datetime(2017, 11, 1):
            return "v07"
        else:
            return "v06"
    else:
        return "v07"


def SWEPAM_version(date, mode="%Y%m%d"):
    date = datetime.strptime(date, mode)
    v7 = datetime.strptime("20031030", "%Y%m%d")
    v8 = datetime.strptime("20050227", "%Y%m%d")
    v9 = datetime.strptime("20050325", "%Y%m%d")
    v10 = datetime.strptime("20061207", "%Y%m%d")
    v11 = datetime.strptime("20130101", "%Y%m%d")

    if date < v7:
        return "v06"
    elif date < v8:
        return "v07"
    elif date < v9:
        return "v08"
    elif date < v10:
        return "v09"
    elif date < v11:
        return "v10"
    else:
        return "v11"


## https://cdaweb.gsfc.nasa.gov/cgi-bin/eval1.cgi
class ACE:
    class SIS(CDAWeb):
        csv_path = lambda self, date: f"./data/ACE/SIS/{date}.csv"
        cdf_path = lambda self, date: f"./data/ACE/SIS/{date}.cdf"
        root_path = "./data/ACE/SIS/"
        phy_obs = [
            "flux_He",
            "flux_C",
            "flux_N",
            "flux_O",
            "flux_Ne",
            "flux_Na",
            "flux_Mg",
            "flux_Al",
            "flux_Si",
            "flux_S",
            "flux_Ar",
            "flux_Ca",
            "flux_Fe",
            "flux_Ni",
        ]
        variables = [f"{name}_{i}" for name in phy_obs for i in range(8)]
        url = (
            lambda self, date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/ace/sis/level_2_cdaweb/sis_h1/{date[:4]}/ac_h1_sis_{date}_{SIS_version(date)}.cdf"
        )

    class MAG(CDAWeb):
        csv_path = lambda self, date: f"./data/ACE/MAG/{date}.csv"
        cdf_path = lambda self, date: f"./data/ACE/MAG/{date}.cdf"
        root_path = "./data/ACE/MAG/"
        phy_obs = ["Magnitude", "BGSM", "SC_pos_GSM", "dBrms", "BGSEc", "SC_pos_GSE"]
        variables = [
            "Bnorm",
            "BGSM_x",
            "BGSM_y",
            "BGSM_z",
            "SC_GSM_x",
            "SC_GSM_y",
            "SC_GSM_z",
            "dBrms",
            "BGSE_x",
            "BGSE_y",
            "BGSE_z",
            "SC_GSE_x",
            "SC_GSE_y",
            "SC_GSE_z",
        ]
        url = (
            lambda self, date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/ace/mag/level_2_cdaweb/mfi_h0/{date[:4]}/ac_h0_mfi_{date}_{MAG_version(date)}.cdf"
        )

    class SWEPAM(CDAWeb):
        csv_path = lambda self, date: f"./data/ACE/SWEPAM/{date}.csv"
        cdf_path = lambda self, date: f"./data/ACE/SWEPAM/{date}.cdf"
        root_path = "./data/ACE/SWEPAM/"
        phy_obs = [
            "Np",
            "Vp",
            "Tpr",
            "alpha_ratio",
            "V_GSE",
            "V_GSM",
        ]  # variables#change
        variables = phy_obs[:4] + [
            "VGSE_x",
            "VGSE_y",
            "VGSE_z",
            "VGSM_x",
            "VGSM_y",
            "VGSM_z",
        ]
        url = (
            lambda self, date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/ace/swepam/level_2_cdaweb/swe_h0/{date[:4]}/ac_h0_swe_{date}_{SWEPAM_version(date)}.cdf"
        )

    class SWICS(CDAWeb):
        csv_path = lambda self, date: f"./data/ACE/SWICS/{date}.csv"
        cdf_path = lambda self, date: f"./data/ACE/SWICS/{date}.cdf"
        root_path = "./data/ACE/SWICS/"
        phy_obs = ["nH", "vH", "vthH"]  # variables#change
        variables = phy_obs
        url = (
            lambda self, date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/ace/swics/level_2_cdaweb/swi_h6/{date[:4]}/ac_h6_swi_{date}_v03.cdf"
        )

    class EPAM(CDAWeb):
        csv_path = lambda self, date: f"./data/ACE/EPAM/{date}.csv"
        cdf_path = lambda self, date: f"./data/ACE/EPAM/{date}.cdf"
        root_path = "./data/ACE/EPAM/"
        phy_obs = [
            "DE1",
            "DE4",
            "P1p",
            "P3p",
            "P5p",
            "FP6",  # from RTSW
            "P7p",
        ]
        variables = phy_obs  # variables#change
        url = (
            lambda self, date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/ace/epam/level_2_cdaweb/epm_h1/{date[:4]}/ac_h1_epm_{date}_{EPAM_version(date)}.cdf"
        )
