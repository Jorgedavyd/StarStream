from ._base import CDAWeb
from datetime import datetime
import asyncio


def WIND_MAG_version(date, mode="%Y%m%d"):
    date = datetime.strptime(date, mode)
    v4 = datetime.strptime("20230101", "%Y%m%d")
    v3 = datetime.strptime("20231121", "%Y%m%d")
    if date < v4:
        return "v05"
    elif date < v3:
        return "v04"
    else:
        return "v03"


def TDP_PM_version(date, mode="%Y%m%d"):
    date = datetime.strptime(date, mode)
    v4 = datetime.strptime("20110114", "%Y%m%d")
    v5 = datetime.strptime("20111230", "%Y%m%d")
    if date < v4:
        return "v03"
    elif date < v5:
        return "v04"
    else:
        return "v05"


class WIND:
    class MAG(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/MAG/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/MAG/{date}.cdf"
            self.phy_obs = ["BF1", "BGSE", "BGSM"]
            self.variables = ["BF1"] + [
                f"{name}_{i}" for name in self.phy_obs[1:3] for i in range(1, 4)
            ]
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/mfi/mfi_h0/{date[:4]}/wi_h0_mfi_{date}_{WIND_MAG_version(date)}.cdf"
            )
            self.root_path = self.cdf_path("")[:-4]

    class SWE_alpha_proton(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/SWE/alpha_proton/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/SWE/alpha_proton/{date}.cdf"
            self.phy_obs = [
                "Proton_V_nonlin",
                "Proton_VX_nonlin",
                "Proton_VY_nonlin",
                "Proton_VZ_nonlin",
                "Proton_Np_nonlin",
                "Alpha_V_nonlin",
                "Alpha_VX_nonlin",
                "Alpha_VY_nonlin",
                "Alpha_VZ_nonlin",
                "Alpha_Na_nonlin",
                "xgse",
                "ygse",
                "zgse",
            ]
            self.variables = [
                "Vp",
                "Vpx",
                "Vpy",
                "Vpz",
                "Np",
                "Va",
                "Vax",
                "Vay",
                "Vaz",
                "Na",
                "xgse",
                "ygse",
                "zgse",
            ]
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/swe/swe_h1/{date[:4]}/wi_h1_swe_{date}_v01.cdf"
            )
            self.root_path = self.cdf_path("")[:-4]

    class SWE_electron_angle(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/SWE/electron_angle/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/SWE/electron_angle/{date}.cdf"
            self.phy_obs = [
                "f_pitch_SPA",
                "Ve",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h3_swe_00000000_v01.skt
            self.variables = [f"f_pitch_SPA_{i}" for i in range(13)] + [
                f"Ve_{i}" for i in range(13)
            ]
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/swe/swe_h3/{date[:4]}/wi_h3_swe_{date}_v01.cdf"
            )
            self.root_path = self.cdf_path("")[:-4]

    class SWE_electron_moments(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/SWE/electron_moments/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/SWE/electron_moments/{date}.cdf"
            self.phy_obs = [
                "N_elec",
                "TcElec",
                "U_eGSE",
                "P_eGSE",
                "W_elec",
                "Te_pal",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.variables = (
                self.phy_obs[:2]
                + ["U_eGSE" + f"_{i}" for i in range(1, 4)]
                + ["P_eGSE" + f"_{i}" for i in range(1, 7)]
                + self.phy_obs[4:]
            )
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/swe/swe_h5/{date[:4]}/wi_h5_swe_{date}_v01.cdf"
            )
            self.root_path = self.cdf_path("")[:-4]

    class TDP_PM(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/PM/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/PM/{date}.cdf"
            self.phy_obs = [
                "P_VELS",
                "P_TEMP",
                "P_DENS",
                "A_VELS",
                "A_TEMP",
                "A_DENS",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.variables = [
                "Vpx",
                "Vpy",
                "Vpz",
                "Tp",
                "Np",
                "Vax",
                "Vay",
                "Vaz",
                "Ta",
                "Na",
            ]  # GSE
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_pm/{date[:4]}/wi_pm_3dp_{date}_{TDP_PM_version(date)}.cdf"
            )
            self.root_path = self.cdf_path("")[:-4]

    class TDP_PLSP(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/PLSP/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/PLSP/{date}.cdf"
            self.phy_obs = [
                "FLUX",
                "ENERGY",
                "MOM.P.VTHERMAL",
                "MOM.P.FLUX",
                "MOM.P.PTENS",
                "MOM.A.FLUX",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.variables = (
                [f"FLUX_{i}" for i in range(1, 16)]
                + [f"ENERGY_{i}" for i in range(1, 16)]
                + [
                    "Vpt",
                    "Jpx",
                    "Jpy",
                    "Jpz",
                    "Pp_XX",
                    "Pp_YY",
                    "Pp_ZZ",
                    "Pp_XY",
                    "Pp_XZ",
                    "Pp_YZ",
                    "Jax",
                    "Jay",
                    "Jaz",
                ]
            )
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_plsp/{date[:4]}/wi_plsp_3dp_{date}_v02.cdf"
            )
            self.root_path = self.cdf_path("")[:-4]

    class TDP_SOSP(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/SOSP/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/SOSP/{date}.cdf"
            self.phy_obs = [
                "FLUX",
                "ENERGY",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.variables = [
                f"{phy}_{k}" for phy in self.phy_obs[:2] for k in range(1, 10)
            ]
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_sosp/{date[:4]}/wi_sosp_3dp_{date}_v01.cdf"
            )
            self.root_path = self.cdf_path("")[:-4]

    class TDP_SOPD(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/SOPD/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/SOPD/{date}.cdf"
            self.phy_obs = "FLUX"  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.pitch_angles = [15, 35, 57, 80, 102, 123, 145, 165]
            self.energy_bands = [
                "70keV",
                "130keV",
                "210keV",
                "330keV",
                "550keV",
                "1000keV",
                "2100keV",
                "4400keV",
                "6800keV",
            ]
            self.root_path = self.cdf_path("")[:-4]
            self.variables = [
                f"Proton_flux_{deg}_{ener}"
                for deg in self.pitch_angles
                for ener in self.energy_bands
            ]
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_sopd/{date[:4]}/wi_sopd_3dp_{date}_v02.cdf"
            )

    class TDP_ELSP(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/ELSP/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/ELSP/{date}.cdf"
            self.phy_obs = [
                "FLUX",
                "ENERGY",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.energy_bands = [
                "1113eV",
                "669.2eV",
                "426.8eV",
                "264.8eV",
                "165eV",
                "103.3eV",
                "65.25eV",
                "41.8eV",
                "27.25eV",
                "18.3eV",
                "12.8eV",
                "9.4eV",
                "7.25eV",
                "5.9eV",
                "5.2eV",
            ]
            self.root_path = self.cdf_path("")[:-4]
            self.variables = [
                f"electron_{phy}_{ener}"
                for phy in self.phy_obs[:2]
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elsp_3dp_00000000_v01.skt
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_elsp/{date[:4]}/wi_elsp_3dp_{date}_v01.cdf"
            )

    class TDP_ELPD(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/ELPD/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/ELPD/{date}.cdf"
            self.phy_obs = [
                "FLUX"
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elpd_3dp_00000000_v01.skt
            self.pitch_angles = [15, 35, 57, 80, 102, 123, 145, 165]
            self.energy_bands = [
                "1150eV",
                "790eV",
                "540eV",
                "370eV",
                "255eV",
                "175eV",
                "121eV",
                "84eV",
                "58eV",
                "41eV",
                "29eV",
                "20.5eV",
                "15eV",
                "11.3eV",
                "8.6eV",
            ]
            self.root_path = self.cdf_path("")[:-4]
            self.variables = [
                f"electron_flux_{deg}_{ener}"
                for deg in self.pitch_angles
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elsp_3dp_00000000_v01.skt
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_elpd/{date[:4]}/wi_elpd_3dp_{date}_v02.cdf"
            )

    class TDP_EHSP(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/EHSP/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/EHSP/{date}.cdf"
            self.phy_obs = [
                "FLUX",
                "ENERGY",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_ehsp_3dp_00000000_v01.skt
            self.energy_bands = [
                "27660eV",
                "18940eV",
                "12970eV",
                "8875eV",
                "6076eV",
                "4161eV",
                "2849eV",
                "1952eV",
                "1339eV",
                "920.3eV",
                "634.4eV",
                "432.7eV",
                "292.0eV",
                "200.1eV",
                "136.8eV",
            ]
            self.root_path = self.cdf_path("")[:-4]
            self.variables = [
                f"electron_{phy}_{ener}"
                for phy in self.phy_obs[:2]
                for ener in self.energy_bands
            ]
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_ehsp/{date[:4]}/wi_ehsp_3dp_{date}_v02.cdf"
            )

    class TDP_EHPD(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/EHPD/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/EHPD/{date}.cdf"
            self.phy_obs = [
                "FLUX"
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_ehpd_3dp_00000000_v01.skt
            self.pitch_angles = [15, 35, 57, 80, 102, 123, 145, 165]
            self.energy_bands = [
                "27660eV",
                "18940eV",
                "12970eV",
                "8875eV",
                "6076eV",
                "4161eV",
                "2849eV",
                "1952eV",
                "1339eV",
                "920.3eV",
                "634.4eV",
                "432.7eV",
                "292.0eV",
                "200.1eV",
                "136.8eV",
            ]
            self.root_path = self.cdf_path("")[:-4]
            self.variables = [
                f"electron_flux_{deg}_{ener}"
                for deg in self.pitch_angles
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_ehpd_3dp_00000000_v01.skt
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_ehpd/{date[:4]}/wi_ehpd_3dp_{date}_v02.cdf"
            )

    class TDP_SFSP(CDAWeb):
        def __init__(self) -> None:

            self.csv_path = lambda date: f"./data/WIND/TDP/SFSP/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/SFSP/{date}.cdf"
            self.phy_obs = [
                "FLUX",
                "ENERGY",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.energy_bands = [
                "27keV",
                "40keV",
                "86keV",
                "110keV",
                "180keV",
                "310keV",
                "520keV",
            ]
            self.root_path = self.cdf_path("")[:-4]
            self.variables = [
                f"electron_{phy}_{ener}"
                for phy in self.phy_obs[:2]
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_sfsp_3dp_00000000_v01.skt
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_sfsp/{date[:4]}/wi_sfsp_3dp_{date}_v01.cdf"
            )

    class TDP_SFPD(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/SFPD/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/SFPD/{date}.cdf"
            self.phy_obs = [
                "FLUX"
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elpd_3dp_00000000_v01.skt
            self.pitch_angles = [15, 35, 57, 80, 102, 123, 145, 165]
            self.energy_bands = [
                "27keV",
                "40keV",
                "86keV",
                "110keV",
                "180keV",
                "310keV",
                "520keV",
            ]
            self.root_path = self.cdf_path("")[:-4]
            self.variables = [
                f"electron_flux_{deg}_{ener}"
                for deg in self.pitch_angles
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elsp_3dp_00000000_v01.skt
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_sfpd/{date[:4]}/wi_sfpd_3dp_{date}_v02.cdf"
            )

    class SMS(CDAWeb):
        def __init__(self) -> None:
            self.csv_path = lambda date: f"./data/WIND/TDP/SFPD/{date}.csv"
            self.cdf_path = lambda date: f"./data/WIND/TDP/SFPD/{date}.cdf"
            self.root_path = self.cdf_path("")[:-4]
            self.angle = [53, 0, -53]
            self.phy_obs = [
                "counts_tc_he2plus",
                "counts_tc_heplus",
                "counts_tc_hplus",
                "counts_tc_o6plus",
                "counts_tc_oplus",
                "counts_tc_c5plus",
                "counts_tc_fe10plus",
                "dJ_tc_he2plus",
                "dJ_tc_heplus",
                "dJ_tc_hplus",
                "dJ_tc_o6plus",
                "dJ_tc_oplus",
                "dJ_tc_c5plus",
                "dJ_tc_fe10plus",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_l2-3min_sms-stics-vdf-solarwind_00000000_v01.skt
            self.variables = [
                f"{phy}_{deg}" for phy in self.phy_obs for deg in self.angle
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_l2-3min_sms-stics-vdf-solarwind_00000000_v01.skt
            self.url = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/data/wind/sms/l2/stics_cdf/3min_vdf_solarwind/{date[:4]}/wi_l2-3min_sms-stics-vdf-solarwind_{date}_v01.cdf"
            )

        async def downloader_pipeline(self, scrap_date, session):
            self.check_tasks(scrap_date)
            tasks = self.get_download_tasks(session)

            for i in range(0, len(self.new_scrap_date_list), self.batch_size):
                await asyncio.gather(*tasks[i : i + self.batch_size])
