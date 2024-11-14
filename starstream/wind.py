from typing import Callable, List
from datetime import datetime
from ._base import CDAWeb


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
        def __init__(self, root: str = "./data/WIND/MAG", batch_size: int = 10) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = ["BF1", "BGSE", "BGSM"]
            self.variables: List[str] = ["BF1"] + [
                f"{name}_{i}" for name in self.phy_obs[1:3] for i in range(1, 4)
            ]
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/mfi/mfi_h0/{date[:4]}/wi_h0_mfi_{date}_{WIND_MAG_version(date)}.cdf"
            )

    class SWE_alpha_proton(CDAWeb):
        def __init__(
            self,
            root: str = "./data/WIND/SWE/alpha_proton",
            batch_size: int = 10,
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
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
            self.variables: List[str] = [
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
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/swe/swe_h1/{date[:4]}/wi_h1_swe_{date}_v01.cdf"
            )

    class SWE_electron_angle(CDAWeb):
        def __init__(
            self,
            root: str = "./data/WIND/SWE/electron_angle/",
            batch_size: int = 10,
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "f_pitch_SPA",
                "Ve",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h3_swe_00000000_v01.skt
            self.variables: List[str] = [f"f_pitch_SPA_{i}" for i in range(13)] + [
                f"Ve_{i}" for i in range(13)
            ]
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/swe/swe_h3/{date[:4]}/wi_h3_swe_{date}_v01.cdf"
            )

    class SWE_electron_moments(CDAWeb):
        def __init__(
            self,
            root: str = "./data/WIND/SWE/electron_moments/",
            batch_size: int = 10,
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "N_elec",
                "TcElec",
                "U_eGSE",
                "P_eGSE",
                "W_elec",
                "Te_pal",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.variables: List[str] = (
                self.phy_obs[:2]
                + ["U_eGSE" + f"_{i}" for i in range(1, 4)]
                + ["P_eGSE" + f"_{i}" for i in range(1, 7)]
                + self.phy_obs[4:]
            )
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/swe/swe_h5/{date[:4]}/wi_h5_swe_{date}_v01.cdf"
            )

    class TDP_PM(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/PM/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "P_VELS",
                "P_TEMP",
                "P_DENS",
                "A_VELS",
                "A_TEMP",
                "A_DENS",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.variables: List[str] = [
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
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_pm/{date[:4]}/wi_pm_3dp_{date}_{TDP_PM_version(date)}.cdf"
            )

    class TDP_PLSP(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/PLSP/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX",
                "ENERGY",
                "MOM.P.VTHERMAL",
                "MOM.P.FLUX",
                "MOM.P.PTENS",
                "MOM.A.FLUX",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.variables: List[str] = (
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
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_plsp/{date[:4]}/wi_plsp_3dp_{date}_v02.cdf"
            )

    class TDP_SOSP(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/SOSP/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX",
                "ENERGY",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.variables: List[str] = [
                f"{phy}_{k}" for phy in self.phy_obs[:2] for k in range(1, 10)
            ]
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_sosp/{date[:4]}/wi_sosp_3dp_{date}_v01.cdf"
            )

    class TDP_SOPD(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/SOPD/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX"  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            ]
            self.pitch_angles: List[int] = [15, 35, 57, 80, 102, 123, 145, 165]
            self.energy_bands: List[str] = [
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
            self.variables: List[str] = [
                f"Proton_flux_{deg}_{ener}"
                for deg in self.pitch_angles
                for ener in self.energy_bands
            ]
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_sopd/{date[:4]}/wi_sopd_3dp_{date}_v02.cdf"
            )

    class TDP_ELSP(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/ELSP/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX",
                "ENERGY",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.energy_bands: List[str] = [
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
            self.variables: List[str] = [
                f"electron_{phy}_{ener}"
                for phy in self.phy_obs[:2]
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elsp_3dp_00000000_v01.skt
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_elsp/{date[:4]}/wi_elsp_3dp_{date}_v01.cdf"
            )

    class TDP_ELPD(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/ELPD/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX"
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elpd_3dp_00000000_v01.skt
            self.pitch_angles: List[int] = [15, 35, 57, 80, 102, 123, 145, 165]
            self.energy_bands: List[str] = [
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
            self.root_path: str = self.cdf_path("")[:-4]
            self.variables: List[str] = [
                f"electron_flux_{deg}_{ener}"
                for deg in self.pitch_angles
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elsp_3dp_00000000_v01.skt
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_elpd/{date[:4]}/wi_elpd_3dp_{date}_v02.cdf"
            )

    class TDP_EHSP(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/EHSP/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX",
                "ENERGY",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_ehsp_3dp_00000000_v01.skt
            self.energy_bands: List[str] = [
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
            self.root_path: str = self.cdf_path("")[:-4]
            self.variables: List[str] = [
                f"electron_{phy}_{ener}"
                for phy in self.phy_obs[:2]
                for ener in self.energy_bands
            ]
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_ehsp/{date[:4]}/wi_ehsp_3dp_{date}_v02.cdf"
            )

    class TDP_EHPD(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/EHPD/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX"
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_ehpd_3dp_00000000_v01.skt
            self.pitch_angles: List[int] = [15, 35, 57, 80, 102, 123, 145, 165]
            self.energy_bands: List[str] = [
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
            self.root_path: str = self.cdf_path("")[:-4]
            self.variables: List[str] = [
                f"electron_flux_{deg}_{ener}"
                for deg in self.pitch_angles
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_ehpd_3dp_00000000_v01.skt
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_ehpd/{date[:4]}/wi_ehpd_3dp_{date}_v02.cdf"
            )

    class TDP_SFSP(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/SFSP/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX",
                "ENERGY",
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_h5_swe_00000000_v01.skt
            self.energy_bands: List[str] = [
                "27keV",
                "40keV",
                "86keV",
                "110keV",
                "180keV",
                "310keV",
                "520keV",
            ]
            self.root_path: str = self.cdf_path("")[:-4]
            self.variables: List[str] = [
                f"electron_{phy}_{ener}"
                for phy in self.phy_obs[:2]
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_sfsp_3dp_00000000_v01.skt
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_sfsp/{date[:4]}/wi_sfsp_3dp_{date}_v01.cdf"
            )

    class TDP_SFPD(CDAWeb):
        def __init__(
            self, root: str = "./data/WIND/TDP/SFPD/", batch_size: int = 10
        ) -> None:
            super().__init__(root, batch_size)
            self.phy_obs: List[str] = [
                "FLUX"
            ]  ## metadata: https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elpd_3dp_00000000_v01.skt
            self.pitch_angles: List[int] = [15, 35, 57, 80, 102, 123, 145, 165]
            self.energy_bands: List[str] = [
                "27keV",
                "40keV",
                "86keV",
                "110keV",
                "180keV",
                "310keV",
                "520keV",
            ]
            self.variables: List[str] = [
                f"electron_flux_{deg}_{ener}"
                for deg in self.pitch_angles
                for ener in self.energy_bands
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_elsp_3dp_00000000_v01.skt
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/sp_phys/data/wind/3dp/3dp_sfpd/{date[:4]}/wi_sfpd_3dp_{date}_v02.cdf"
            )

    class SMS(CDAWeb):
        def __init__(self, root: str = "./data/WIND/SMS", batch_size: int = 10) -> None:
            super().__init__(root, batch_size)
            self.angle: List[int] = [53, 0, -53]
            self.phy_obs: List[str] = [
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
            self.variables: List[str] = [
                f"{phy}_{deg}" for phy in self.phy_obs for deg in self.angle
            ]  # https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_l2-3min_sms-stics-vdf-solarwind_00000000_v01.skt
            self.url: Callable[[str], str] = (
                lambda date: f"https://cdaweb.gsfc.nasa.gov/data/wind/sms/l2/stics_cdf/3min_vdf_solarwind/{date[:4]}/wi_l2-3min_sms-stics-vdf-solarwind_{date}_v01.cdf"
            )

        async def _prep_(self, idx: int):
            _ = idx
