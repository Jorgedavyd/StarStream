import cudf


class MHD:
    """
    Feature Engineering for ACE and DSCOVR based on magnetohydrodinamics
    """

    @staticmethod
    def scaled_flow_pressure(Np: cudf.Series, Vp: cudf.Series):
        return 2e-6 * Np * Vp**2

    @staticmethod
    def scaled_plasma_beta(T: cudf.Series, Np: cudf.Series, B: cudf.Series):
        T_eV = T * 4.16e-05
        beta = ((T_eV + 5.34) * Np) / (B**2)
        return beta

    @staticmethod
    def scaled_alfven_velocity(B, Np: cudf.Series):
        return 20 * (B / Np.sqrt())

    @staticmethod
    def scaled_alfven_mach_number(Vp: cudf.Series, B: cudf.Series, Np: cudf.Series):
        return Vp / MHD.scaled_alfven_velocity(B, Np)

    @staticmethod
    def scaled_sound_speed(Tp: cudf.Series):
        return 0.12 * (Tp + 1.28e5).sqrt()

    @staticmethod
    def scaled_magnetosonic_speed(B: cudf.Series, Np: cudf.Series, Tp: cudf.Series):
        V_A = MHD.scaled_alfven_velocity(B, Np)
        C_s = MHD.scaled_sound_speed(Tp)
        return (C_s**2 + V_A**2).sqrt()

    @staticmethod
    def scaled_magnetosonic_mach_number(Vp, B, Np, Tp):
        V_MS = MHD.scaled_magnetosonic_speed(B, Np, Tp)
        return Vp / V_MS

    @staticmethod
    def apply_features(
        df: cudf.DataFrame,
        magnetic_field_norm: str,
        proton_density: str,
        proton_bulk_velocity: str,
        proton_temperature: str,
    ):
        df["flow_pressure"] = MHD.scaled_flow_pressure(
            df[proton_density], df[proton_bulk_velocity]
        )
        df["beta"] = MHD.scaled_plasma_beta(
            df[proton_temperature], df[proton_density], df[magnetic_field_norm]
        )
        df["alfven_mach_number"] = MHD.scaled_alfven_mach_number(
            df[proton_bulk_velocity], df[magnetic_field_norm], df[proton_density]
        )
        df["magnetosonic_mach_number"] = MHD.scaled_magnetosonic_mach_number(
            df[proton_bulk_velocity],
            df[magnetic_field_norm],
            df[proton_density],
            df[proton_temperature],
        )
        df["alfven_velocity"] = MHD.scaled_alfven_velocity(
            df[magnetic_field_norm], df[proton_density]
        )
        df["magnetosonic_speed"] = MHD.scaled_magnetosonic_speed(
            df[magnetic_field_norm], df[proton_density], df[proton_temperature]
        )
        return df
