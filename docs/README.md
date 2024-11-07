# StarStream
This is a simple, yet comprehensive documentation of all StarStream utilities.

## DataDownloading
This is the base method to download data from the respective satellite repositories, its usage is pretty straightforward and versatile, allowing you to include a variety of satellites per query, and diverse set of date intervals.

```python
from starstream import DSCOVR
from starstream.utils import DataDownloading
from datetime import datetime

if __name__ == '__main__':
    DataDownloading(
        DSCOVR.Faraday(
            root = './data',
            batch_size = 10
        ),
        (datetime(2014, 12, 12), datetime(2014, 12, 30))
    )
```

## Satellite
This is the base class for every satellite implementation, defines the set of commonutilities and tools for data processing, downloading, and date checking. Overall, you'llsee that these objects posess different methods based on the nature of its data:

```python
from starstream.utils import DataDownloading
from datetime import datetime
from starstream import ACE

scrap_date_list: List[Tuple[datetime, datetime]] = ... ## Any date interval within satellite life interval
scrap_date_tuple: Tuple[datetime, datetime] = ... ## Any date interval within satellite life interval
swepam = ACE.SWEPAM()
swepam.get_numpy(scrap_date_list) ## Returns Tuple[NDArray] of the length of scrap_date_list
swepam.get_numpy(scrap_date_tuple) ## Returns Tuple[NDArray] of length 1.
```

NOTE: Spacecrafts' instruments belong to the same class Satellite for ease of development, it will be probably deprecated and reimplemented to fit scientific formalism.

### List of valid spacecrafts, instruments and products:
**StarStream** supports over 12 spacecrafts, and almost 30 instruments ready for scientific usage (some of them should be calibrated with SolarSoft or the respective calibration library).

- Advanced Composite Explorer (ACE)
    - SWEPAM (L2)
    - SWICS (L2)
    - MAG (L2)
    - EPAM (L2)
    - SIS (L2)

- Deep Space Climate Observatory (DSCOVR)
    - Faraday Cup (L1 and L2)
    - Magnetometer (L1 and L2)

- Solar and Heliospheric Observatory (SOHO)
    - CELIAS_PM (L2)
    - CELIAS_SEM (L2)
    - COSTEP_EPHIN (L2)
    - ERNE (L2)
    - LASCO (Not implemented here) see: [CorKit](https://github.com/Jorgedavyd/corkit)

- Dst (World Geomagnetic Data Center, Tokyo)

- Solar Dynamics Observatory (SDO)
    - Atmospheric Imager Assembly (AIA) (L2)
    - Helioseismic and Magnetic Imager (HMI) (L2) (Integrated with AIA for ease of development, will be changed in a future)
    - EUV Variability Experiment (EVE) (L3)

- OMNI

- WIND
    - Magnetic Field Investigator (MAG)
    - Solar Wind Experiment (SWE) Faraday Cup - Ion Data (SWE_alpha_proton)
    - Solar Wind Experiment (SWE) Electron angles (SWE_electron_angle)
    - Solar Wind Experiment (SWE) Electron moments (SWE_electron_moments)
    - 3D Plasma Analyzer Proton Monitor - 3 second Onboard Ion moments (TDP_PM)
    - 3D Plasma Analyzer Proton Monitor - Proton Omnidirectional Fluxes and Moments (TDP_SOPD)
    - 3D Plasma Analyzer Proton Monitor - Proton Omnidirectional Fluxes and Moments (TDP_ELSP)
    - 3D Plasma Analyzer Proton Monitor - Proton Omnidirectional Fluxes and Moments (TDP_ELPD)
    - 3D Plasma Analyzer Proton Monitor - Proton Omnidirectional Fluxes and Moments (TDP_EHSP)
    - 3D Plasma Analyzer Proton Monitor - Proton Omnidirectional Fluxes and Moments (TDP_EHPD)
    - 3D Plasma Analyzer Proton Monitor - Proton Omnidirectional Fluxes and Moments (TDP_SFSP)
    - 3D Plasma Analyzer Proton Monitor - Proton Omnidirectional Fluxes and Moments (TDP_SFPD)
    - SWICS / MASS / STICS : STICS L2 Product => (SMS)

_ Hinode
    - XRT

- GOES
    - GOES16 (Ultraviolet Imager)

- PROBA 2
    - LYRA (L3)

- STEREO
    - A
        - SECCHI
            - EUVI

- SWARM (Alpha, Beta, Charlie)
    - MAG
    - ION
    - FAC
    - EEF
    - EFI
    - MAG_ION

### Methods
```python
date_interval = ...
Satellite.get_numpy(date_interval) ## Valid for all satellites
Satellite.get_torch(date_interval) ## Valid for all satellites
Satellite.get_polars(date_interval) ## Valid for satellites with tabular data structure (Recommended)
Satellite.get_pandas(date_interval) ## Valid for satellites with tabular data structure
```
#### Satellite.get_numpy(*scrap_date*, *resolution*)
Returns data as NumPy array.
scrap_date (Tuple[datetime, datetime] | List[...])
resolution (datetime.timedelta)

#### Satellite.get_torch(*scrap_date*, *resolution*)
Returns data as PyTorch tensor.
scrap_date (Tuple[datetime, datetime] | List[...])
resolution (datetime.timedelta)

#### Satellite.get_polars(*scrap_date*, *resolution*)
Returns data as Polars DataFrame.
scrap_date (Tuple[datetime, datetime] | List[...])
resolution (datetime.timedelta)

#### Satellite.get_pandas(*scrap_date*, *resolution*)
Returns data as Pandas DataFrame.
scrap_date (Tuple[datetime, datetime] | List[...])
resolution (datetime.timedelta)
