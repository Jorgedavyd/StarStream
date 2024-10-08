![status](https://img.shields.io/badge/status-stable-red.svg)
[![license](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![code-style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![pypi](https://img.shields.io/pypi/v/starstream)](https://pypi.org/project/starstream)
![CI-CD](https://github.com/Jorgedavyd/StarStream/actions/workflows/CI.yml/badge.svg)
![CI-CD](https://github.com/Jorgedavyd/StarStream/actions/workflows/CD.yml/badge.svg)

<p align="center">
  <img src="https://raw.githubusercontent.com/Jorgedavyd/starstream/main/docs/source/logo.png"/ height=450 width=450>
</p>

Asynchronous satellite data downloading for CDAWeb, JSOC, etc.

## Spacecrafts and datasets
<img src="https://upload.wikimedia.org/wikipedia/commons/9/9b/ACE_mission_logo.png" height=200 width=200> <img src="https://www.nesdis.noaa.gov/s3/styles/webp/s3/migrated/DSCOVR-Logo_NOAA_NASA_USAF.png.webp?itok=EGpby_uX" height=200 width=350>
<img src="https://wdc.kugi.kyoto-u.ac.jp/figs/logoh.gif" height=200 width=300><img src="https://upload.wikimedia.org/wikipedia/commons/d/d0/Windlogo.gif" height=200 width=200>
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/8/85/Jaxa_logo.svg/1024px-Jaxa_logo.svg.png" height=150 width=300> <img src='https://upload.wikimedia.org/wikipedia/commons/thumb/e/e5/NASA_logo.svg/1224px-NASA_logo.svg.png' height = 200 width = 250>
<img src="https://3.bp.blogspot.com/-YdNujMhGAkI/WeIyjnfiN8I/AAAAAAABAII/JDV2MvutF_kNJ9ManBMmTM-0X4G6m3KiACLcBGAs/s1600/logo_sdo.gif" height = 250 width = 250> <img src="http://esdcdoi.esac.esa.int/doi/html/img/soho_logo.png" height = 250 width = 250>
<img src="https://earth.esa.int/eogateway/documents/20142/0/swarm.png/656b6a23-f035-f7e3-78c6-02a47d1a4b6e?t=1608141199732" height = 100 width = 300><img src="http://esdcdoi.esac.esa.int/doi/html/img/Proba2_logo2020.svg" height = 200 width = 200>

## External dependencies
1. **CDF**: [installation](https://spacepy.github.io/install_linux.html)

## Example
```python
from starstream import ACE, DSCOVR
from starstream.utils import DataDownloading
from datetime import datetime

if __name__ == '__main__':
    DataDownloading(
        [
            ACE(),
            DSCOVR()
        ],
        (datetime(2014, 12, 12), datetime(2014, 12, 30))
    )

```
## Citation

```
@misc{starstream,
  author = {Jorge Enciso},
  title = {StarStream: An Open-Source Asynchronous Data Downloader},
  howpublished = {\url{https://github.com/Jorgedavyd/starstream}},
  year = {2024}
}
```
