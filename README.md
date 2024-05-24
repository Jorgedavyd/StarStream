![status](https://img.shields.io/badge/status-beta-red.svg)
[![license](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![code-style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# StarStream

<p align="center">
  <img src="https://raw.githubusercontent.com/Jorgedavyd/SatFetch/main/docs/source/logo.png"/>
</p>

![image]()

Asynchronous satellite data downloading for CDAWeb, JSOC, etc.

# Spacecrafts and datasets
<img src="https://upload.wikimedia.org/wikipedia/commons/9/9b/ACE_mission_logo.png" height=200 width=200> <img src="https://www.nesdis.noaa.gov/s3/styles/webp/s3/migrated/DSCOVR-Logo_NOAA_NASA_USAF.png.webp?itok=EGpby_uX" height=200 width=350>
<img src="https://wdc.kugi.kyoto-u.ac.jp/figs/logoh.gif" height=200 width=300><img src="https://upload.wikimedia.org/wikipedia/commons/d/d0/Windlogo.gif" height=200 width=200>
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/8/85/Jaxa_logo.svg/1024px-Jaxa_logo.svg.png" height=200 width=400> <img src='https://upload.wikimedia.org/wikipedia/commons/thumb/e/e5/NASA_logo.svg/1224px-NASA_logo.svg.png' height = 200 width = 250>


## Example
```python 
from starstream.sat import ACE, DSCOVR
from starstream.utils import DataDownloading
from datetime import datetime

downloader = DataDownloading()

if __name__ == '__main__':
    downloader(
        [
            ACE,
            DSCOVR
        ],
        [
            (datetime(2014, 12, 12), datetime(2014, 12, 30))
        ]
    )

```
