![status](https://img.shields.io/badge/status-beta-red.svg)
[![license](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![code-style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# SatFetch

![image](https://github.com/Jorgedavyd/SatFetch/raw/docs/source/logo.png)

Asynchronous satellite data downloading for CDAWeb, JSOC, etc.

## Example
```python 
from downloader import ACE, DSCOVR
from downloader.utils import DataDownloading
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