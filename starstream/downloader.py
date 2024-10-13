from typing import List, Union, Tuple
from datetime import datetime
from ._base import Satellite
import asyncio
import aiohttp


def DataDownloading(
    sat_objs: Union[List, Satellite],
    scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]],
) -> None:
    if not isinstance(sat_objs, list):
        sat_objs = [sat_objs]
    asyncio.run(downloader(sat_objs, scrap_date))


async def downloader(
    sat_objs: List[Satellite],
    scrap_date: Union[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]],
) -> None:
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[satellite.fetch(scrap_date, session) for satellite in sat_objs]
        )
