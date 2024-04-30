from utils import datetime_interval, timedelta_to_freq, asyncGZ
from datetime import timedelta, datetime
from ..preprocessing.MHD import MHD
from io import BytesIO
import xarray as xr
import asyncio
import cudf
import os

class DSCOVR(MHD):
	fc1_root = lambda self, date: f'./data/DSCOVR_L1/faraday/fc1_{date}.csv'
	mg1_root = lambda self, date: f'./data/DSCOVR_L1/magnetometer/mg1_{date}.csv'
	f1m_root = lambda self, date: f'./data/DSCOVR_L2/faraday/f1m_{date}.csv'
	m1m_root = lambda self, date: f'./data/DSCOVR_L2/magnetometer/m1m_{date}.csv'
	mg_var = ['bx_gsm', 'by_gsm', 'bz_gsm', 'bt']
	fc_var = ['proton_density', 'proton_speed', 'proton_temperature']
	roots = [fc1_root, mg1_root, f1m_root, m1m_root]
	var_meta = {
		'fc1': [fc1_root, fc_var],
		'mg1': [mg1_root, mg_var],
		'f1m': [f1m_root, fc_var],
		'm1m': [m1m_root, mg_var] 
	}
	
	def check_tasks(self, scrap_date: tuple[datetime, datetime]):
		scrap_date = datetime_interval(scrap_date[0], scrap_date[-1], timedelta(days = 1))
		self.new_scrap_date_list = [date for date in scrap_date if not os.path.exists(self.mg1_root(date))]

	def gz_processing(self, gz_file, url, date):
		tool = url.split('_')[1]
		dataset = xr.open_dataset(gz_file.read())
		df = dataset.to_dataframe()
		dataset.close()
		faraday_cup = df[self.var_meta[tool][1]]
		faraday_cup = faraday_cup.resample('1T').mean()
		faraday_cup.to_csv(self.var_meta[tool][0](self, date))

	async def download_url(self, url , date, session):
		async with session.get(url, ssl = True) as response:
			if response.status == 200:
				data = await response.read()
				await asyncGZ(BytesIO(data), self.gz_processing, url, date)

	def get_urls(self, scrap_date):
		with open('data/URLs.csv', 'r') as file:
				lines = file.readlines()
		url_list = []
		for url in lines:
			for date in scrap_date:
				if date+'000000' in url:
					url_list.append((url, date))
		return url_list
	
	def get_download_tasks(self, session):
		self.urls_dates = self.get_urls(self.new_scrap_date_list)
		return [self.download_url(url, date,session) for url, date in self.urls_dates]

	"""Prep pipeline"""
	def get_df(self, path, ind_date, obs):
		df = cudf.read_csv(path+ind_date+'.csv', index_col = 0)
		df = df[obs]
		return df	
	def get_dfs(self, path, scrap_date, obs):
		dfs = [self.get_df(path, date, obs) for date in scrap_date]
		return cudf.concat(dfs)
	
	def data_prep(self, scrap_date, step_size: timedelta):
		init, end = [cudf.to_datetime(date) for date in scrap_date]
		scrap_date = datetime_interval(scrap_date[0], scrap_date[-1], timedelta(days =1))

		fc1 = self.get_dfs(self.fc1_root, scrap_date, ['proton_density', 'proton_speed', 'proton_temperature'])
		mg1 = self.get_dfs(self.mg1_root, scrap_date, ['bx_gsm', 'by_gsm', 'bz_gsm', 'bt'])
		f1m = self.get_dfs(self.f1m_root, scrap_date, ['proton_density', 'proton_speed', 'proton_temperature'])
		m1m = self.get_dfs(self.m1m_root, scrap_date, ['bx_gsm', 'by_gsm', 'bz_gsm', 'bt'])

		l1 = cudf.concat([fc1.resample(timedelta_to_freq(step_size)).mean(), mg1.resample(timedelta_to_freq(step_size)).mean()], axis = 1)
		l2 = cudf.concat([f1m.resample(timedelta_to_freq(step_size)).mean(), m1m.resample(timedelta_to_freq(step_size)).mean()], axis = 1)

		#sampling
		l1 = l1[(l1.index >= init) & (l1.index <= end)]
		l2 = l2[(l2.index >= init) & (l2.index <= end)]
		
		return {
			'l1': self.apply_features(l1, 'bt', 'proton_density', 'proton_speed', 'proton_temperature'),
			'l2': self.apply_features(l2, 'bt', 'proton_density', 'proton_speed', 'proton_temperature'),
		}
		
	"""Downloader pipeline"""
	async def downloader_pipeline(self, scrap_date: tuple[datetime, datetime], session):
		self.check_tasks(scrap_date)
		if self.new_scrap_date_list == []:
			print('Already downloaded')
		else:
			await asyncio.gather(*self.get_download_tasks(session))