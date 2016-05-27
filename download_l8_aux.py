'''
File: download_aux.py
Author: Min Feng
Version: 0.1
Create: 2016-05-27 11:01:22
Description: download auxiliary data for running L8SR
modified based on a codesnip wrote by @yannforget
'''

import os
from subprocess import call
import re
import requests

def _dl_cmg(date, data_dir):
	"""Download CMG products."""
	_host = 'e4ftl01.cr.usgs.gov'

	_mod09 = 'MOLT/MOD09CMG.006'
	_myd09 = 'MOLA/MYD09CMG.006'

	for _path in [_mod09, _myd09]:
		_code = _path[5:13]
		baseurl = 'http://%s/%s/%s/' % (_host, _path, date.strftime('%Y.%m.%d'))
		html = requests.get(baseurl).content.decode('utf-8')
		re_pattern = '%s.A%s%s.006.\d{13}.hdf' % (_code, date.strftime('%Y'), date.strftime('%j'))

		fn = re.search(re_pattern, html).group(0)
		url = baseurl + fn
		call(['wget', '-P', '-a', data_dir, url])

def _ftp_download(ftp, path, f_out):
	if os.path.exists(f_out) and os.path.getsize(f_out) > 0:
		# skip existed file
		return

	with open(f_out, 'wb') as _fo:
		ftp.retrbinary('RETR ' + path, _fo.write)

def _dl_cma(date, data_dir, username, password):
	"""Download CMA products."""
	os.path.exists(data_dir) or os.makedirs(data_dir)

	import ftplib

	_ftp = ftplib.FTP('ladssci.nascom.nasa.gov')
	_ftp.login(username, password)

	for product in ['MOD09CMA', 'MYD09CMA']:
		_dir = '/'.join(['', '6', product, date.strftime('%Y'), date.strftime('%j')])

		for _f in _ftp.nlst(_dir):
			_ftp_download(_ftp, _dir + '/' + _f, os.path.join(data_dir, _f))

def main():
	_opts = _usage()

	_d_out = _opts.output

	import datetime
	_d = datetime.datetime(2013, 1, 1)
	_t = datetime.timedelta(1)

	while _d < datetime.datetime.now():
		print '+ date', _d
		_dl_cma(_d, _d_out, _opts.username, _opts.password)
		_dl_cmg(_d, _d_out)

		_d += _t

def _usage():
	import argparse

	_p = argparse.ArgumentParser()

	_p.add_argument('-o', '--output', dest='output', required=True)
	_p.add_argument('-u', '--username', dest='username', required=True)
	_p.add_argument('-p', '--password', dest='password', required=True)

	return _p.parse_args()

if __name__ == '__main__':
	main()

