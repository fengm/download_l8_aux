'''
File: download_aux.py
Author: Min Feng
Version: 0.1
Create: 2016-05-27 11:01:22
Description: download auxiliary data for running L8SR
modified based on a codesnip wrote by @yannforget
'''

import logging

def _combine(d_inp, date, d_out):
	from subprocess import call
	from glob import glob
	import os

	# Get filenames
	terra_cmg = glob(os.path.join(d_inp, 'MOD09CMG.A%s.006.*' % date.strftime('%Y%j')))[0]
	terra_cma = glob(os.path.join(d_inp, 'MOD09CMA.A%s.006.*' % date.strftime('%Y%j')))[0]
	aqua_cma = glob(os.path.join(d_inp, 'MYD09CMA.A%s.006.*' % date.strftime('%Y%j')))[0]
	aqua_cmg = glob(os.path.join(d_inp, 'MYD09CMG.A%s.006.*' % date.strftime('%Y%j')))[0]

	_f_out = os.path.join(d_out, date.strftime('L8ANC%Y%j.hdf_fused'))
	if not (os.path.exists(_f_out) and os.path.getsize(_f_out) > 0):
		# Combine data for l8sr
		call(['combine_l8_aux_data', '--terra_cmg', terra_cmg,
			  '--terra_cma', terra_cma, '--aqua_cmg', aqua_cmg,
			  '--aqua_cma', aqua_cma, '--output_dir', d_out])

	if os.path.exists(_f_out) and os.path.getsize(_f_out) > 0:
		map(os.remove, [terra_cmg, terra_cma, aqua_cma, aqua_cmg])

def _dl_cmg(date, data_dir):
	"""Download CMG products."""
	import requests
	import re
	import os
	from subprocess import call

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

		fo = os.path.join(data_dir, fn)
		os.path.exists(fo) and os.remove(fo)

		call(['wget', '--user', 'mfeng', '--password', '127321Xy', '-P', data_dir, url])

def _ftp_download(ftp, path, f_out):
	with open(f_out, 'wb') as _fo:
		ftp.retrbinary('RETR ' + path, _fo.write)

def _dl_cma(date, data_dir, username, password):
	"""Download CMA products."""
	import os
	import ftplib

	os.path.exists(data_dir) or os.makedirs(data_dir)

	_ftp = ftplib.FTP('ladssci.nascom.nasa.gov')
	_ftp.login(username, password)

	for product in ['MOD09CMA', 'MYD09CMA']:
		_dir = '/'.join(['', '6', product, date.strftime('%Y'), date.strftime('%j')])

		for _f in _ftp.nlst(_dir):
			_ftp_download(_ftp, _dir + '/' + _f, os.path.join(data_dir, _f))

def _to_date(d):
	import datetime

	if len(d) == 8:
		return datetime.datetime.strptime(d, '%Y%m%d')
	if len(d) == 7:
		return datetime.datetime.strptime(d, '%Y%j')

	import re
	_m = re.match('(\d{4})\D(\d{2})\D(\d{2})', d)
	if _m:
		return datetime.datetime.strptime('%s%s%s' % (_m.group(1), _m.group(2), _m.group(3)), '%Y%m%d')

	raise Exception('failed to parse the date')

def main():
	_opts = _usage()

	import datetime
	import os

	_d1 = _to_date(_opts.date[0])
	if len(_opts.date) > 1:
		_d2 = _to_date(_opts.date[1])
	else:
		_d2 = _d1

	assert(_d2 >= _d1)

	_d = _d1
	_t = datetime.timedelta(1)

	while _d <= _d2:
		print '+ date', _d

		_d_out = os.path.join(_opts.output, _d.strftime('%Y'))

		_f_out = os.path.join(_d_out, _d.strftime('L8ANC%Y%j.hdf_fused'))
		if not (os.path.exists(_f_out) and os.path.getsize(_f_out) > 0):

			try:
				_dl_cma(_d, _d_out, _opts.username, _opts.password)
				_dl_cmg(_d, _d_out)
				_combine(_d_out, _d, _d_out)
			except KeyboardInterrupt:
				print '\n\n* User stopped the program'
			except Exception, err:
				import traceback

				logging.error(traceback.format_exc())
				logging.error(str(err))

				print '\n\n* Error:', err

		_d += _t

def _usage():
	import argparse

	_p = argparse.ArgumentParser()

	_p.add_argument('-o', '--output', dest='output', required=True)
	_p.add_argument('-u', '--username', dest='username', required=True)
	_p.add_argument('-p', '--password', dest='password', required=True)
	_p.add_argument('-d', '--date', dest='date', required=True, nargs='+')

	return _p.parse_args()

if __name__ == '__main__':
	main()

