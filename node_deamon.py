import os

cmd = 'ps | grep -c exchange-xiaozhi-express'
cnt = os.popen(cmd).read().replace('\n','');

print cnt

if cnt == '2':
	cmd = '/etc/init.d/nodejs restart'
	print cmd
	os.system(cmd)
