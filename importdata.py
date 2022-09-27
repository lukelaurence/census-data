import sys,smart_open

def cleanfile(path):
	ILLINOIS = '21'
	COOK = '310'
	with smart_open.open(path,encoding='utf8') as f:
		header = next(f)
		print(header,end='')
		h = header.split(',')
		state = h.index('"STATEICP"')
		county = h.index('"COUNTYICP"')
		for x in f:
			y = x.split(',')
			if y[state] == ILLINOIS and y[county] == COOK:
				print(x,end='')

with open('cookcountycensus.csv','w') as f1:
	sys.stdout = f1
	cleanfile('census_microdata.csv.gz')

with open('cookcountycensus.csv','r') as f:
	a = 2000
	for x in f:
		print(x,end='')
		a -= 1
		if not a:
			exit(0)