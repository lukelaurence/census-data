def condensecensusdata():
	ILLINOIS = '21'
	COOK = '310'
	with open('census.csv','r') as f:
		header = next(f)
		print(header,end='')
		h = header.split(',')
		state = h.index('"STATEICP"')
		county = h.index('"COUNTYICP"')
		for x in f:
			y = x.split(',')
			if y[state] == ILLINOIS and y[county] == COOK:
				print(x,end='')

with open('cookcountycensus.csv','r') as f:
	a = 2000
	for x in f:
		print(x,end='')
		a -= 1
		if not a:
			exit(0)