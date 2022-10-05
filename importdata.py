import smart_open,gzip,PyPDF2,re

def cleanfile(path='census_microdata.csv.gz'):
	ILLINOIS = '21'
	COOK = '310'
	with smart_open.open(path,encoding='utf8') as f:
		header = next(f)
		yield header
		h = header.split(',')
		STATE = h.index('"STATEICP"')
		COUNTY = h.index('"COUNTYICP"')
		OWNERSHIP = h.index('"OWNERSHP"')
		for x in f:
			y = x.split(',')
			if y[STATE] == ILLINOIS and y[COUNTY] == COOK and y[OWNERSHIP] != '' and y[OWNERSHIP] != '0':
				yield x

def printdata(input,output,function):
	with gzip.open(f'{output}.csv.gz','wb') as f1:
		for x in function(f'{input}.csv.gz'):
			f1.write(x.encode())

def iscode(input):
	if len(input) != 8:
		return False
	for index,digit in enumerate(input):
		if index == 2:
			if digit != " ":
				return False
		else:
			if digit not in "0123456789":
				return False
	return True

def getpumas(year):
	with open(f'{year}_PUMA_Names.pdf','rb') as f:
		with open(f'{year}_PUMA_Names.txt','w') as f2:
			reader = PyPDF2.PdfFileReader(f)
			for x in range(reader.numPages):
				page = re.sub(' +',' ',reader.getPage(x).extractText()).split('\n')
				for line in page:
					if not iscode(line[:8]):
						continue
					statefp,puma,*name = line.split(" ")
					f2.write(puma+';'+' '.join(name)+'\n')

def getsample():
	with smart_open.open('cookcountycensus.csv.gz',encoding='utf8') as f:
		header = next(f)
		print(header)
		h = header.split(',')
		YEAR = h.index('"YEAR"')
		years = {}
		for x in f:
			y = x.split(',')
			year = y[YEAR]
			if year not in years:
				years[year] = 0
				print(x,end='')
			elif years[year] < 5:
				years[year] += 1
				print(x,end='')
			else:
				continue

def getacceptable(input):
	output = {}
	with open(f'{input}.txt','r') as f:
		for x in f:
			head,*tail = x.split(',')
			output[head] = tail
	return output

def getsouthside(path='cookcountycensus.csv.gz'):
	wards = getacceptable("acceptablewards")
	pumas = getacceptable("acceptablepumas")
	with smart_open.open(path,encoding='utf8') as f:
		header = next(f)
		h = header.split(',')
		yield header
		YEAR = h.index('"YEAR"')
		WARD = h.index('"WARD"')
		PUMA = h.index('"PUMA"')
		for x in f:
			y = x.split(',')
			year = y[YEAR]
			if year in wards.keys():
				if y[WARD] in wards[year]:
					yield x
			elif year in pumas.keys():
				if '0'+y[PUMA] in pumas[year] or y[PUMA] in pumas[year]:
					yield x
			else:
				yield x

def analyzedata():
	with smart_open.open('southside.csv.gz',encoding='utf8') as f:
		h = next(f).split(',')
		YEAR = h.index('"YEAR"')
		OWNERSHIP = h.index('"OWNERSHP"')
		RACE = h.index('"RACE"')
		output = {}
		for x in f:
			y = x.split(',')
			year = y[YEAR]
			if year not in output.keys():
				output[year] = [{},{}]
			if y[OWNERSHIP] not in output[year][0].keys() and y[OWNERSHIP] != 0:
				output[year][0][y[OWNERSHIP]] = 1
			else:
				output[year][0][y[OWNERSHIP]] += 1
			if y[RACE] not in output[year][1].keys():
				output[year][1][y[RACE]] = 1
			else:
				output[year][1][y[RACE]] += 1
		return output

def assortdata():
	header = True
	# print('year','ownership rate','percent white',sep=',')
	for year,items in analyzedata().items():
		for x in range(1,10):
			if str(x) not in items[1].keys():
				items[1][str(x)] = 0
		owners = items[0]['1']
		renters = items[0]['2']
		whites = items[1]['1']
		blacks = items[1]['2']
		natives = items[1]['3']
		chinese = items[1]['4']
		japanese = items[1]['5']
		asian = items[1]['6']
		other = items[1]['7']
		two_races = items[1]['8']
		three_plus = items[1]['9']
		l = locals()
		racestart = list(l.keys()).index('whites')
		raceend = list(l.keys()).index('three_plus')+1
		notwhite = sum(list(l.values())[racestart+1:raceend])
		ownership_rate = owners/(owners+renters)
		percent_white = whites/(whites+notwhite)
		# print(year,ownership_rate,percent_white,sep=',')
		if header:
			print('year','ownership rate',*list(l.keys())[racestart:raceend],sep=',')
			header = False
		print(year,ownership_rate,*list(l.values())[racestart:raceend],sep=',')

# printdata('cookcountycensus','southside',getsouthside)
assortdata()