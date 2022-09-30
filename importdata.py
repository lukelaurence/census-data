import smart_open,gzip,PyPDF2,re

def cleanfile(path):
	ILLINOIS = '21'
	COOK = '310'
	with smart_open.open(path,encoding='utf8') as f:
		header = next(f)
		yield header
		h = header.split(',')
		STATE = h.index('"STATEICP"')
		COUNTY = h.index('"COUNTYICP"')
		for x in f:
			y = x.split(',')
			if y[STATE] == ILLINOIS and y[COUNTY] == COOK:
				yield x

def cleandata():
	with gzip.open('cookcountycensus.csv.gz','wb') as f1:
		for x in cleanfile('census_microdata.csv.gz'):
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