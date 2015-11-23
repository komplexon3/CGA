# CGA - CSV Gender Adder
# written by Marc Widmer 2015
# for support contact marcmwwidmer@gmail.com

import urllib2
import threading
import json
import time
import csv

debug = False

printLock = threading.Lock()
rawDataArrLock = threading.Lock()
threads =[]

def threadPrint(input):
	printLock.acquire()
	print(input)
	printLock.release()

class GenderAdd():
	genderData = {}
	rawDataArr = []
	def __init__(self):
		self.importFile = raw_input('\nEnter name of the CSV file to which you want to add gender information with extension e.g. .csv: ')
		self.excel = True if raw_input('\nWas this file orinally a excel file? (yes/no): ') == 'yes' else False
		self.exportFile = raw_input('\nEnter the name you want to give the export file with extension (If left empty, input file will be overridden and corruption might happen if errors occur): ') or self.importFile
		self.fnTag = raw_input('\nEnter title of "first name"-column: ')
		self.gTag = raw_input('\nEnter the name you want to give the "gender"-column or if this column already exists, enter its name (Errors occur if wrong): ')
		self.pTag = raw_input('\nEnter the name you want to give the "probability"-column or if this column already exists, enter its name (Errors occur if wrong): ')
		self.splitChar = raw_input('\nEnter the character used to seperate values ( "," is standart and will be used if left empty): ') or ','
		nPerReq = raw_input('\nEnter the number of names that should be used per request (10 is maximum and will be used if left empty), ') or str(10)
		self.namesPerReq = int(nPerReq)

	def csvImporter(self):
		self.importedNames = []
		try:
			with open(self.importFile, 'rU') as csvFile:
				if self.excel:
					reader = csv.reader(csvFile, dialect = csv.excel_tab)
				else:
					reader = csv.reader(csvFile)
				count = 0
				if debug: print reader #debug
				self.csvData = []
				
				for row in reader:
					csvSub = row[0].split(self.splitChar)
					if debug: print csvSub #debug
					self.csvData.append(csvSub)
		except:
			print '\n#########################################################\n\nThe file ' + self.importFile + ' does not exist in this directory.\nCheck name, make sure that the file is placed in the same directory as this script and try again.\n\n#########################################################\n\nClosing CGA...\n'
			raise SystemExit
				
		if debug: print self.csvData #debug
		self.csvLen = len(self.csvData)
		self.addFields = 0 #for csvDataFiller
		
		for i in range(self.csvLen):
			
			if count == 0:
				try:
					self.fnIndex = self.csvData[0].index(self.fnTag)
				except:
					print '\n#########################################################\n\nThe "first name"- column name you gave cannot be found!\nCheck the name in ' + self.importFile + ' and try again.\n\n#########################################################\n\nClosing CGA...\n'
					raise SystemExit
				
				try:
					self.gIndex = self.csvData[0].index(self.gTag)
				except:
					self.gIndex = len(self.csvData[0])
					self.csvData[0].append(self.gTag)
					self.addFields += 1
				
				try:
					self.pIndex = self.csvData[0].index(self.pTag)
				except:
					self.pIndex = len(self.csvData[0])
					self.csvData[0].append(self.pTag)
					self.addFields += 1
				
			else:
				self.importedNames.append(self.csvData[i][self.fnIndex].lower())
				if debug: print self.csvData[i][self.fnIndex] #debug
				
			count+= 1
				
		self.nLen = len(self.importedNames)
		if debug: print self.importedNames #debug

	def namesSpliter(self):
		nameArr = []
		nOfSplits = self.nLen/self.namesPerReq
		plusOne = self.nLen%self.namesPerReq
		count = 0
		
		for i in range(nOfSplits):
			subNameArr = []
			
			for j in range(self.namesPerReq):
				subNameArr.append(self.importedNames[count])
				count+= 1
				
			nameArr.append(subNameArr)
			if debug: print subNameArr #debug
			
		if plusOne > 0:
			subNameArr = []
			
			for j in range(plusOne):
				subNameArr.append(self.importedNames[count])
				count+= 1
				
			nameArr.append(subNameArr)
			
		if debug: print nameArr #debug
		return nameArr

	def singleUrlCreator(self, reqNames, reqN):
		url = 'http://api.genderize.io/?'
		print 'Creating request URL #' + str(reqN+1) + '...'
		
		for i in range(len(reqNames)):
			url +=('name['+str(i)+']='+reqNames[i].lower()+'&')
			
		url = url[:-1]
		if debug: print url #debug
		return url

	def urlsCreator(self):
		names = self.namesSpliter()
		self.urls = []
		
		for i in range(len(names)):
			url = self.singleUrlCreator(names[i], i)
			self.urls.append(url)

	def dataRequest(self, reqUrl, reqN):
		threadPrint('Sending request #' + str(reqN+1) + '...')
		
		try:
			req = urllib2.urlopen(reqUrl)
		except urllib2.HTTPError as error:
			threadPrint('\n#########################################################\n\nERROR ' + str(error.code) + ' - ' + str(error.reason) + '\n')
			if error.code == 429:
				threadPrint('######################################################### \n\nYou used up all 1000 names. Try again tomorrow or change internet access.')
				threadPrint('\n#########################################################\n\nAboarding Requests... \nClosing CGA...\n')
			raise SystemExit
		except urllib2.URLError as error:
			threadPrint('\n#########################################################\n\nERROR ' + str(error.args[0][0]) + ' - ' + str(error.args[0][1]))
			if error.args[0][0] == 8:
				threadPrint('\n######################################################### \n\nNo intenet connection. Connect to the internet and try again.')
			threadPrint('\n#########################################################\n\nAboarding Requests... \nClosing CGA...\n')
			raise SystemExit
			
		threadPrint('Answer for request #' + str(reqN+1) + ' received.')
		jData = req.read()
		data = json.loads(jData)
		if debug: print ans #debug
		rawDataArrLock.acquire()
		self.rawDataArr.extend(data)
		rawDataArrLock.release()
		threadPrint('Raw data from request #' + str(reqN+1) + ' saved.')

	def requestThreadsController(self):
		urlsLen = len(self.urls)
		for i in range(urlsLen):
			threads.append(requestThread(self, self.urls[i], i))
			threads[i].start()
		for thread in threads:
			thread.join()

	def rawDataProcessor(self):
		for i in range(self.nLen):
			print 'Processing raw data from #' + str(i+1) + ' out of ' + str(self.nLen) + ' names...'
			subDict = self.rawDataArr[i]
			if subDict['gender'] == None:
				self.genderData[str(subDict['name'])] = ['unknown','0.00']
			else:
				self.genderData[str(subDict['name'])] = [str(subDict['gender']),str(subDict['probability'])]

			if debug: print subDict #debug
		print 'All data from ' + str(self.nLen) + ' names processed.'
		if debug: print self.genderData #debug

	def csvDataFiller(self):
		for i in range(self.csvLen):
			if i > 0:
				name = self.importedNames[i-1]
				
				for j in range(self.addFields):
					self.csvData[i].append(' ')
					
				if debug: print len(self.csvData[i]) #debug
				if debug: print self.gIndex #debug
				if debug: print self.pIndex #debug
				
				self.csvData[i][self.gIndex] = self.genderData[name][0]
				self.csvData[i][self.pIndex] = self.genderData[name][1]
				if debug: print str(i) #debug
				
		if debug: print self.csvData #debug

	def csvDataCleaner(self): #getting rid of the f**king "'s
		for row in range(self.csvLen):
			for element in range(self.pIndex): #pIndex = row length
				self.csvData[row][element] = self.csvData[row][element].replace('"','')

	def csvExporter(self):
		print 'Exporting data...'
		with open(self.exportFile, 'w') as csvFile:
			writer = csv.writer(csvFile)
			writer.writerows(self.csvData)
		print 'Information for all ' + str(self.nLen) + ' saved in ' + self.exportFile + '.'

	def main(self):
		start = time.time()
		self.csvImporter()
		self.urlsCreator()
		self.requestThreadsController()
		self.rawDataProcessor()
		self.csvDataFiller()
		self.csvDataCleaner()
		self.csvExporter()
		duration = time.time() - start
		print '\n#########################################################\n\nFinishhed.\nThe task took ' + str(duration) + ' seconds to complete.\n'

class requestThread (threading.Thread):
	def __init__(self, gAdder, reqUrl, reqN):
		threading.Thread.__init__(self)
		self.gAdder = gAdder
		self.reqUrl = reqUrl
		self.reqN = reqN

	def run(self):
		self.gAdder.dataRequest(self.reqUrl, self.reqN)

if True:
	print '\nWelcome to CGV - CSV Gender Adder - 2015 \n\nby Nasib Naimi & Marc Widmer - written by Marc Widmer \nIf any errors or other problems occur, contact marcmwwidmer@gmail.com \n\nThis program takes a csv contacts file and adds all given information plus the gender and the probability that it is this gender to a new CSV file. This is done using the web database of "genderize.io". Even though this database covers a wide selcetion of names, some are missing. If this is the case, the gender will be set to "unknown" and the probability to 0%. \n\nIMPORTANT: The web database used in this program limits the amount of requests to 1000 names per day. Therefore if you go over this limit, the program will inform you and then close. If this problem occurs, try again tomorrow or change your internet access to bypass this limitation. \n\nPlace the file you want to add the gender information to into the same folder as this script and then follow the the commands below.'

	genderAdd = GenderAdd()
	genderAdd.main()