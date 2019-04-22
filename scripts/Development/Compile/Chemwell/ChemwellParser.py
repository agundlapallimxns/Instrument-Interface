'''
# Author :Abhinav Gundlapalli
# Email: agundlapalli@uh.edu / abhinav.gundlapalli@mxns.com
# Code : Developed for parsing script for Chem well Allergenrs at Crete Lab
# Input: Raw data file from the instrument
# ouput: CSV file to be imported into LIMS
# Version : 1
'''

# Import Functions
import pandas as pd
import os
import time
import csv
import sqlalchemy as sql

# Default Parameters setting
path='//mxns.loc//shares//NA-Instruments//Prod//Crete//Import//Chemwell//RawFiles//'
pathrawfiles='//mxns.loc//shares//NA-Instruments//Prod//Crete//Import//Chemwell//RawFiles//'
archivedDestination='//mxns.loc//shares//NA-Instruments//Prod//Crete//Import//Chemwell//Archived//'
importDest='//mxns.loc//shares//NA-Instruments//Prod//Crete//impf//'
colsOrder=['Sample.Sample_Number', 'Test.Analysis','Result.Name', 'Result.Entry', 'Result.Entered_By']
lab='CRE'
inst='Chemwell'
dbHost='mxnunx41'
dbPort='1521'
dbUser='lims'
dbPassword='Mumwit63'
dbName='hwi1'
dburl='oracle://'

# get Data from DB
def getDataFromDB(sampleNumbersList):
    dburl = 'oracle://'+dbUser+':'+dbPassword+'@'+dbHost+':'+dbPort+'/'+dbName
    engine=sql.create_engine(dburl)
    resultdf=pd.DataFrame()
    sampleNumbrStr=(', '.join("'" + item + "'" for item in sampleNumbersList))
    queryText="SELECT tsttbl.SAMPLE_NUMBER as Sample, tsttbl.TEST_NUMBER, max(tsttbl.REPLICATE_COUNT) AS REPLICATE_COUNT FROM ( SELECT SAMPLE_NUMBER , TEST_NUMBER, REPLICATE_COUNT FROM TEST"
    queryText=queryText+" WHERE SAMPLE_NUMBER IN ("+sampleNumbrStr+")AND ANALYSIS = 'ALLERGLIAD' ) tsttbl GROUP BY (tsttbl.SAMPLE_NUMBER, tsttbl.TEST_NUMBER)"
    conn = engine.connect()
    resultdf = pd.read_sql(queryText, engine)
    conn.close()
    resultdf=resultdf.drop(['replicate_count'], axis=1)
    return resultdf

# Get the List of files
files = os.listdir(pathrawfiles)
# Iterate through List of files
for name in files:
	datapathxl = pathrawfiles + str(name)
	dataxl = pd.read_excel(datapathxl)   # read the Excel file from the instrument
	dataxl1 = dataxl.drop_duplicates(subset='Sample', keep='first')
	dataxl11 = dataxl1[dataxl1['Sample'].map(len) == 11] # Subset only the valid sample numbers
	collist = ['Sample', 'Abs', 'Conc']   # define the required columns for the import files
	dataxl111 = dataxl11[collist] 			# Subset the required columns for the import files
	dataxl111['Sample'] = dataxl111['Sample'].str[2:] # format the sample numbers
	dataxl111 = pd.melt(dataxl111, id_vars=['Sample'], value_vars=['Abs', 'Conc']) # Pivot the samples numbers
	# formatting the dataframe to get in respect to the Import Functionality
	sampleNumbersList = list(dataxl111['Sample'].unique())
	testNumberdf = getDataFromDB(sampleNumbersList)
	testNumberdf = testNumberdf.rename(index=str, columns={'sample': 'Sample'})
	dataxl111 = dataxl111.astype({"Sample": int})
	dfResult = pd.merge(dataxl111, testNumberdf, on='Sample')
	dfResult = dfResult.replace('Abs', 'Absorbance')
	dfResult = dfResult.replace('Conc', 'Gliadin (Component of Gluten)')
	dfResult = dfResult.rename(index=str, columns={'Sample': 'Sample.Sample_Number', 'variable': 'Result.Name',
												   'value': 'Result.Entry', 'test_number': 'Test.Test_Number'})
	# dfResult['Test.Analysis'] = 'ALLERGLIAD'
	dfResult['Result.Entered_By'] = 'IMPORTER'
	dfResult = dfResult.sort_values(by=['Sample.Sample_Number'])
	dfResult = dfResult.astype({"Sample.Sample_Number": int})
	timestr = time.strftime("%Y%m%d%H%M%S")
	FileName = importDest + lab + '_' + inst + '_' + timestr + '.csv'
	dfResult.to_csv(FileName, index=False, encoding="utf-8", quotechar='"',
					quoting=csv.QUOTE_NONNUMERIC)  # write  out the file to be imported
	destination = archivedDestination + name[:-4] + '_raw.ARK'  # Backup the processed file
	os.rename(datapathxl, destination)
	time.sleep(2)