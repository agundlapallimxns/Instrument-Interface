# Import Functions
import pandas as pd
import os
import time
import csv
import json
import fnmatch
import csv
import sqlalchemy as sql
import numpy as np
import warnings
import chardet

warnings.filterwarnings("ignore")
dbHost='mxnunx41'
dbPort='1521'
dbUser='lims'
dbPassword='Mumwit63'
dbName='hwi1'
dburl='oracle://'

# Default Parameters setting
with open('./ConfigFiles/HachConfig.json', 'r') as f:
    config = json.load(f)
print(config)
lab = config['lab']
analysis = config['analysis']
inst = config['inst']
colsOrder=['Sample.Sample_Number', 'Test.Test_Number','Result.Name', 'Result.Entry', 'Result.Entered_By']


# get Data from DB
def getDataFromDB(sampleNumbersList):
    dburl = 'oracle://'+dbUser+':'+dbPassword+'@'+dbHost+':'+dbPort+'/'+dbName
    engine=sql.create_engine(dburl)
    resultdf=pd.DataFrame()
    sampleNumbrStr=(', '.join("'" + item + "'" for item in sampleNumbersList))
    queryText="SELECT sample_number, analysis, REPLICATE_COUNT,TEST_NUMBER FROM test"
    queryText=queryText+" WHERE SAMPLE_NUMBER IN ("+sampleNumbrStr+") AND analysis IN ('SODIUM-ISE')"
    conn = engine.connect()
    resultdf = pd.read_sql(queryText, engine)
    conn.close()
    return resultdf

def changeEncoding(filePath):
    '''take a full path to a file as input, and change its encoding from gb18030 to utf-8'''
    with open(filePath, 'rb') as f:
        content_bytes = f.read()
    detected = chardet.detect(content_bytes)
    encoding = detected['encoding']
    print(f"{filename}: detected as {encoding}.")
    content_text = content_bytes.decode(encoding)
    with open(filePath, 'w', encoding='utf-8') as f:
        f.write(content_text)

def formatSampleNumber(sampleNumstring):
    return sampleNumstring[:9]

#write the file
def writeFile(dframe,lab,inst,archDestination,importDest,datapath,fname):
    if os.path.exists(importDest):
        pass
    else:
        os.makedirs(importDest)
    timestr = time.strftime("%Y%m%d%H%M%S")
    FileName = importDest + lab+'_'+inst+'_' + timestr + '.csv'
    dframe.to_csv(FileName, index=False, encoding="utf-8",quotechar='"',quoting=csv.QUOTE_NONNUMERIC) # write  out the file to be imported
    destination = archDestination + fname[:-4] + '.bak' # Backup the processed file
    os.rename(datapath, destination)
    print("Processed File: " + fname[:-4])
    time.sleep(1)

for eachlab in lab:
    path='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//Import//'+inst+'//'
    archivedDestination='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//Import//'+inst+'//'
    importDest='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//impf//'
    if os.path.exists(path):
        pass
    else:
        os.makedirs(path)
    files = os.listdir(path)
    for filename in files:
        if fnmatch.fnmatch(filename,'*.CSV'):
            datapath = path + str(filename)
            changeEncoding(datapath)
            rawdf = pd.read_csv(datapath, encoding='utf-8')
            ImpCols = ['Type', 'Sample ID', 'Primary Reading Value']
            rawdf = rawdf[ImpCols]
            rawdf.drop(rawdf[rawdf.Type != 'RD'].index, inplace=True)
            rawdf['sample_number'] = rawdf['Sample ID'].apply(formatSampleNumber)
            sampleNumbersList = list(rawdf['sample_number'].unique())
            databasedf = getDataFromDB(sampleNumbersList)
            rawdf = rawdf.astype({"sample_number": int})
            dfResult = rawdf.merge(databasedf, how='inner', on=['sample_number'])
            dfResult = dfResult.rename(index=str,
                                       columns={'sample_number': 'Sample.Sample_Number', 'resultname': 'Result.Name',
                                                'Primary Reading Value': 'Result.Entry',
                                                'test_number': 'Test.Test_Number'})
            dfResultReplicateMax = dfResult.groupby('Sample.Sample_Number')['replicate_count'].max().reset_index()
            dfResultFinal = dfResult.merge(dfResultReplicateMax, how='inner',
                                           on=['Sample.Sample_Number', 'replicate_count'])
            dfResultFinal['Result.Name'] = 'Inst. Read-out'
            dfResultFinal['Result.Entered_By'] = 'IMPORTER'
            dfResultFinal = dfResultFinal[colsOrder]
            writeFile(dfResultFinal, eachlab, inst, path, importDest, datapath, filename)
print("Process Done")