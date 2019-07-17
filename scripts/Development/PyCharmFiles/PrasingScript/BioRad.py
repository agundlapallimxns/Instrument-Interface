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
dbHost='mxnunx41'
dbPort='1521'
dbUser='lims'
dbPassword='Mumwit63'
dbName='hwi1'
dburl='oracle://'


# Default Parameters setting
with open('.//configFiles//BioRad.json', 'r') as f:
    config = json.load(f)
lab = config['lab']
analysis = config['analysis']
inst = config['inst']
mode = config['mode']
colsOrder=['Sample.Sample_Number', 'Test.Test_Number','Result.Name', 'Result.Entry', 'Result.Entered_By','Result.Instrument']

# get Data from DB
def getDataFromDB(sampleNumbersList):
    dburl = 'oracle://'+dbUser+':'+dbPassword+'@'+dbHost+':'+dbPort+'/'+dbName
    engine=sql.create_engine(dburl)
    resultdf=pd.DataFrame()
    sampleNumbrStr=(', '.join("'" + item + "'" for item in sampleNumbersList))
    queryText="SELECT sample_number, analysis, REPLICATE_COUNT,TEST_NUMBER FROM test"
    queryText=queryText+" WHERE SAMPLE_NUMBER IN ("+sampleNumbrStr+") AND analysis IN ('LSP-IQ125C')"
    conn = engine.connect()
    resultdf = pd.read_sql(queryText, engine)
    conn.close()
    return resultdf

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
    print('Lab:'+eachlab)
    path='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//Import//'+inst+'//Datafolder//'
    archivedDestination='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//Import//'+inst+'//'
    if(mode=='T'):
        importDest='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//impf//testing//'
    else:
        importDest='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//impf//'
    if os.path.exists(path):
        pass
    else:
        os.makedirs(path)
    if os.path.exists(importDest):
        pass
    else:
        os.makedirs(importDest)
    files = os.listdir(path)
    print (path)
    print(files)
    for filename in files:
        if fnmatch.fnmatch(filename,'*.csv'):
            print (filename)
            datapath = path + str(filename)
            rawdf = pd.read_csv(datapath, sep=',', index_col=False, skiprows=23)
            rawdf = rawdf[rawdf.Fluor != 'HEX']
            rawdf = rawdf.rename(index=str, columns={'Sample': 'sample_number'})
            rawdf = rawdf[pd.notnull(rawdf['sample_number'])]
            rawdf = rawdf.astype({"sample_number": int})
            rawdf = rawdf.astype({"sample_number": str})
            sampleNumbersList = list(rawdf['sample_number'].unique())
            databasedf = getDataFromDB(sampleNumbersList)
            rawdf = rawdf.astype({"sample_number": int})
            dfResult = rawdf.merge(databasedf, how='inner', on=['sample_number'])
            dfResult = dfResult.rename(index=str,
                                       columns={'sample_number': 'Sample.Sample_Number', 'Result': 'Result.Entry',
                                                'Titrant volume for sample (ml)': 'Result.Entry',
                                                'test_number': 'Test.Test_Number'})
            dfResultReplicateMax = dfResult.groupby('Sample.Sample_Number')['replicate_count'].max().reset_index()
            dfResultFinal = dfResult.merge(dfResultReplicateMax, how='inner',
                                           on=['Sample.Sample_Number', 'replicate_count'])
            dfResultFinal['Result.Name'] = 'PCR'
            dfResultFinal['Result.Entered_By'] = 'IMPORTER'
            dfResultFinal['Result.Instrument'] = 'BIO-RAD'
            dfResultFinal = dfResultFinal[colsOrder]
            writeFile(dfResultFinal, eachlab, inst, path, importDest, datapath, filename)
print("Process Done!")