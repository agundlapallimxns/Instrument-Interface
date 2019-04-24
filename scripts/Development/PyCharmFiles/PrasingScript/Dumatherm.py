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


# get Data from DB
def getDataFromDB(sampleNumbersList):
    dburl = 'oracle://'+dbUser+':'+dbPassword+'@'+dbHost+':'+dbPort+'/'+dbName
    engine=sql.create_engine(dburl)
    resultdf=pd.DataFrame()
    sampleNumbrStr=(', '.join("'" + str(item) + "'" for item in sampleNumbersList))
    queryText="SELECT sample_number, analysis, REPLICATE_COUNT,TEST_NUMBER FROM test"
    queryText=queryText+" WHERE SAMPLE_NUMBER IN ("+sampleNumbrStr+") AND analysis IN ('PROT-COMII', 'PROT-HICOM')"
    conn = engine.connect()
    resultdf = pd.read_sql(queryText, engine)
    conn.close()
    return resultdf

#write the file
def writeFile(dframe,lab,inst,archDestination,importDest,datapath,fname):
    timestr = time.strftime("%Y%m%d%H%M%S")
    FileName = importDest + lab+'_'+inst+'_' + timestr + '.csv'
    dframe.to_csv(FileName, index=False, encoding="utf-8",quotechar='"',quoting=csv.QUOTE_NONNUMERIC) # write  out the file to be imported
    destination = archDestination + fname[:-4] + '.bak' # Backup the processed file
    os.rename(datapath, destination)
    time.sleep(1)

# Default Parameters setting
with open('.\configFiles\DumathermConfig.json', 'r') as f:
    config = json.load(f)
print(config)
labs = config['lab']
analysis = config['analysis']
inst = config['inst']
colsOrder=['Sample.Sample_Number', 'Test.Test_Number','Result.Name', 'Result.Entry', 'Result.Entered_By','Result.Instrument']

for eachlab in labs:
    path='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//Import//'+inst+'//'
    archivedDestination='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//Import//'+inst+'//'
    importDest='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//impf//'
    print(path)
    if os.path.exists(path):
        pass
    else:
        os.makedirs(path)
    files = os.listdir(path)
    for filename in files:
        if fnmatch.fnmatch(filename,'*.CSV'):
            datapath = path + str(filename)
            rawdf = pd.read_csv(datapath, header=None)
            rawdf = rawdf.rename(index=str, columns={0: 'sample_number', 1: 'var1', 2: 'var2'})
            rawdf = rawdf[rawdf['sample_number'].map(len) == 9]
            sampleNumbersList = list(rawdf['sample_number'].unique())
            databasedf = getDataFromDB(sampleNumbersList)
            rawdf = rawdf.astype({"sample_number": int})
            dfResult = rawdf.merge(databasedf, how='inner', on=['sample_number'])
            dfResult = dfResult.rename(index=str,
                                       columns={'sample_number': 'Sample.Sample_Number', 'var2': 'Result.Entry',
                                                'test_number': 'Test.Test_Number'})
            dfResultReplicateMax = dfResult.groupby('Sample.Sample_Number')['replicate_count'].max().reset_index()
            dfResultFinal = dfResult.merge(dfResultReplicateMax, how='inner',
                                           on=['Sample.Sample_Number', 'replicate_count'])
            dfResultFinal['Result.Name'] = 'Nitrogen (from instrument)'
            dfResultFinal['Result.Entered_By'] = 'IMPORTER'
            dfResultFinal['Result.Instrument'] = 'Dumatherm'
            dfResultFinal = dfResultFinal[colsOrder]
            writeFile(dfResultFinal, eachlab, inst, path, importDest, datapath, filename)