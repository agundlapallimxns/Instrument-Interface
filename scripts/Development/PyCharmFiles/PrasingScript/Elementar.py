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
test1=None
dbHost='mxnunx41'
dbPort='1521'
dbUser='lims'
dbPassword='Mumwit63'
dbName='hwi1'
dburl='oracle://'


#write the file
def writeFile(dframe,lab,inst,archDestination,importDest,datapath,fname):
    timestr = time.strftime("%Y%m%d%H%M%S")
    FileName = importDest + lab+'_'+inst+'_' + timestr + '.csv'
    dframe.to_csv(FileName, index=False, encoding="utf-8",quotechar='"',quoting=csv.QUOTE_NONNUMERIC) # write  out the file to be imported
    destination = archDestination + fname[:-4] +'_'+timestr +'.bak' # Backup the processed file
    os.rename(datapath, destination)
    print("Processed File: "+fname[:-4])
    time.sleep(1)

# get Data from DB
def getDataFromDB(sampleNumbersList):
    dburl = 'oracle://'+dbUser+':'+dbPassword+'@'+dbHost+':'+dbPort+'/'+dbName
    engine=sql.create_engine(dburl)
    resultdf=pd.DataFrame()
    sampleNumbrStr=(', '.join("'" + str(item) + "'" for item in sampleNumbersList))
    queryText="SELECT sample_number, analysis, REPLICATE_COUNT,TEST_NUMBER FROM test"
    queryText=queryText+" WHERE SAMPLE_NUMBER IN ("+sampleNumbrStr+") AND analysis IN ('FBR-SR-3C', 'CODEX-IST', 'CODEX-T')"
    conn = engine.connect()
    resultdf = pd.read_sql(queryText, engine)
    conn.close()
    return resultdf

#Get the suffix from the batch
def extractsuffix(sampleNumstring):
    global test1
    if len(sampleNumstring)>18.0:
        if '/' in sampleNumstring:
            t=sampleNumstring.index("/")+1
            test1=sampleNumstring[t:]
            return test1
        elif '-' in sampleNumstring:
            t=sampleNumstring.index("-")
            analysis=sampleNumstring[:t]
            if(analysis=='CODEX'):
                test1='CODEX'
                return test1
            elif (analysis=='FBR'):
                test1='FBR'
                return test1
    elif len(sampleNumstring)<=9:
        return test1
    elif len(sampleNumstring)==11:
        return test1
    return None

#Map the analysis
def analysisdef(test):
    analysis = {'IDF': 'CODEX-IST', 'SDF':'CODEX-IST','CODEX':'CODEX-T','FBR':'FBR-SR-3C'}
    return analysis.get(test)

#Map the result type
def resultType(resultype):
    resultName = {'IDF': 'IDF % Protein', 'SDF':'SDF % Protein','CODEX':'TDF % Protein','FBR':'% Protein (xtest)'}
    return resultName.get(resultype)

#Get the replicate if present
def getreplicate(sampleNumstring):
    if len(sampleNumstring)==11:
        if '/' in sampleNumstring:
            t=sampleNumstring.index("/")+1
            rep=sampleNumstring[t:]
            return rep
    return 1

#formatting Sample Number
def formatSampleNumber(sampleNumstring):
    return sampleNumstring[:9]


# Default Parameters setting
with open('./ConfigFiles/ElementarConfig.json', 'r') as f:
    config = json.load(f)
print(config)
lab = config['lab']
analysis = config['analysis']
inst = config['inst']
colsOrder=['Sample.Sample_Number', 'Test.Test_Number','Result.Name', 'Result.Entry', 'Result.Entered_By']
for eachlab in lab:
    path='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//Import//'+inst+'//'
    archivedDestination='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//Import//'+inst+'//'
    importDest='//mxns.loc//shares//NA-Instruments//Prod//'+eachlab+'//impf//'
    files = os.listdir(path)
    for filename in files:
        if fnmatch.fnmatch(filename,'*.CSV'):
            datapath = path + str(filename)
            rawdf = pd.read_csv(datapath, skiprows=1)
            rawdf1 = rawdf[['Name  ', 'Protein  [%]']]
            rawdf12 = rawdf1.rename(index=str, columns={'Name  ': 'sample', 'Protein  [%]': 'Result'})
            rawdf12 = rawdf12.dropna()
            rawdf12['suffix'] = rawdf12['sample'].apply(extractsuffix)
            df_filtered = rawdf12[rawdf12['sample'] != 'orch lvs	']
            df_filtered.fillna(value=pd.np.nan, inplace=True)
            df_filtered = df_filtered.dropna()
            df_filtered = df_filtered[df_filtered['sample'] != 'orch lvs']
            # define the Analysis
            df_filtered['analysis'] = df_filtered['suffix'].apply(analysisdef)
            df_filtered['resultname'] = df_filtered['suffix'].apply(resultType)
            df_filtered['replicate_count'] = df_filtered['sample'].apply(getreplicate)
            dfResult = df_filtered[df_filtered['sample'].map(len) <= 11]
            dfResult = dfResult[dfResult['sample'].map(len) == 9]
            dfResult['sample_number'] = dfResult['sample'].apply(formatSampleNumber)
            dfResult.drop('sample', axis=1, inplace=True)
            dfResult = dfResult.astype({"sample_number": int})
            dfResult = dfResult.astype({"replicate_count": int})
            sampleNumbersList = list(dfResult['sample_number'].unique())
            databasedf = getDataFromDB(sampleNumbersList)
            merged_df = dfResult.merge(databasedf, how='inner', on=['sample_number', 'analysis', 'replicate_count'])
            merged_df['Result.Entered_By'] = 'IMPORTER'
            dfResult = merged_df.rename(index=str,
                                        columns={'sample_number': 'Sample.Sample_Number', 'resultname': 'Result.Name',
                                                 'Result': 'Result.Entry', 'test_number': 'Test.Test_Number'})
            dfResult = dfResult[colsOrder]
            writeFile(dfResult, eachlab, inst, archivedDestination, importDest, datapath, filename)
        else:
            continue
    print("Process Done")
