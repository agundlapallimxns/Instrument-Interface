# Import Functions
import pandas as pd
import os
import time
import json
import csv
import sqlalchemy as sql
dbHost = 'mxnunx41'
dbPort = '1521'
dbUser = 'lims'
dbPassword = 'Mumwit63'
dbName = 'hwi1'
dburl = 'oracle://'


# get Data from DB
def getDataFromDB(sampleNumbersList):
    dburl = 'oracle://' + dbUser + ':' + dbPassword + '@' + dbHost + ':' + dbPort + '/' + dbName
    engine = sql.create_engine(dburl)
    resultdf = pd.DataFrame()
    sampleNumbrStr = (', '.join("'" + str(item) + "'" for item in sampleNumbersList))
    queryText = "SELECT sample_number, analysis, REPLICATE_COUNT,TEST_NUMBER FROM test"
    queryText = queryText + " WHERE SAMPLE_NUMBER IN (" + sampleNumbrStr + ") AND analysis IN ('ALLERGLIAD')"
    conn = engine.connect()
    resultdf = pd.read_sql(queryText, engine)
    conn.close()
    return resultdf


# write the file
def writeFile(dframe, lab, inst, archDestination, importDest, datapath, fname):
    timestr = time.strftime("%Y%m%d%H%M%S")
    FileName = importDest + lab + '_' + inst + '_' + timestr + '.csv'
    # write file to importer folder
    dframe.to_csv(FileName, index=False, encoding="utf-8", quotechar='"',
                  quoting=csv.QUOTE_NONNUMERIC)  # write  out the file to be imported
    destination = archDestination + fname[:-4] + '_' + timestr + '.bak'  # Backup the processed file
    os.rename(datapath, destination)
    print("Processed File: " + fname[:-4])
    time.sleep(1)


# Default Parameters setting
with open('./ConfigFiles/ChemwellConfig.json', 'r') as f:
    config = json.load(f)
print(config)
labs = config['lab']
analysis = config['analysis']
inst = config['inst']
mode = config['mode']
colsOrder = ['Sample.Sample_Number', 'Test.Test_Number', 'Result.Name', 'Result.Entry', 'Result.Entered_By',
             'Result.Instrument']

for eachlab in labs:
    path = '//mxns.loc//shares//NA-Instruments//Prod//' + eachlab + '//Import//' + inst + '//RawFiles//'
    archivedDestination = '//mxns.loc//shares//NA-Instruments//Prod//' + eachlab + '//Import//' + inst + '//Archived//'
    importDest = '//mxns.loc//shares//NA-Instruments//Prod//' + eachlab + '//impf//'
    print(path)
    if os.path.exists(path):
        pass
    else:
        os.makedirs(path)
    files = os.listdir(path)
    for name in files:
        datapath = path + str(name)
        dataxl = pd.read_excel(datapath)  # read the Excel file from the instrument
        dataxl1 = dataxl.drop_duplicates(subset='Sample', keep='first')
        dataxl11 = dataxl1[dataxl1['Sample'].map(len) == 11]  # Subset only the valid sample numbers
        collist = ['Sample', 'Abs', 'Conc']  # define the required columns for the import files
        dataxl111 = dataxl11[collist]  # Subset the required columns for the import files
        dataxl111['Sample'] = dataxl111['Sample'].str[2:]  # format the sample numbers
        dataxl111 = pd.melt(dataxl111, id_vars=['Sample'], value_vars=['Abs', 'Conc'])  # Pivot the samples numbers
        sampleNumbersList = list(dataxl111['Sample'].unique())
        databasedf = getDataFromDB(sampleNumbersList)
        databasedf = databasedf.rename(index=str, columns={'sample_number': 'Sample'})
        dataxl111 = dataxl111.astype({"Sample": int})
        dfResult = pd.merge(dataxl111, databasedf, on='Sample')
        dfResult = dfResult.replace('Abs', 'Absorbance')
        dfResult = dfResult.replace('Conc', 'Gliadin (Component of Gluten)')
        dfResult = dfResult.rename(index=str, columns={'Sample': 'Sample.Sample_Number', 'variable': 'Result.Name',
                                                       'value': 'Result.Entry', 'test_number': 'Test.Test_Number'})
        dfResult['Result.Entered_By'] = 'IMPORTER'
        dfResult['Result.Instrument'] = str(inst)
        dfResult = dfResult.sort_values(by=['Sample.Sample_Number'])
        dfResult = dfResult.astype({"Sample.Sample_Number": int})
        dfResult = dfResult[colsOrder]
        writeFile(dfResult, eachlab, inst, archivedDestination, importDest, datapath, name)
print("Process Done!")