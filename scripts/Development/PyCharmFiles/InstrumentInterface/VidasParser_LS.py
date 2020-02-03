import pandas as pd
import os
import fnmatch
import numpy as np
import json
from datetime import datetime
import time
from threading import Thread
import csv

threads = []
basePath = '//mxns.loc//shares//NA-Instruments//VIDAS//VIDAS_LIMS//'
colsOrder = ['Sample.Sample_Number', 'Test.Analysis', 'Result.Name', 'Result.Entry', 'Result.Entered_By',
             'Result.Instrument']

os
# function to generate the result name
def generateResultname(testIdentifier, variable):
    if (variable == 'R2'):
        return str(testIdentifier)
    else:
        return str('OD' + str(testIdentifier))


# function to generate the result name
def generateResult(value,variable):
    if(variable =='R1'):
        if value in ('ABSENT','NEGATIVE'):
            return str('NEGATIVE')
        else:
            return str('SUSPECT')

# fucntion to Create a folder if it dosen't exsist.
def createFolderIfNotExsist(folderPath):
    if os.path.exists(folderPath):
        pass
    else:
        os.makedirs(folderPath)
        print('Folder Created:' + folderPath)


# Moce to ARK folder
def moveFiletoArkFolder(arkPath, sourcePath, fname):
    # checking the exsitnace of Archive directory and path
    createFolderIfNotExsist(arkPath)
    print("Source for moving" + sourcePath)
    print("dest for moving" + arkPath)
    print("ProcessName" + process.getName())
    arkDest = arkPath + fname[:-4] + '.bak'  # Backup the processed file
    os.rename(sourcePath, arkDest)
    time.sleep(1)


# write the file to importer folder
def writeFile(dframe, lab, instName, basePath):
    timestr = time.strftime("%Y%m%d%H%M%S")
    fname = instName + '_' + timestr + '.csv'
    importerFilePath = basePath + lab + '_IMPORTER//' + fname
    dframe.to_csv(importerFilePath, index=False, encoding="utf-8", quotechar='"',
                  quoting=csv.QUOTE_NONNUMERIC)  # write  out the file to be imported
    print('Importer File Generated' + fname)
    time.sleep(1)


# Function to Process Each Lab seperately
def processEachLabFiles(lab, labcount, labImporterPath, basePath):
    print('Process started for ' + lab)
    fileCounter = 0
    createFolderIfNotExsist(labImporterPath)
    li = []
    fileStatusFlag = False
    for each in range(1, labcount + 1):
        instName = lab + 'VIDAS' + str(each)
        sourceFolderPath = basePath + instName + '//'
        arkFolderPath = sourceFolderPath + 'Archive//' + datetime.now().strftime('%Y') + '//' + datetime.now().strftime(
            '%m_%d') + '//'
        # checking the exsitnace of importer path
        createFolderIfNotExsist(sourceFolderPath)
        files = os.listdir(sourceFolderPath)
        for filename in files:
            if fnmatch.fnmatch(filename, '*.CSV'):
                print('Processing Files for :' + lab)
                sourcefilePath = sourceFolderPath + str(filename)
                try:
                    rawdf = pd.read_csv(sourcefilePath, index_col=None, header=None)
                    rawdf['instrumentName'] = instName
                    li.append(rawdf)
                    print('Processed :' + filename)
                    moveFiletoArkFolder(arkFolderPath, sourcefilePath, filename)
                    fileStatusFlag = True
                    fileCounter = fileCounter + 1
                except:
                    continue
    if (fileStatusFlag):
        raw_data = pd.concat(li, axis=0, ignore_index=True)
        raw_data['specimenIdentifier'] = raw_data[0]
        raw_data['testIdentifier'] = raw_data[1]
        raw_data['R1'] = raw_data[5]
        raw_data['R2'] = raw_data[2]
        raw_data['patientIdentifier'] = raw_data[4]
        raw_data = raw_data[np.isfinite(raw_data["patientIdentifier"])]
        raw_data['patientIdentifier'] = raw_data['patientIdentifier'].astype(int)
        raw_data["samplenumber"] = raw_data["patientIdentifier"].map(str) + raw_data["specimenIdentifier"].str[-6:]
        dfResult = pd.melt(raw_data,
                           id_vars=['instrumentName', 'specimenIdentifier', 'testIdentifier', 'samplenumber'],
                           value_vars=['R1', 'R2'])
        dfResult['Result.Name'] = dfResult.apply(lambda x: generateResultname(x.testIdentifier, x.variable), axis=1)
        dfResult['Result.Entry'] = dfResult.apply(lambda x: generateResult(x.value,x.variable), axis=1)
        dfResult = dfResult.sort_values(by=['samplenumber'])
        dfResult['Test.Analysis'] = 'VIDAS'
        dfResult['Result.Entered_By'] = lab + '_VIDAS_IMPORTER'
        dfResult = dfResult.rename(index=str,
                                   columns={'samplenumber': 'Sample.Sample_Number', 'value': 'Result.Entry',
                                            'instrumentName': 'Result.Instrument'})
        dfResult = dfResult['Sample.Sample_Number'].astype(int)
        dfResult = dfResult[colsOrder]
        writeFile(dfResult, lab, instName, basePath)


# get the data from the Config fle
with open('.\configFiles\VIDASParserConfig.json', 'r') as f:
    config = json.loads(f.read())
    for d1 in config:
        lab = d1
        labcount = int(config[d1]['count'])
        labImporterPath = config[d1]['importerPath']
        # processEachLabFiles(lab, labcount, labImporterPath, basePath)
        process = Thread(target=processEachLabFiles, args=[lab, labcount, labImporterPath, basePath])
        process.start()
        print('Thread Statretd for' + lab + "Porcess:" + process.getName())
        threads.append(process)

for process in threads:
    process.join()

print("All threads completed End of Process")
