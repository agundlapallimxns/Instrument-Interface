import pandas as pd
import os
import fnmatch
import numpy as np
from datetime import datetime
import time
import csv
import sys


basePath = '//mxns.loc//shares//NA-Instruments//VIDAS//VIDAS_LIMS//'
colsOrder = ['Sample.Sample_Number', 'Test.Analysis', 'Test.Replicate_Count','Result.Name', 'Result.Entry', 'Result.Entered_By']

# function to generate the result name
def generateResultname(testIdentifier, variable):
    if (variable == 'R2'):
        return str(testIdentifier)
    else:
        return str('OD' + str(testIdentifier))


# function to generate the result name
def generateResult(value):
    if value in ('ABSENT','NEGATIVE'):
        return str('NEGATIVE')
    else:
        return str('SUSPECT')


# function to generate the result name
def generateReplicateCnt(value):
    retVal= ord(value) - 64
    return  retVal

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
    arkDest = arkPath + fname[:-4] + '.bak'  # Backup the processed file
    os.rename(sourcePath, arkDest)
    time.sleep(1)


# write the file to importer folder
def writeFile(dframe, lab, instName, basePath):
    timestr = time.strftime("%Y%m%d%H%M%S")
    fname = instName + '_' + timestr + '.csv'
    importerPath = basePath + lab + '_IMPORTER//'
    createFolderIfNotExsist(importerPath)
    importerFilePath = importerPath + fname
    dframe.to_csv(importerFilePath, index=False, encoding="utf-8", quotechar='"',
                  quoting=csv.QUOTE_NONNUMERIC)  # write  out the file to be imported
    print('Importer File Generated' + fname)
    time.sleep(1)


# Function to Process Each Lab seperately
def processEachLabFiles(lab, labcount, labImporterPath, basePath):
    print('Process started for ' + lab)
    fileCounter = 0
    #createFolderIfNotExsist(labImporterPath)
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
                    moveFiletoArkFolder(arkFolderPath, sourcefilePath, filename)
                    continue
    if (fileStatusFlag):
        raw_data = pd.concat(li, axis=0, ignore_index=True)
        raw_data['specimenIdentifier'] = raw_data[0]
        raw_data['testIdentifier'] = raw_data[1]
        raw_data['R1'] = raw_data[5]
        raw_data['Rtext'] = raw_data[2]
        raw_data['R2'] = raw_data.apply(lambda x: generateResult(x.Rtext), axis=1)
        raw_data['patientIdentifier'] = raw_data[4]
        raw_data = raw_data[np.isfinite(raw_data["patientIdentifier"])]
        raw_data['patientIdentifier'] = raw_data['patientIdentifier'].astype(int)
        raw_data["samplenumber"] = raw_data["patientIdentifier"].map(str) + raw_data["specimenIdentifier"].str[-6:]
        raw_data["replicateStr"] = raw_data["specimenIdentifier"].str[-7:-6]
        raw_data["replicatecnt"] = raw_data.apply(lambda x: generateReplicateCnt(x.replicateStr), axis=1)
        dfResult = pd.melt(raw_data,
                           id_vars=['instrumentName', 'specimenIdentifier', 'testIdentifier', 'samplenumber','replicatecnt'],
                           value_vars=['R1', 'R2'])
        dfResult['Result.Name'] = dfResult.apply(lambda x: generateResultname(x.testIdentifier, x.variable), axis=1)

        dfResult['Test.Analysis'] = 'VIDAS'
        dfResult['Result.Entered_By'] = 'VIDASBCI'
        dfResult = dfResult.rename(index=str,
                                   columns={'samplenumber': 'Sample.Sample_Number', 'value': 'Result.Entry','replicatecnt' :'Test.Replicate_Count',
                                            'instrumentName': 'Result.Instrument'})
        dfResult = dfResult.astype({"Sample.Sample_Number": int})
        dfResult = dfResult.sort_values(by=['Sample.Sample_Number','Result.Name'])
        dfResult = dfResult[colsOrder]
        writeFile(dfResult, lab, instName, basePath)

lab = sys.argv[1]
labcount = int(sys.argv[2])
labImporterPath = ""
processEachLabFiles(lab, labcount, labImporterPath, basePath)

'''
if __name__ == '__main__':
    # get the data from the Config fle
    with open('.\configFiles\VIDASParserConfig.json', 'r') as f:
        config = json.loads(f.read())
        processes = []
        for d1 in config:
            lab = config[d1]['labCode']
            labcount = int(config[d1]['count'])
            print(lab,labcount)
            labImporterPath = basePath
            p = mp.(target=processEachLabFiles, args=(lab, labcount, labImporterPath, basePath))
            p = mp.Process(target=processEachLabFiles, args=(lab, labcount, labImporterPath, basePath))
            p.start()
            processes.append(p)
        for process in processes:
            process.join()
            #processEachLabFiles(lab, labcount, labImporterPath, basePath)
'''