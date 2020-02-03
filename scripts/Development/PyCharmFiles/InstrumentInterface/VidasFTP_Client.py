# imports
import xml.etree.cElementTree as ET
import pandas as pd
import os
import fnmatch
import time


# Fucntion to convert the the to XML format  -- Refer to the VIDAS BCI Connectivity format.
def dfToXMLStr(df):
    xml = []
    for index, row in df.iterrows():
        instrumentSectionId = str(row['instrumentSectionId'])
        patientIdentifier = str(row['patientIdentifier'])
        specimenIdentifier = str(row['specimenIdentifier'])
        testIdentifier = str(row['testIdentifier'])
        dilution = str(row['dilution'])
        testReqStr = '<request><patientInformation><patientIdentifier>'
        testReqStr = testReqStr + patientIdentifier + '</patientIdentifier></patientInformation><testOrder><specimen><specimenIdentifier>'
        testReqStr = testReqStr + specimenIdentifier + '</specimenIdentifier></specimen><test><universalIdentifier><testIdentifier>'
        testReqStr = testReqStr + testIdentifier + '</testIdentifier><dilution>' + dilution + '</dilution></universalIdentifier><instrumentSectionId>'
        testReqStr = testReqStr + instrumentSectionId + '</instrumentSectionId></test></testOrder></request>'
        xml.append(testReqStr)
    return ''.join(xml)


# Shoudld make the path dynamic.
path = 'C:\\GitHub\\Instrument-Interface\\scripts\\Development\\JupyterNotebooks\\SampleFiles_Instruments\\VIDAS\\'
files = os.listdir(path)
li = []
for filename in files:
    if fnmatch.fnmatch(filename, '*.CSV'):
        print(filename)
        datapath = path + str(filename)
        df = pd.read_csv(datapath, header=None)
        li.append(df)
rawdf = pd.concat(li, axis=0, ignore_index=True)
rawdf['instrumentSectionId'] = rawdf[2]
rawdf['patientIdentifier'] = rawdf[4]
rawdf['specimenIdentifier'] = rawdf[5]
rawdf['testIdentifier'] = rawdf[6]
rawdf['dilution'] = rawdf[7]
rawdf.drop(columns=[0, 1, 2, 3, 4, 5, 6, 7], inplace=True)
# VIDAS XML String
VidasDateTimeStamp = time.strftime("%Y%m%d_%H%M%S")
xmlHeaderString = '<?xml version="1.0" encoding="UTF-8"?>'
xmlHeaderString = xmlHeaderString + '<lisMessage xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="ASTM.xsd">'
xmlHeaderString = xmlHeaderString + '<header><version>1394-97</version><dateTime>' + VidasDateTimeStamp + '</dateTime></header>'
testRequestString = dfToXMLStr(rawdf)
vidasXMLFileString = xmlHeaderString + testRequestString + '</lisMessage>'

seqNumber = 0  # check the logic for seq number
VidasxmlFileName = 'bcinet_' + VidasDateTimeStamp + '_' +seqNumber
with open(VidasxmlFileName, "w") as f:
    f.write(ET.tostring(vidasXMLFileString))
