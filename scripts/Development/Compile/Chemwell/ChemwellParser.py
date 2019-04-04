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

# Default Parameters setting
path='//mxns.loc//shares//NA-Instruments//Prod//Crete//Import//Chemwell//RawFiles//'
pathrawfiles='//mxns.loc//shares//NA-Instruments//Prod//Crete//Import//Chemwell//RawFiles//'
archivedDestination='//mxns.loc//shares//NA-Instruments//Prod//Crete//Import//Chemwell//Archived//'
importDest='//mxns.loc//shares//NA-Instruments//Prod//Crete//impf//'
colsOrder=['Sample.Sample_Number', 'Test.Analysis','Result.Name', 'Result.Entry', 'Result.Entered_By']
lab='CRE'
inst='Chemwell'

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
	dataxl111 = dataxl111.replace('Abs', 'Absorbance')
	dataxl111 = dataxl111.replace('Conc', 'Gliadin (Component of Gluten)')
	dataxl111 = dataxl111.rename(index=str, columns={'Sample': 'Sample.Sample_Number', 'variable': 'Result.Name',
													 'value': 'Result.Entry'})
	dataxl111['Test.Analysis'] = 'ALLERGLIAD'
	dataxl111['Result.Entered_By'] = 'IMPORTER'
	dataxl111 = dataxl111.sort_values(by=['Sample.Sample_Number'])
	colsOrder = ['Sample.Sample_Number', 'Test.Analysis', 'Result.Name', 'Result.Entry', 'Result.Entered_By']
	dataxl111 = dataxl111.astype({"Sample.Sample_Number": int})
	timestr = time.strftime("%Y%m%d%H%M%S")
	FileName = importDest + lab+'_'+inst+'_' + timestr + '.csv'
	dataxl111.to_csv(FileName, index=False, encoding="utf-8",quotechar='"',quoting=csv.QUOTE_NONNUMERIC) # write  out the file to be imported
	destination = archivedDestination + name[:-4] + '_raw.ARK' # Backup the processed file
	os.rename(datapathxl, destination)