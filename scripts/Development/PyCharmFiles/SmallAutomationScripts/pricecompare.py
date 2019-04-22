import pandas as pd
import numpy as np
import os, fnmatch

listOfFiles = os.listdir('.')
csvpattern = "*.csv"
xlxspattern = "*.xlsx"
limsFname=''
JDEFname=''

for entry in listOfFiles:
    if fnmatch.fnmatch(entry, csvpattern):
        limsFname=entry
    if fnmatch.fnmatch(entry, xlxspattern):
        JDEFname=entry

#LIMS
dflims = pd.read_csv(limsFname)
dflims['type'], dflims['testCodeL'] = dflims['Pricing Rule : '].str.split(':', 1).str
col_list_lims = ['type', 'testCodeL','Disc','Price']
dflims1 = dflims[col_list_lims]
dflims2 = dflims1.loc[dflims1['type'] == 'Analysis ']
dflims2['testCodeL']=dflims2['testCodeL'].str.strip()
dflims2.round(2)


#JDE
dfjde = pd.read_excel(JDEFname)
col_list_jde=['Item Grp 1 Description','Factor Value Numeric']
dfjde1 = dfjde[col_list_jde]
dfjde1.rename(columns={'Item Grp 1 Description': 'testCodeJ', 'Factor Value Numeric': 'PriceJDE'}, inplace=True)
dfjde1=dfjde1.sort_values(by=['testCodeJ'])
dfjde1 = dfjde1.reset_index(drop=True)
dfjde1['PriceJDE'].fillna(0, inplace=True)
dfjde1['testCodeJ']=dfjde1['testCodeJ'].str.strip()
dfjde1.round(2)

#Result
dfResult = dflims2.merge(dfjde1, left_on='testCodeL', right_on='testCodeJ', how='outer')
dfResult['pricematch'] = np.where((dfResult['Price'] == dfResult['PriceJDE']), 'Match', 'Difference')
dfResult= dfResult.loc[dfResult['pricematch'] == 'Difference']
opfname=limsFname[:-9]+'_Result.xlsx'
writer = pd.ExcelWriter(opfname)
dfResult.to_excel(writer,'Sheet1',index=False)
writer.save()
