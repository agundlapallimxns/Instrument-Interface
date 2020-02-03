import xml.etree.cElementTree as et
import pandas as pd
import os
import fnmatch
import sqlalchemy as sql
import numpy as np

colsOrder = ['Sample.Sample_Number', 'Test.Analysis', 'Result.Name', 'Result.Entry', 'Result.Entered_By',
             'Result.Instrument']


# Generate The Result name
def generateResultname(testIdentifier, variable):
    if (variable == 'R2'):
        return str(testIdentifier)
    else:
        return str('OD' + str(testIdentifier))
