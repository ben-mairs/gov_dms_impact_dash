# -*- coding: utf-8 -*-
"""
Created on Fri May  6 12:57:05 2022

@author: MairsB
"""

#!/usr/bin/env python
# coding: utf-8

# # Final Impact Dashboard Data Assembly

# In[1]:

# time how long script takes
import time
startTime = time.time()

# import standard data manipulation libraries
import pandas as pd
import numpy as np

# import other libraries as necessary

# In[2]:

# ## Import, Concat, Export old FYs

# ### County-level Data

df_county18_19 = pd.read_excel('Fiscal Years/County Level FY 2018-2019.xlsx')

df_county19_20 = pd.read_excel('Fiscal Years/County Level FY 2019-2020.xlsx')

df_county20_21 = pd.read_excel('Fiscal Years/County Level FY 2020-2021.xlsx')

df_county21_22 = pd.read_excel('Fiscal Years/County Level FY 2021-2022.xlsx')

# can't have duplicate columns when using concat
# len(df_county20_21.columns) == len(set(df_county20_21.columns))

# df_county20_21.columns

# can't have duplicate columns when using concat
# len(df_county19_20.columns) == len(set(df_county19_20.columns))

# df_county19_20.columns


# can't have duplicate columns when using concat
# len(df_county20_21.columns) == len(set(df_county20_21.columns))


# df_county20_21.columns

# can't have duplicate columns when using concat
# len(df_county21_22.columns) == len(set(df_county21_22.columns))

# df_county21_22.columns

# concat years of data vertically
years = [df_county18_19, df_county19_20, df_county20_21, df_county21_22]
df_county = pd.concat(years, ignore_index=True, axis=0)

# df_county.head()

# not really sure when I picked up this piece of code
try:
    df_county.drop(['Unnamed: 0'], axis=1, inplace=True)
except KeyError:
    pass

# create excel files for import to Tableau
df_county.to_excel('needed for tableau/CountyLevelData_DMS.xlsx')

# df_county.dtypes

# In[3]:

# ### City-level Data

df_city18_19 = pd.read_excel('Fiscal Years/City Level FY 2018-2019.xlsx')

# show which metrics we have, ready for dashboarding
df_city18_19.dtypes

df_city19_20 = pd.read_excel('Fiscal Years/City Level FY 2019-2020.xlsx')

# show which metrics we have, ready for dashboarding
df_city19_20.dtypes

df_city20_21 = pd.read_excel('Fiscal Years/City Level FY 2020-2021.xlsx')

# show which metrics we have, ready for dashboarding
# df_city20_21.dtypes

# don't need category
try:
    df_city20_21.drop(['Category'], axis=1, inplace=True)
except:
    pass

df_city21_22 = pd.read_excel('Fiscal Years/City Level FY 2021-2022.xlsx')

df_city21_22.dtypes

# can't have duplicate columns when using concat
# len(df_city21_22.columns) == len(set(df_city21_22.columns))

# can't have duplicate columns when using concat
# len(df_city20_21.columns) == len(set(df_city20_21.columns))

# can't have duplicate columns when using concat
# len(df_city19_20.columns) == len(set(df_city19_20.columns))

# can't have duplicate columns when using concat
# len(df_city18_19.columns) == len(set(df_city18_19.columns))

# concat years of data vertically
years = [df_city18_19, df_city19_20, df_city20_21, df_city21_22]
df_city = pd.concat(years, ignore_index=True, axis=0)

# df_city.dtypes

# df_city.FY.value_counts()

df_city['FY'] = df_city['FY'].astype(str)

# df_city.head()

try:
    df_city.drop(['Unnamed: 0'], axis=1, inplace=True)
except:
    pass

# export to excel file for tableau
df_city.to_excel('needed for tableau/CityLevelData_DMS.xlsx')




# In[4]:

# ## Time

executionTime = (time.time() - startTime)
print('Execution time: {} minutes, {} seconds: '.format(str(round(executionTime/60)), str(round(executionTime%60,2))))





