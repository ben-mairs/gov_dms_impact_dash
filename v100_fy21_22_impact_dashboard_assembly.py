"""
Created on Fri May  6 09:18:54 2022

@author: MairsB
"""

# !/usr/bin/env python
# coding: utf-8

# In[1]:
# # FY 21-22 Impact Dashboard Data Assembly

# ## Import Libraries and Files

# import standard data manipulation libraries
import pandas as pd
import numpy as np
# import other libraries as necessary


# time how long script takes
import time
startTime = time.time()

# Read in the geographic file of cities,counties and legislative districts
filepath = 'Geo Files for Merging/GeoFile-Copy1.csv'
df_geo_city = pd.read_csv(filepath)

# drop duplicates with exact same city name
df_geo_city.drop_duplicates(subset='City', keep='first', inplace=True)

# initiliaze city data frames
df_city21_22 = pd.DataFrame(data=df_geo_city)
# we will join required datasets to this df


# get separate county dataset by getting list of FL counties
# tableau already has shape info for counties, so we only need the name
df_geo_county = df_geo_city[['County']].drop_duplicates(keep='first')

# initiliaze county data frame
df_county21_22 = pd.DataFrame(data=df_geo_county)
# we will join required datasets to this df

# check for correct number of counties
# len(df_county21_22)

# import data to attach city names to zip codes
filepath = 'Geo Files for Merging/flzips.xlsx'

# read & coerce zip code to str
df_geo_zip = pd.read_excel(filepath, dtype={'Zip Code': str})

# city names will be upper case
df_geo_zip['City'] = df_geo_zip['City'].str.upper()


# Function to count missing values for each column, took from stack overflow
def missing_data(data):
    # Count number of missing value in a column
    total = data.isnull().sum()

    # Get Percentage of missing values
    percent = (data.isnull().sum()/data.isnull().count()*100)
    temp = pd.concat([total, percent], axis=1, keys=['Total', 'Percent(%)'])

    # Create a Type column, that indicates the data-type of the column.
    types = []
    for col in data.columns:
        dtype = str(data[col].dtype)
        types.append(dtype)
    temp['Types'] = types

    return(np.transpose(temp))


# correct misspelled city names to match df_geo_city
# if city names don't match, data will be lost in merge
# build dict of replacement to make
# closest city in same district was chosen if city was not in our GeoFile
to_replace = {}

# add to dict frm file
with open('other/to_replace.txt', 'r') as f:
    # text file format is incorrectname:correctname, parse that into a dict
    # dict is accepted as an argument in the .replace method in pd
    to_replace = {i: j for line in f for (i, j) in
                  [line.strip('\n').split(':')]}

# this dict was built by differencing lists of df_geo cities and dms data with:
# list(set(<dataframe_column>.to_list()).difference(list(set(df_geo_city['City'].to_list()))))
# this generates a unique list of cities that don't match df_geo
# can also be used for zip codes by replacing the pandas columns
# you have to manually put the 'correct' names in though
# 'correct' meaning it must be one of the 505 in df_geo_city

# ambiguous or other state's cities must be dropped
# same as to_replace, kept running file
to_drop = []
# grab from file
with open('other/to_drop.txt', 'r') as f2:
    # list comp so we can drop cities in this list
    to_drop = [k for line in f2 for k in [line.strip('\n')]]


# In[2]:

# ## FY 2021-2022

# ### DivTel
# Metrics: Internet Circuits, Value of Internet Circuits, 911 Grants,
# 911 Circuits, SLERS Towers, SUNCOM Local/Digital Phone Charges,
# SUNCOM VOIP Charges

# #### Internet Circuits
# Level: City

# Metrics: Number of Internet Circuits, Value of Internet Circuits
# import divtel files
filepath = 'Divisions/Divtel- CSAB- Internet Circuits/FY2021-2022/DivTel CSAB_Inventory_Retail.xlsx'
df_csab21_22 = pd.read_excel(filepath, header=3)

# df_csab21_22.columns

# check for missing data
# missing_data(df_csab21_22)

# Get the necessary columns from CSAB data
df_csab21_22 = df_csab21_22[['Inventory ID', 'Site City', 'Retail']]

df_csab21_22['Site City'] = df_csab21_22['Site City'].str.upper()

# make replacements to ensure merge with df_geo_city is smooth
df_csab21_22.replace(to_replace=to_replace, inplace=True)

# drop known issue cities
df_csab21_22 = df_csab21_22[df_csab21_22['Site City'].isin(to_drop) == False]

# aggregate CSAB data to the city level in preparation for merge
df_csab21_22_agg = df_csab21_22.groupby(df_csab21_22['Site City'
                                                     ].str.upper()
                                        ).agg(
                                              {'Inventory ID': 'nunique',
                                               'Retail': 'sum'
                                               }
                                              ).reset_index()

# rename columns
df_csab21_22_agg.rename(columns={'Inventory ID': 'Internet Circuits',
                                 'Retail': 'Value of Internet Circuits',
                                 'Site City': 'City'}, inplace=True)

# check sum of assets and retail value against DB
# df_csab21_22_agg['Internet Circuits'].sum()

# check sum of assets and retail value against DB
# df_csab21_22_agg['Value of Internet Circuits'].sum()

# check for cities that won't merge cleanly
# list(set(df_csab21_22_agg['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# Merge csab data with master set
df_city21_22 = pd.merge(left=df_city21_22, right=df_csab21_22_agg,
                        left_on='City', right_on='City',
                        how='left')

# df_city21_22.head()

# In[3]:

# #### 911 Grants
# Level: County Level

# Metrics: Grants Awarded

# import grants information
df_grants21_22 = pd.read_excel('Divisions/Divtel - 911 Grants/FY2021-2022/DivTel 911 Grants FY 21-22.xlsx')

# df_grants21_22.columns

# df_grants21_22.head()

# Grants Data
df_grants21_22 = df_grants21_22[['County Name', 'FinalAward']]

# rename
df_grants21_22.rename(columns={'FinalAward': 'Grants Awarded',
                               'County Name': 'County'
                               },
                      inplace=True)

# add county to county name
df_grants21_22['County'] = df_grants21_22['County'] + ' County'

# group and agg to city level
df_grants21_22_agg = df_grants21_22.groupby(['County'
                                             ]).agg({
                                                     'Grants Awarded':
                                                     'sum'
                                                     }).reset_index()

# check sum of grants against dashboard
# df_grants21_22_agg['Grants Awarded'].sum()

# see if any counties won't make the merge
# list(set(df_grants21_22_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# Merge grants and e-rate disbursements to generate a county-level data frame
df_county21_22 = pd.merge(left=df_county21_22, right=df_grants21_22_agg,
                          left_on='County', right_on='County', how='left')

# df_county21_22.head()

# In[3.5]:

# #### 911 Disbursements
# Level: County Level

# Metrics: E911 Disbursements

# import grants information
df_disb21_22 = pd.read_excel('C:\\Users\\mairsb\\OneDrive - Florida Department of Management Services\\07-Strategic Planning\\02-Projects\\Government Impact Dashboard Project\\Implementation\\Data Assembly\\Divisions\\DivTel - 911 Disbursements\\FY2021-2022\\County Monthly disbu - Copy.xlsx')


# df_disb21_22.columns

# df_disb21_22.tail(55)


# slice of last few rows that have totals
df_disb21_22 = df_disb21_22.iloc[:68,:]


# only get total columns, others are unneccessary
df_disb21_22 = df_disb21_22[['Unnamed: 0',
                             'fy21/22 Total', 'fy21/22 Total.1',
                             'fy21/22 Total.2', 'fy21/22 Total.3',
                             'fy21/22 Total.4'
                             ]]

# change column names to what's in the first row
df_disb21_22.columns = df_disb21_22.iloc[0]

# slice off the first row

df_disb21_22 = df_disb21_22[1:]

# sum various categories of spend
df_disb21_22['E911 Disbursements'] = (
                                     df_disb21_22['Wireless'] +
                                     df_disb21_22['Nonwireless'] +
                                     df_disb21_22['Prepaid Wireless'] +
                                     df_disb21_22['Supplemental'] +
                                     df_disb21_22['Special']
                                     )

# add 'County' to county field for merge prep
df_disb21_22['County'] = df_disb21_22['County'] + ' County'

# check to make sure there are no misspellings
# list(set(df_disb21_22['County'].to_list()).difference(df_geo_county['County'].to_list()))

# group and agg is not really necessary, but it can't hurt

df_disb21_22_agg = df_disb21_22.groupby(['County']).agg(
                                                        {'E911 Disbursements':
                                                         'sum'
                                                         }
                                                          ).reset_index()
# we're good, go ahead and merge
df_county21_22 = pd.merge(left=df_county21_22, left_on='County',
                          right=df_disb21_22_agg, right_on='County',
                          how='left')
# df_county21_22.head()

# In[4]:

# #### 911 Circuits
# Metrics: 911 Circuits

# import psap data
df_psap21_22 = pd.read_excel('Divisions/Divtel- PSAP/FY2021-2022/DivTel PSAP_Impacts_YTD.xlsx')

# add county to county name
# city data is too sparse to use
df_psap21_22['County'] = df_psap21_22['County'] + ' County'

# df_psap21_22.columns

# need county and 1 column to get count
df_psap21_22 = df_psap21_22[['County', 'PSAP name ']]

# replace county names that didn't make it into the merge
df_psap21_22.replace(to_replace=to_replace, inplace=True)

# group and agg to county level for merge at bottom
df_psap21_22_agg = df_psap21_22.groupby(['County']
                                        ).agg({
                                               'PSAP name ': 'count'
                                               }).reset_index()

# rename to desired metric name
df_psap21_22_agg.rename(columns={'PSAP name ': '911 Circuits'}, inplace=True)
# ready for merge

# check against dashboard to make sure we don't lose any
# df_psap21_22_agg['911 Circuits'].sum()

# check to see if any counties will get left out of the merge
# list(set(df_psap21_22_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# merge with psap county data
df_county21_22 = pd.merge(left=df_county21_22, left_on='County',
                          right=df_psap21_22_agg, right_on='County',
                          how='left')

# df_county21_22.head()

# In[5]:

# #### SLERS Towers
# Metrics: Number of SLERS Towers

# import SLERS towers data
df_towers = pd.read_csv('Divisions/Divtel- SLERS/FY2021-2022/DivTel SLERS Towers_11_10_2020.csv')

# list columns, won't need all of them
df_towers.columns

# only need county and 1 column for counts, use name (no blanks)
df_towers = df_towers[['County', 'Name']]

# can't get these counties to cooperate
df_towers.replace({'Dade County': 'Miami-Dade County',
                   'Saint Lucie County': 'St. Lucie County',
                   'De Soto County': 'Desoto County'},
                  inplace=True)

# group by county and count up number of names to get county count
df_towers_agg = df_towers.groupby(['County']).agg({
                                                   'Name': 'count'
                                                   }).reset_index()

# rename to desired metric name
df_towers_agg.rename(columns={'Name': 'SLERS Towers'},
                     inplace=True)

# add county to names of counties for merge
df_towers_agg['County'] = df_towers_agg['County'] + ' County'

# check # of towers against dashboard
# df_towers_agg['SLERS Towers'].sum()

# replace counties that won't make the merge
df_towers_agg.replace(to_replace=to_replace, inplace=True)

# check to see if any counties will get left out of the merge
# list(set(df_towers_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# merge county with with SLERS towers info
df_county21_22 = pd.merge(left=df_county21_22, left_on='County',
                          right=df_towers_agg, right_on='County',
                          how='left')

# df_county21_22.head()

# In[6]:

# #### SUNCOM -- Local & Digital
# Level: City
# Metrics: Local/Digital Phone Charges

# import first lot of suncom data
df_sun1 = pd.read_excel('Divisions/Divtel- Suncom/FY2021-2022/DivTel SUNCOM phone spend.xlsx')

# df_sun1.columns

df_sun1 = df_sun1[['CITY', 'CHARGES']]

# drop rows where city is na
df_sun1.dropna(axis=0, how='any', subset=(['CITY']), inplace=True)

# strip spaces from left side
df_sun1['CITY'] = df_sun1['CITY'].str.lstrip(' ')

# capitalize city
df_sun1['CITY'] = df_sun1['CITY'].str.upper()

# rename city to match our merging data
df_sun1.rename(columns={'CITY': 'City',
                        'CHARGES': 'Local/Digital Phone Charges'},
               inplace=True)

# check for cities that didn't make it
# list(set(df_sun1['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# drop known-issue cities
df_sun1 = df_sun1[df_sun1['City'].isin(to_drop) == False]

# replace known city name issues before group/agg
df_sun1.replace(to_replace=to_replace, inplace=True)

# group and agg to city level
df_sun1_city = df_sun1.groupby(['City']).agg({'Local/Digital Phone Charges':
                                              'sum'}).reset_index()

# check sum of phone charges against dashboard
# df_sun1['Local/Digital Phone Charges'].sum()


# #### SUNCOM -- VOIP
# Level: City
# 
# Metrics: Internet Phone Charges

# read in Voice Over Internet Protocol (VOIP) data
df_sun2 = pd.read_excel('Divisions/Divtel- Suncom/FY2021-2022/DivTel VOIP.xlsx')
# df_sun2.columns

# drop rows where city is na
df_sun2.dropna(axis=0, how='any', subset=(['CITY']), inplace=True)

# strip spaces from left side
df_sun2['CITY'] = df_sun2['CITY'].str.lstrip(' ')

# capitalize city
df_sun2['CITY'] = df_sun2['CITY'].str.upper()

# rename city to match our merging data
df_sun2.rename(columns={'CITY': 'City',
                        'CHARGES': 'Internet Phone Charges'},
               inplace=True)

# check for cities that didn't make it
# list(set(df_sun2['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# reaplce known city name issues before group/agg
df_sun2.replace(to_replace=to_replace, inplace=True)

# drop known-issue cities
df_sun2 = df_sun2[df_sun2['City'].isin(to_drop) == False]

# group and agg to the city level for merging
df_sun2_city = df_sun2.groupby(['City']).agg({'Internet Phone Charges':
                                              'sum'}).reset_index()

# check sum of phone charges against dashboard
# df_sun2['Internet Phone Charges'].sum()

# merge with sun1 to get all suncom data together
df_sun21_22 = pd.merge(left=df_sun1_city, left_on='City',
                       right=df_sun2_city, right_on='City',
                       how='outer')

# merge with DivTel SUNCOM data
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_sun21_22, right_on='City',
                        how='left')

# df_city21_22.head()

# In[7]:

# ### DSGI
# Metrics: Total Pharma Spend, Total Medical Spend

# 4 sheets with every combination of (medical,pharmacy) and (retirees, emps)
# skip footer =1 because DSGI, so kindly, included grand totals
# medical retirees
# home zip need to be a string, not a number or leading zeroes will be dropped
# causing issue with the merge, so we do a dtype=
df_himis0 = pd.read_excel('Divisions/DSGI-HIMIS/FY2021-2022/May 2022/July 2021 to Mar 2022 Claim - Copy.xlsx',
                          sheet_name='Medical Retiree',
                          names=['Home Zip', 'E - Enrollee', 'Dependents'],
                          skipfooter=1, dtype={'Home Zip': str})
# df_himis0.columns

# get total spend for medical retirees
df_himis0['Total Spend-MR'] = (df_himis0['E - Enrollee'] +
                               df_himis0['Dependents'])

# we only want the total, I think
df_himis0 = df_himis0[['Home Zip', 'Total Spend-MR']]

# medical employees
# home zip need to be a string, not a number or leading zeroes will be dropped
# causing issue with the merge, so we do dtype= again
df_himis1 = pd.read_excel('Divisions/DSGI-HIMIS/FY2021-2022/April 2022/July 2021 to Feb 2022 Claims Final.xlsx',
                          sheet_name='Medical Active',
                          skipfooter=1, dtype={'Home Zip': str})


# get total spend for medical employees
df_himis1['Total Spend-ME'] = (df_himis1['E - Enrollee'] +
                               df_himis1['Dependent'])

# we only want the total, I think
df_himis1 = df_himis1[['Home Zip', 'Total Spend-ME']]

# merge the 2 medical sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_medical = pd.merge(left=df_himis0, left_on='Home Zip',
                      right=df_himis1, right_on='Home Zip',
                      how='outer')

# total medical spend: retirees and employees, and their dependents
df_medical['Total Medical Spend'] = (df_medical['Total Spend-ME'] +
                                     df_medical['Total Spend-MR'])

# pharma retirees
df_himis2 = pd.read_excel('Divisions/DSGI-HIMIS/FY2021-2022/April 2022/July 2021 to Feb 2022 Claims Final.xlsx',
                          sheet_name='Pharmacy Retiree',
                          skipfooter=1, dtype={'Home Zip': str})

# add
df_himis2['Total Spend-PR'] = (df_himis2['E - Enrollee'] +
                               df_himis2['Dependents'])

# we only want the total, I think
df_himis2 = df_himis2[['Home Zip', 'Total Spend-PR']]

# pharma employees
df_himis3 = pd.read_excel('Divisions/DSGI-HIMIS/FY2021-2022/April 2022/July 2021 to Feb 2022 Claims Final.xlsx',
                          sheet_name='Pharmacy Active',
                          skipfooter=1, dtype={'Home Zip': str})

df_himis3['Total Spend-PE'] = (df_himis3['E - Enrollee'] +
                               df_himis3['Dependent'])

# we only want the total, I think
df_himis3 = df_himis3[['Home Zip', 'Total Spend-PE']]

# merge the 2 pharma sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_pharma = pd.merge(left=df_himis2, left_on='Home Zip',
                     right=df_himis3, right_on='Home Zip',
                     how='outer')

# total pharma spend: retirees and employees, and their dependents
df_pharma['Total Pharma Spend'] = (df_pharma['Total Spend-PE'] +
                                   df_pharma['Total Spend-PR'])

# merge medical and pharma into one dataset
df_himis = pd.merge(left=df_medical, left_on='Home Zip',
                    right=df_pharma, right_on='Home Zip',
                    how='outer')

# add up total HIMIS spend
df_himis['Total HIMIS Spend'] = (df_himis['Total Medical Spend'] +
                                 df_himis['Total Pharma Spend'])
# get total employee spend
df_himis['Total Employee Spend'] = (df_himis['Total Spend-ME'] +
                                    df_himis['Total Spend-PE'])

# get columns to make sure we get all we need during agg
# df_himis.columns

# prepare to merge with df_geo_zip to get city names
# by grouping and agg'ing
df_himis_agg = df_himis.groupby(['Home Zip'
                                 ]).agg({'Total Spend-MR': 'sum',
                                         'Total Spend-ME': 'sum',
                                         'Total Medical Spend': 'sum',
                                         'Total Spend-PR': 'sum',
                                         'Total Spend-PE': 'sum',
                                         'Total Pharma Spend': 'sum',
                                         'Total HIMIS Spend': 'sum'
                                         }).reset_index()

# rename zip at this point to match df_geo_zip
df_himis_agg.rename(columns={'Home Zip': 'Zip Code'}, inplace=True)
# check length to ensure we don't lose too many
# some zips are non-FL,so they will be lost
# len(df_himis_agg)

# change GeoFile zip code to str (object) type so merge will work
df_geo_zip['Zip Code'] = df_geo_zip['Zip Code'].astype(str)

# merge with df_geo_zip to attach city names
df_himis_agg = pd.merge(left=df_himis_agg, left_on='Zip Code',
                        right=df_geo_zip, right_on='Zip Code',
                        how='left')

# remove rows where city is null--they aren't florida zip codes
# we filtered out non-FL zips from df_geo_zips when we did retirement
df_himis_agg.dropna(axis=0, how='any', subset=(['City']), inplace=True)

# reset index after removing rows
df_himis_agg.reset_index(inplace=True)

# make replacements of known issues
df_himis_agg.replace(to_replace=to_replace, inplace=True)

# check to see which cities won't make the merge
# list(set(df_himis_agg['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# ready to group and agg in preparation for merge with df_geo_city
df_himis_agg = df_himis_agg.groupby(['City']).agg({
                                                  'Total Medical Spend':
                                                  'sum',
                                                  'Total Pharma Spend':
                                                  'sum',
                                                  'Total HIMIS Spend':
                                                  'sum'}).reset_index()

# rename city to avoid duplication after the merge
df_himis_agg.rename(columns={'city': 'City'}, inplace=True)

# merge with df_geo_city to attach district, county, etc information
# at the bottom; last step

# check totals of metrics
# df_himis_agg['Total Medical Spend'].sum()

# check totals of metrics
# df_himis_agg['Total Pharma Spend'].sum()

# merge with DSGI HIMIS data
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_himis_agg, right_on='City',
                        how='left')

df_city21_22.rename(columns={'Total HIMIS Spend': 'Total Claims',
                             'Total Medical Spend': 'Medical Claims',
                             'Total Pharma Spend': 'Pharmacy Claims',
                             'Total Employee Spend': 'Total Employee Claims'},
                    inplace=True)

# df_city21_22.head()



# In[8]:
    
# ### DSS
# Metrics: Fleet Maintenance Cost, Fuel Cost, Private Prison Contract Spend, 
# Federal Surplus Savings

# #### Private Prison Monitoring

# read private prison data
# it is already agg'd, so no need to
df_ppm = pd.read_excel('Divisions/DSS-Private Prisons/FY2021-2022/Feb 2022/21-22 PPM Man-Day Spreadsheet.xlsx',
                       sheet_name=7,
                       header=1,
                       skipfooter=16,
                       dtype={'Reimbursement to Vendor w/Deductions': float})
# df_ppm.columns

# df_ppm.head()

# data ends at 28th row, grab columns at same time
df_ppm = df_ppm.loc[:28, ['Facility', 'Reimbursement to Vendor w/Deductions']]

df_ppm.loc[2, 'Facility'] = 'BLACKWATER'

# need to join facility names with addresses frm dms website
df_ppm_location = pd.read_excel('Geo Files for Merging/ppm_locations.xlsx')

# df_ppm_location.columns

# join to attach addresses
# inner join to get rid of extra unnecessary rows
df_ppm_city = pd.merge(left=df_ppm, left_on='Facility',
                       right=df_ppm_location, right_on='Facility',
                       how='inner')

# no longer need name of facility - for now
df_ppm_city = df_ppm_city.iloc[:, 1:]

# rename to something a little digestible
df_ppm_city.rename(columns={'Reimbursement to Vendor w/Deductions':
                            'Private Prison Contract Spend'}, inplace=True)
# ready for merge at end

df_ppm_city['Private Prison Contract Spend'].sum()

# check to see which cities won't make the merge
# list(set(df_ppm_city['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# merge with PPM data
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_ppm_city, right_on='City',
                        how='left')

# df_city21_22.head()

# In[9]:

# #### LESO Federal Surplus

# Metrics: Federal Surplus Savings (dollars), Number of Items 
# this one comes in monthly sheets only, will need to concat

# semi-deprecated ExcelFile class helps get sheet names
xl = pd.ExcelFile('Divisions/DSS-LESO/FY2021-2022/May 2022/DSS LESO FY 21-22.xlsx')

# xl.sheet_names

# import current fiscal year's leso data, concat months together
# current month must match file name; need to fix directory structure there
current_month = 'May'
current_year = '2022'
folder = current_month + ' ' + current_year

JULY2021 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='JULY2021')
AUG_2021 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='AUG 2021')
SEP_2021 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='SEP 2021')
OCT_2021 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='OCT 2021')
NOV_2021 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='NOV 2021')
DEC_2021 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='DEC 2021')
JAN_2022 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='JAN 2022')
FEB_2022 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='FEB 2022')
MARCH_2022 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                           sheet_name='MARCH 2022')
APRIL_2022 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                           sheet_name='APRIL 2022')
MAY_2022 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                         sheet_name='MAY 2022')
'''
JUNE_2022 = pd.read_excel('Divisions/DSS-LESO/FY2021-2022/'+folder+'/DSS LESO FY 21-22.xlsx',
                          sheet_name='JUNE 2022')
'''


# months to come
# , JUNE_2022]
years21_22 = [JULY2021, AUG_2021, SEP_2021, OCT_2021, NOV_2021, DEC_2021,
              JAN_2022, FEB_2022, MARCH_2022, APRIL_2022, MAY_2022]

# concat sheets together vertically
df_leso21_22 = pd.concat(years21_22, ignore_index=True, axis=0)

# need to drop months with rows that are blanks
df_leso21_22.dropna(subset=['Law Enforcement\nAgency Name'],
                    axis=0, how='all', inplace=True)
# get rid of last 2 bad rows
df_leso21_22.dropna(subset=['Invoice\nNumber'],
                    axis=0, how='all', inplace=True)

df_leso21_22 = df_leso21_22.reset_index()

# rows 5 thru 13 are being difficult, let's remvoe them
df_leso21_22.drop(labels=[5, 6, 7, 8, 9, 10, 11, 12, 13], axis=0,
                  inplace=True)

# df_leso21_22.head(50)
# looks good, now the fun part

# fix dtypes
df_leso21_22['Initial Acquisition\nCost (IAC)'] = df_leso21_22['Initial Acquisition\nCost (IAC)'].astype(float)
df_leso21_22['Service Charge'] = df_leso21_22['Service Charge'].astype(float)
df_leso21_22.dtypes
# for easy copy pasting

df_leso21_22['Savings'] = df_leso21_22['Initial Acquisition\nCost (IAC)'] - df_leso21_22['Service Charge']

df_leso21_22.rename(columns={'Law Enforcement\nAgency Name': 'LE Agency Name'},
                    inplace=True)

df_leso21_22.replace(to_replace=to_replace, inplace=True)

df_leso21_22.rename(columns={'LE Agency Name': 'City'}, inplace=True)

# only get columns we need
df_leso21_22 = df_leso21_22[['Total Quantity\nof Items', 'Savings', 'City']]
# replace where appropriate, just added to master replace file
df_leso21_22.replace(to_replace=to_replace, inplace=True)
# ready for merge

# check to see which cities won't make the merge
# list(set(df_leso21_22['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# group and agg to the city level
df_leso_agg21_22 = df_leso21_22.groupby(['City']
                                        ).agg({'Savings': 'sum',
                                               'Total Quantity\nof Items':
                                               'sum'}
                                              ).reset_index()

# merge with LESO city-level data
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_leso_agg21_22, right_on='City',
                        how='left')

df_city21_22.rename(columns={'Total Quantity\nof Items':
                             'LESO Items',
                             'Savings': 'LESO Savings'
                             },
                    inplace=True)
    
# df_city21_22['LESO Savings].sum()

df_city21_22.head()

# In[10]:


# #### SASP
# Metrics: SASP Savings

# import raw file
df_sasp21_22 = pd.read_excel('C:\\Users\\mairsb\\OneDrive - Florida Department of Management Services\\07-Strategic Planning\\02-Projects\\Government Impact Dashboard Project\\Implementation\\Data Assembly\\Divisions\\DSS-SASP\\FY2021-2022\\April 2022\\Revised July21 - April 2022 - Copy.xlsx',
                             dtype={'Date Requested': str})

# df_sasp21_22.columns

# df_sasp21_22['Grand Donee Savings SUM'].sum()

# separate into components so we can separate by fiscal year
df_sasp21_22[['Year', 'Month', 'Day']] = df_sasp21_22["Date Requested"].str.split("-", expand = True)

# deal with one calendar year at a time
df_sasp21_22_21 = df_sasp21_22[df_sasp21_22['Year'] == '2021']

# coerce month to int
df_sasp21_22_21['Month'] = df_sasp21_22_21['Month'].astype('int')
df_sasp21_22_21 = df_sasp21_22_21[df_sasp21_22_21['Month'] >= 7]

# deal with one calendar year at a time
df_sasp21_22_22 = df_sasp21_22[df_sasp21_22['Year'] == '2022']

# coerce month to int
df_sasp21_22_22['Month'] = df_sasp21_22_22['Month'].astype('int')
df_sasp21_22_22 = df_sasp21_22_22[df_sasp21_22_22['Month'] <= 6]

df_sasp21_22 = pd.concat([df_sasp21_22_21, df_sasp21_22_22], ignore_index=True)

df_sasp21_22.head()

# df_sasp21_22['Grand Donee Savings SUM'].sum()

# import donee locations to attach to first dataset
# sometimes the file has this info, sometimes not. it does for now


# df_donees = pd.read_excel('Divisions/DSS-SASP/FY2021-2022/Donee City - SharePoint/April 2022 - Copy.xlsx')
# # df_donees.columns

# # df_donees.head()

# # merge not working, issue is the Cxxxxxx on the end of sasp data
# # need to remove, obv
# #df_sasp21_22['Donee Name'] = df_sasp21_22['Donee Name'].str.extract('(.+)C\d+')
# # make it uppercase
# df_sasp21_22['Donee Name'] = df_sasp21_22['Donee Name'].str.upper()
# df_donees['Donee Name'] = df_donees['Donee Name'].str.upper()


# # spaces on right side were causing merge issues
# df_donees['Donee Name'] = df_donees['Donee Name'].str.rstrip(' ')
# df_sasp21_22['Donee Name'] = df_sasp21_22['Donee Name'].str.rstrip(' ')


# # merge to attach city names to sasp data
# df_sasp21_22 = pd.merge(left=df_sasp21_22, left_on='Donee Name',
#                         right=df_donees, right_on='Donee Name',
#                         how='left')


# df_sasp21_22.columns

# df_sasp21_22.head()

# narrow down to columns we need
df_sasp21_22 = df_sasp21_22[['Grand Donee Savings SUM', 'City']]

# make city uppercase
df_sasp21_22['City'] = df_sasp21_22['City'].str.upper()

# check to see which cities won't make the merge
# list(set(df_sasp21_22['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# replace where appropriate, just added to master replace file
df_sasp21_22.replace(to_replace=to_replace, inplace=True)

# drop the ones that were ambiguous or not in FL
df_sasp21_22 = df_sasp21_22[df_sasp21_22['City'].isin(to_drop) == False]

# trim off whitespace, was causing issues
df_sasp21_22['City'] = df_sasp21_22['City'].str.strip()

# group and agg to the city level
df_sasp21_22_agg = df_sasp21_22.groupby(['City']).agg({
                                                       'Grand Donee Savings SUM':
                                                       'sum',
                                                       }).reset_index()

# list(set(df_sasp21_22['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_sasp21_22_agg, right_on='City',
                        how='left')

df_city21_22.rename(columns={
                             'Grand Donee Savings SUM': 'SASP Savings',
                             },
                    inplace=True)

# df_city21_22['SASP Savings'].sum()

# df_city21_22.head()

# In[11]:

# ### F & A
# ####  Disaster Response
# Metrics: Disaster Response Spend, Number of Incidents

# this data is a bit light (and a bit messy), for now
df_fna = pd.read_excel('Divisions/F&A- Disaster Response/FY2021-2022/F&A Disaster Response.xlsx')
total = df_fna['Unnamed: 4'][16]
# this is the all they have, and no locations, so just leave here

# In[12]:

# ### MFMP
# Metrics: Total MFMP Spend, Number of Vendors


# import my florida marketplace data
df_mfmp = pd.read_csv('Divisions/MFMP/FY2021-2022/May 2022/DMS Government Impact Dashboard - Copy.csv')

# list columns, only need a couple
# df_mfmp.columns

# filter out non-FL impact transactions
df_mfmp = df_mfmp[df_mfmp['Supplier Location - PO State'] == 'FL'].reset_index()
# number of transactions left is substantial ~180K
# len(df_mfmp)

# select only the columns we need
df_mfmp = df_mfmp.loc[:, ['Supplier Location - PO City', 'sum(Invoice Spend)',
                          'Supplier - Company Name']]

# rename columns
df_mfmp.rename(columns={'Supplier Location - PO City': 'City',
                        'sum(Invoice Spend)': 'MFMP Spend',
                        'Supplier - Company Name': 'Vendors'},
               inplace=True)

# capitalize city for eventual merge
df_mfmp['City'] = df_mfmp['City'].str.upper()

# go ahead and do all replacements, adding new ones
df_mfmp.replace(to_replace=to_replace, inplace=True)

# drop the ones that were ambiguous or not in FL
df_mfmp = df_mfmp[df_mfmp['City'].isin(to_drop) == False]

# check to see which cities won't make the merge
# list(set(df_mfmp['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# change MFMP spend to numeric column so agg will work
df_mfmp['MFMP Spend'] = df_mfmp['MFMP Spend'].str.replace(',', '', regex=True)
df_mfmp['MFMP Spend'] = df_mfmp['MFMP Spend'].astype('float')

# we have some extra vendors in our db
# df_mfmp['Vendors'].value_counts()

# group and agg to city level
df_mfmp_agg = df_mfmp.groupby(['City']).agg({
                                             'MFMP Spend':
                                             'sum',
                                             'Vendors':
                                             'nunique'
                                             }).reset_index()

# check metric totals for consistency with dashboard
# df_mfmp_agg['MFMP Spend'].sum()

# ended up with 500 more vendors somehow
# df_mfmp_agg['Vendors'].sum()

# merge with MFMP data
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_mfmp_agg, right_on='City',
                        how='left')

# df_city21_22.head()

# In[13]:

# ### OSD
# Metrics: Number of Minority-Owned Businesses Registered

df_osd21_22 = pd.read_excel('Divisions/OSD/FY2021-2022/OSD Certified Vendor File.xlsx')

# df_osd21_22.columns

# df_osd21_22.head()
# many dates are not of this fiscal year

# missing_data(df_osd21_22)

# only grab registrations active for FY 19-20
# there's a much better way to do this, but no time
# generate dates for entire FY
rng = pd.date_range('07-01-2021', '06-30-2022', periods=365).to_frame()

# place in datframe
df_FY21_22 = pd.DataFrame(rng[0].astype(str)).reset_index()

# split the date component and the time component
df_FY21_22['Time'] = df_FY21_22[0].str.split(' ')

# grab the date portion
df_FY21_22['Time'] = df_FY21_22['Time'].str[0]

# place in a list
wrong_format = df_FY21_22['Time'].to_list()

# must change to our precious format
FY21_22 = []

# change to match OSD date format
for i in wrong_format:
    temp = i.split('-')
    FY21_22.append(temp[1]+'/'+temp[2]+'/'+temp[0])
# if date is in our range, grab it


def inYear(row):
    if (row['Effective On'] in FY21_22) or (row['Expire On'] in FY21_22):
        return '1'
    else:
        return '0'


# apply a marker to our df that we can use to filter it
df_osd21_22['Indic'] = df_osd21_22.apply(inYear, axis=1)

df_osd21_22 = df_osd21_22[df_osd21_22['Indic'] == '1']

# we should be getting at least 4 businesses
# df_osd21_22.head()

# conver to upper to match df_geo
df_osd21_22['City'] = df_osd21_22['City'].str.upper() 

# check for cities that didn't make it
# list(set(df_osd21_22['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# replace cities that won't make the merge
# because they don't appear in df_geo_city
df_osd21_22.replace(to_replace=to_replace, inplace=True)

# drop known-issue cities
df_osd21_22 = df_osd21_22[df_osd21_22['City'].isin(to_drop) == False]

# group and agg to the city level
df_osd21_22_agg = df_osd21_22.groupby(['City']).agg({
                                                     'Name':
                                                     'count'}
                                                    ).reset_index()

# rename to match other years
df_osd21_22_agg.rename(columns={
                                'Name':
                                'Minority-Owned Businesses Registered'
                                },
                       inplace=True)

# merge on city with main fy 19-20 dataset
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_osd21_22_agg, right_on='City',
                        how='left')


# df_city21_22.head()

# In[14]:

# ### PeopleFirst
# #### PeopleFirst Data Warehouse
# Metrics: Positions, Employees, Annualized Salary, Health Insurance Employer
# Contribution, Vacancies

# import PeopleFirst files
# grab columns we need at same time, might as well
df_pf21_22 = pd.read_excel('Divisions/People First - SPS/FY2021-2022/March 2022/PF FY 21-22 Mar U - Copy.xlsx')

# df_pf21_22.columns

# len(df_pf21_22)


# df_pf21_22.dtypes


# df_pf21_22.head(5)


# df_pf21_22['Employee Type'].value_counts()

# filter down to OPS employees only
df_pf21_22 = df_pf21_22[(df_pf21_22['Employee Type'] == 1) | (df_pf21_22['Employee Type'] == 2)]

# recheck length for reasonable-ness
# len(df_pf21_22)

df_pf21_22 = df_pf21_22[['Agency Name', 'Location City', 'Pos Num ',
                         'Vacancy Ind', 'Annual Salary',
                         'Health Amt',
                         'Appt FTE']]

# replace vacancy indicator with 1 for agg'ing
df_pf21_22.replace("Y", 1, inplace=True)

df_pf21_22.replace(to_replace=to_replace, inplace=True)

# drop known issue cities
df_pf21_22 = df_pf21_22[df_pf21_22['Location City'].isin(to_drop) == False]

# check for cities that didn't make it
# list(set(df_pf21_22['Location City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# many blank locations, check those
# df_pf21_22[df_pf21_22['Location City'] == ' ']

df_pf21_22['Location City'].fillna('UNDEFINED', inplace=True)

df_pf21_22.replace(to_replace={' ': 'UNDEFINED'}, inplace=True)

# aggregate and rename PF position data to city level
df_pf21_22_agg = df_pf21_22.groupby(['Location City'
                                     ]).agg({'Pos Num ': 'nunique',
                                             'Vacancy Ind': 'sum',
                                             'Annual Salary':
                                             'sum',
                                             'Health Amt':
                                             'sum',
                                             'Appt FTE':
                                             'sum'
                                             }).reset_index()
df_pf21_22_agg.rename(columns={'Pos Num ': 'Positions',
                               'Vacancy Ind': 'Vacancies',
                               'Location City': 'City',
                               'Annual Salary': 'Annualized Salary',
                               'Health Amt':
                               'Employer Insurance Contributions',
                               'Appt FTE': 'Employees'},
                      inplace=True)

# make sure cities are upper for the merge
df_pf21_22_agg['City'] = df_pf21_22_agg['City'].str.upper()

# df_pf21_22_agg.head()

# df_pf21_22_agg['Positions'].sum()

# df_pf21_22_agg['Employees'].sum()

# df_pf21_22_agg['Annualized Salary'].sum()

# Merge all REDM and DivTel data with df_geo_city at city level
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_pf21_22_agg, right_on='City',
                        how='left')

# df_city21_22.head()


# #### PeopleFirst Data Warehouse II
# 
# Metrics: Employee Residences

# import PeopleFirst files
# grab columns we need at same time, might as well
df_pf21_22_2 = pd.read_excel('Divisions/People First - SPS/FY2021-2022/March 2022/PF FY 21-22 Mar U - Copy.xlsx')

df_pf21_22_2.dtypes

df_pf21_22_2 = df_pf21_22_2[['Home  City', 'Annual Salary']]

df_pf21_22_2.replace(to_replace=to_replace, inplace=True)

# drop known issue cities
df_pf21_22_2 = df_pf21_22_2[df_pf21_22_2['Home  City'].isin(to_drop) == False]

# check for cities that didn't make it
# list(set(df_pf21_22_2['Home  City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

df_pf21_22['Location City'].fillna('UNDEFINED', inplace=True)

df_pf21_22.replace(to_replace={' ': 'UNDEFINED'}, inplace=True)

# aggregate and rename PF position data to city level
df_pf21_22_2_agg = df_pf21_22_2.groupby(['Home  City'
                                         ]).agg({
                                                 'Annual Salary':
                                                 'count',
                                                 }).reset_index()
df_pf21_22_2_agg.rename(columns={'Home  City': 'City',
                                 'Annual Salary': 'Employee Residences',
                                 },
                        inplace=True)

# make sure cities are upper for the merge
df_pf21_22_2_agg['City'] = df_pf21_22_2_agg['City'].str.upper()

df_pf21_22_2_agg.head()

# Merge all REDM and DivTel data with df_geo_city at city level
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_pf21_22_2_agg, right_on='City',
                        how='left')

df_city21_22.head()

# #### PeopleFirst Data Warehouse III
# Metrics: OPS Employees

# import PeopleFirst files
# grab columns we need at same time, might as well
df_pf21_22 = pd.read_excel('Divisions/People First - SPS/FY2021-2022/March 2022/PF FY 21-22 Mar Update.xlsx')

# df_pf21_22.dtypes

# df_pf21_22.head(5)

# df_pf21_22['Employee Type'].value_counts()

# filter down to OPS employees only 
df_pf21_22 = df_pf21_22[(df_pf21_22['Employee Type'] == 4) | (df_pf21_22['Employee Type'] == 5)]

# len(df_pf21_22)

df_pf21_22 = df_pf21_22[['Agency Name', 'Pos Num ', 'Location City', 'Vacancy Ind']]

df_pf21_22.replace(to_replace=to_replace, inplace=True)

# drop known issue cities
df_pf21_22 = df_pf21_22[df_pf21_22['Location City'].isin(to_drop) == False]

# check for cities that didn't make it
# list(set(df_pf21_22['Location City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

df_pf21_22 = df_pf21_22[df_pf21_22['Vacancy Ind'] != 'Y']

# len(df_pf21_22)

# aggregate and rename PF position data to city level
df_pf21_22_agg = df_pf21_22.groupby(['Location City'
                                     ]).agg({'Pos Num ':
                                             'nunique'
                                             }).reset_index()
df_pf21_22_agg.rename(columns={'Pos Num ': 'OPS Employees',
                               'Location City': 'City'},
                      inplace=True)

# make sure cities are upper for the merge
df_pf21_22_agg['City'] = df_pf21_22_agg['City'].str.upper()

# df_pf21_22_agg.head()

# df_pf21_22_agg['OPS Employees'].sum()

# Merge all REDM and DivTel data with df_geo_city at city level
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_pf21_22_agg, right_on='City',
                        how='left')

df_city21_22.head()




# In[15]:

# ### REDM-Managed Facilties
# Metrics: Owned Sq. Footage, Facilities Owned, FCO Spend, Structure Value, Land Value

# Read in REDM data into dataframes: owned and leased bldgs
df_redm21_22 = pd.read_csv('Divisions/REDM-SOLARIS/FY2021-2022/REDM FY2021-2022_ALL_Facility.csv')

# get dms blds only per secretary
df_redm21_22 = df_redm21_22[df_redm21_22['Agency Name'] == 'Department of Management Services']

# get necessary columns only
df_redm21_22 = df_redm21_22[['FL-SOLARIS Facility #',
                             'Facility City', 'Gross Sq Ft',
                             'Taxroll Land Value',
                             'Taxroll Structure Value'
                             ]]

# Rename columns to match with the required metric values
df_redm21_22.rename(columns={'Gross Sq Ft': 'Owned Square Footage',
                             'Taxroll Land Value': 'Land Value',
                             'Taxroll Structure Value': 'Structure Value',
                             'FL-SOLARIS Facility #': 'ID',
                             'Facility City': 'City'
                             },
                    inplace=True)

# fill na to make sure agg summing work
df_redm21_22['ID'].fillna(0, inplace=True)

# capitalize facility city
df_redm21_22['City'] = df_redm21_22['City'].str.upper()

# replace county names that didn't make it into the merge
df_redm21_22.replace(to_replace=to_replace, inplace=True)

# ambiguous or other state's cities must be dropped
df_redm21_22_DMS = df_redm21_22[df_redm21_22['City'].isin(to_drop) == False]

# Aggregating and renaming metrics from the Owned file
df_redm21_22_agg = df_redm21_22.groupby(['City'
                                         ]).agg({'ID': 'count',
                                                 'Owned Square Footage':
                                                 'sum',
                                                 'Land Value': 'sum',
                                                 'Structure Value':
                                                 'sum'
                                                 }).reset_index()
df_redm21_22_agg.rename(columns={'ID': 'Facilities Owned'}, inplace=True)
# ready for merge

# check owned to see if any cities won't make the merge
# empty set is good
# list(set(df_redm21_22_agg['City'].str.upper().to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# Merge with df_geo_city to attach location information at city level
df_city21_22 = pd.merge(left=df_city21_22, right=df_redm21_22_agg,
                        left_on='City', right_on='City',
                        how='left')

# df_city21_22.head()


# #### Construction Projects
# Metrics: Fixed Capital Outlay

# import fco data on current construction projects
df_redm = pd.read_excel('Divisions/REDM-FCO/FY2021-2022/Feb 2022/REDM FCO Feb 2022.xlsx')

# capitalize city in accordance with conventions
df_redm['City'] = df_redm.City.str.upper()

# rename columns
df_redm.rename(columns={'Funds Allocated': 'Fixed Capital Outlay'},
               inplace=True)

# sum to check total against dashboard for errors
df_redm['Fixed Capital Outlay'].sum()

# check to see which cities didn't make the merge
# list(set(df_redm['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# make replacements to ensure clean merge
df_redm.replace(to_replace=to_replace, inplace=True)

# ambiguous or other state's cities must be dropped
df_redm = df_redm[df_redm['City'].isin(to_drop) == False]

# group and agg to the city level
df_redm_agg = df_redm.groupby(['City']).agg({'Fixed Capital Outlay':
                                             'sum'}).reset_index()
# ready for merge with overall data at end

# sum to check total against dashboard for errors
df_redm_agg['Fixed Capital Outlay'].sum()

# Merge all REDM and DivTel data with df_geo_city at city level -- vijay
df_city21_22 = pd.merge(left=df_city21_22, right=df_redm_agg,
                        left_on='City', right_on='City',
                        how='left')

# df_city21_22.head()

# In[16]:

# ### RET
# Metrics: Benefits Paid, Number of Payees, Number of Employers, Employer Contributions

# #### Payees
# Metrics: Number of Payees, Payment Amount
# now get payments info
# skipping 1 header, assigned zip to str to preserve any leading zeroes
# skip 1 footer, which is grand total
df_ret3 = pd.read_excel('Divisions/RET-IRIS/FY2021-2022/April/RET-IRIS.xlsx',
                        sheet_name='FY Payments per zip and city',
                        header=1,
                        dtype={'Florida Zip Code': str},
                        skipfooter=1)
# df_ret3.columns

# check payment amount
# includes all states
# df_ret3['Payment Amount'].sum()

# drop first 60 rows to remove other state's data
df_ret3 = df_ret3.loc[60:, 'Florida Zip Code':].reset_index()

# check payment amount again
# df_ret3['Payment Amount'].sum()

# only grab columns we need
df_ret3 = df_ret3[['Florida Zip Code', 'Payment Amount', 'Number of Payees']]

# rename columns now (optional)
df_ret3.rename(columns={'Florida Zip Code': 'zip',
                        'City Name': 'City'}, inplace=True)

# do drops of zip codes that aren't in state
df_ret3 = df_ret3[df_ret3['zip'].isin(['95136', '30564', '28792', '63042']) == False]

# track down zip codes that didn't get matched up with cities
# list(set(df_ret3['zip'].to_list()).difference(list(set(df_geo_zip['Zip Code'].to_list()))))

# merge with zip code file to attach city names
df_ret3_city = pd.merge(left=df_geo_zip, left_on='Zip Code',
                        right=df_ret3, right_on='zip',
                        how='left')

df_ret3_city['City'] = df_ret3_city['City'].str.upper()

# don't need zip anymore, slice it off
df_ret3_city = df_ret3_city[['City', 'Payment Amount', 'Number of Payees']]

# check payment amount again
# we had lost $40M at one point
# df_ret3_city['Payment Amount'].sum()

# capitalize city
df_ret3_city.rename(columns={'city': 'City',
                             'Payment Amount': 'Benefits Paid to Retirees'
                             },
                    inplace=True)

# perform replacements that we already did in OSD
# do before group and agg
df_ret3_city.replace(to_replace=to_replace, inplace=True)

# check to see which cities won't make the merge
# list(set(df_ret3_city['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# group and agg to city level to merge with other retirement data
df_ret3_agg = df_ret3_city.groupby(['City']).agg({'Benefits Paid to Retirees':
                                                  'sum',
                                                  'Number of Payees': 'sum'
                                                  }).reset_index()

# check to see which cities won't make the merge
# list(set(df_ret3_agg['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# df_ret3_agg['Benefits Paid to Retirees'].sum()

# df_ret3_agg['Number of Payees'].sum()

# join with master set
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_ret3_agg, right_on='City',
                        how='left')

# df_city21_22.head()


# #### Employer Locations

df_ret = pd.read_excel('C:\\Users\\mairsb\\OneDrive - Florida Department of Management Services\\07-Strategic Planning\\02-Projects\\Government Impact Dashboard Project\\Implementation\\Data Assembly\\Divisions\\RET-IRIS\\FY2021-2022\\APRIL\\RET-IRIS.xlsx',
                       sheet_name='FY Agency Employer Contribution')
# df_ret.columns

# df_ret.head()

df_ret.rename(columns={'City Name': 'City',
                       'Agency Name': 'Number of Employers Using Retirement System'},
              inplace=True)

# make replacements we know will be a problem
df_ret.replace(to_replace=to_replace, inplace=True)

# do drops while we're at it
df_ret = df_ret[df_ret['City'].isin(to_drop) == False]

# check to see which cities won't make the merge with city
# list(set(df_ret['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

df_ret_agg = df_ret.groupby(['City']).agg({'Number of Employers Using Retirement System':
                                           'nunique'}).reset_index()

df_ret['Number of Employers Using Retirement System'].nunique()

# merge with first set of retirement data
# outer join is appropriate so we don't lose cities
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_ret_agg, right_on='City',
                        how='outer')

# df_city21_22.head()

# df_city21_22['Number of Employers Using Retirement System'].sum()


# #### Employer Contributions
# Metrics: Sum of employer contributions

# read in employer contribution data
df_ret4 = pd.read_excel('Divisions/RET-IRIS/FY2021-2022/April/RET-IRIS.xlsx',
                        sheet_name='FY Agency Employer Contribution',
                        skipfooter=1)
# list columns
# df_ret4.columns

# grab relevant columns
df_ret4 = df_ret4[['City Name', 'Employer Contribution Amount']]
# rename to match our format
df_ret4.rename(columns={'City Name': 'City',
                        'Employer Contribution Amount':
                        'Employer Contributions'
                        }, inplace=True)

# check sum so we don't lose too much
# df_ret4['Employer Contributions'].sum()

# we need to do replacements, can see mistakes already
df_ret4.replace(to_replace=to_replace, inplace=True)

# drop known issues with names
df_ret4 = df_ret4[df_ret4['City'].isin(to_drop) == False]

# check to see which cities won't make the merge
# list(set(df_ret4['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# lstrip to coerce one particular error
df_ret4['City'] = df_ret4['City'].str.lstrip()

# group and agg to city level to prep for merge
df_ret4_agg = df_ret4.groupby(['City']).agg({'Employer Contributions': 'sum'
                                             }).reset_index()
# ready for merge with other retirement data

# check to see which cities won't make the merge
# list(set(df_ret4_agg['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# df_ret4_agg['Employer Contributions'].sum()

# need to merge RET with master df 21-22 on CITY
df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_ret4_agg, right_on='City',
                        how='left')

df_city21_22.rename(columns={
                             'Number of Employer Locations':
                             'Number of Employers Using Retirement System',
                             'Number of Active Members':
                             'Active Members',
                             },
                    inplace=True)

# df_city21_22.head()




# In[17]:

# ### STMS
# Metrics: STMS Travel Spend

# separate fy 18-10 from the rest
df_stms21_22 = pd.read_csv('Divisions/STMS/FY2021-2022/May 2022/Travel Data for Impact Dashboard 05022022.csv', encoding='cp1252')

# df_stms21_22.columns

# df_stms21_22.dtypes

# len(list(set(df_stms21_22['Form ID: * Destination'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# create list of cities to search for
city_list = df_geo_city['City'].to_list()


def cityFinder(row):
    # iterate over every name in the city list
    for city in city_list:
        # if our city appears anywhere, return it
        if str(city) in str(row['Form ID: * Destination']).upper():
            return city
        # else leave it alone and capitalize
        else:
            pass


df_stms21_22['City'] = df_stms21_22.apply(cityFinder, axis=1)

# find counties in the remaining locations in 'City'
# get list of all fl counties to iterate over
county_list = df_geo_county['County'].to_list()

# get oen city for each county to assign when we find a county
cc_df = df_geo_city[['City', 'County']].drop_duplicates(subset='County')

# build dict of county names and the city within them we're going to use
county_city = dict(zip(cc_df['County'].to_list(), cc_df['City']))


def countyFinder(row):
    # iterate over every county for every row
    for county in county_list:
        # if there's a match, assign that
        if str(county).split(' ')[0].upper() in str(row['Form ID: * Destination']).upper():
            return county_city[county]
        # otherwise do nothing
        else:
            pass


df_stms21_22['City2'] = df_stms21_22.apply(countyFinder, axis=1)

# fill city with counties that were found
df_stms21_22['City'].fillna(df_stms21_22['City2'].str.upper(), inplace=True)

# fill in cities and counties that weren't found with the original
df_stms21_22['City'].fillna(df_stms21_22['Form ID: * Destination'].str.upper(), inplace=True)

# check cities than are still erroneous
# len(list(set(df_stms21_22['City'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# make replacements we have collected, maybe will make a difference
df_stms21_22.replace(to_replace=to_replace, inplace=True)

# check again
# len(list(set(df_stms21_22['City'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# drop the ones that were ambiguous or not in FL
df_stms21_22 = df_stms21_22[df_stms21_22['City'].isin(to_drop) == False]

# len(list(set(df_stms21_22['City'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# df_stms21_22.columns

# df_stms21_22.head()

df_stms21_22_agg = df_stms21_22.groupby(['City']).agg({'Total Amount': 'sum'
                                                       }).reset_index()

df_stms21_22_agg = df_stms21_22_agg[['City', 'Total Amount']]

df_stms21_22_agg.rename(columns={'Total Amount': 'Travel Spend',
                                 },
                        inplace=True)

df_city21_22 = pd.merge(left=df_city21_22, left_on='City',
                        right=df_stms21_22_agg, right_on='City',
                        how='left')

# df_city21_22['Travel Spend'].sum()

# df_city21_22.head()


# In[18]:

# ### Year-End Processing

# assign years for tableau filter
df_city21_22['FY'] = '21-22'

# assign years for tableau filter
df_county21_22['FY'] = '21-22'


# ## Export

# ### County-level Data

# df_county21_22.dtypes


# create excel files for import to Tableau
df_county21_22.to_excel('Fiscal Years/County Level FY 2021-2022.xlsx')


# ### City-level Data

# show which metrics we have, ready for dashboarding
# df_city21_22.dtypes


# In[344]:


try:
    df_city21_22.drop(['Unnamed: 0', 'index'], axis=1, inplace=True)
except KeyError:
    pass

# export to excel file for tableau
df_city21_22.to_excel('Fiscal Years/City Level FY 2021-2022.xlsx')


# ## Time
executionTime = (time.time() - startTime)

# convert to mins
mins = str(round(executionTime/60))
secs = str(round(executionTime % 60, 2))

print('Execution time: {} minutes, {} seconds: '.format(mins, secs))
