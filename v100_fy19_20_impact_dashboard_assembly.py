# -*- coding: utf-8 -*-
"""
Created on Fri May  6 12:54:02 2022

@author: MairsB
"""

# !/usr/bin/env python
# coding: utf-8

# # FY 19-20 Impact Dashboard Data Assembly

# In[1]:

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
df_city19_20 = pd.DataFrame(data=df_geo_city)
# we will join required datasets to this df

# get separate county dataset by getting list of FL counties
# tableau already has shape info for counties, so we only need the name
df_geo_county = df_geo_city[['County']].drop_duplicates(keep='first')

# initiliaze county data frames for each FY
df_county19_20 = pd.DataFrame(data=df_geo_county)
# we will join required datasets to this df

# import data to attach city names to zip codes
filepath = 'Geo Files for Merging/flzips.xlsx'

df_geo_zip = pd.read_excel(filepath, dtype={'Zip Code': str})

# city names will be upper case
df_geo_zip['City'] = df_geo_zip['City'].str.upper()

# correct misspelled city names to match df_geo_city
# if city names don't match, data will be lost in merge 
# build dict of replacement to make
# closest city in same district was chosen if city was not in our GeoFile
to_replace = {}
with open('other/to_replace.txt', 'r') as f:
    # text file format is incorrectname:correctname, parse that into a dict
    # dict is accepted as an argument in the .replace method in pd
    to_replace = {i: j for line in f for (i, j) in [line.strip('\n').split(':')]}
# this dict was built by differencing lists of df_geo cities and dms data with this code:
# list(set(<dataframe_column>.to_list()).difference(list(set(df_geo_city['City'].to_list()))))
# this generates a unique list of cities that don't match df_geo
# can also be used for zip codes by replacing the pandas column with the difference method

# ambiguous or other state's cities must be dropped
# same as to_replace, kept running file
to_drop = []
# grab from file
with open('other/to_drop.txt', 'r') as f2:
    # list comp so we can drop cities in this list
    to_drop = [k for line in f2 for k in [line.strip('\n')]]

# Function to count missing values for each columns in a DataFrame, took from stack overflow
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



# In[2]:

# ## FY 2019-2020

# ### DivTel
# Metrics: Number of Internet Circuits, Value of Internet Circuits

# #### Internet Circuits
# Metrics: Number of Internet Circuits, Value of Internet Circuits

df_csab19_20 = pd.read_excel('Divisions/Divtel- CSAB- Internet Circuits/DivTel CSAB Inventory Retail FY 19-20.xlsx',
                            dtype={'ZIPCODE': str})

# df_csab19_20.columns

# missing_data(df_csab19_20)

# Get the necessary columns from CSAB data
df_csab19_20 = df_csab19_20[['ZIPCODE', 'CIRCUITS', 'CHARGE']]

# check which zips aren't in our flzips geofile
# list(set(df_csab19_20['ZIPCODE'].to_list()).difference(list(set(df_geo_zip['Zip Code'].to_list()))))

# join with flzips to attach city names
df_csab19_20 = pd.merge(left=df_csab19_20, left_on='ZIPCODE',
                        right=df_geo_zip, right_on='Zip Code',
                        how='left')

# check for cities that won't merge cleanly
# list(set(df_csab19_20['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# make replacements to ensure merge with df_geo_city is smooth
df_csab19_20.replace(to_replace=to_replace, inplace=True)

# drop known issue cities
df_csab19_20 = df_csab19_20[df_csab19_20['City'].isin(to_drop) == False]

# check for cities that won't merge cleanly
# list(set(df_csab19_20['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# aggregate CSAB data to the city level in preparation for merge
df_csab19_20_agg = df_csab19_20.groupby(df_csab19_20['City'
                                                     ].str.upper()
                                        ).agg({'CIRCUITS': 'sum',
                                               'CHARGE': 'sum'
                                               }).reset_index()

# rename columns
df_csab19_20_agg.rename(columns={'CIRCUITS': 'Internet Circuits',
                                 'CHARGE': 'Value of Internet Circuits'
                                 },
                        inplace=True)

# check sum of assets and retail value against DB
# df_csab19_20_agg['Internet Circuits'].sum()

# check sum of assets and retail value against DB
# df_csab19_20_agg['Value of Internet Circuits'].sum()

# better
# df_csab19_20_agg['Internet Circuits'].sum()

# Merge csab data with master set
df_city19_20 = pd.merge(left=df_city19_20, right=df_csab19_20_agg,
                        left_on='City', right_on='City',
                        how='left')

# check our work
# df_city19_20.head()

# In[3.5]:

# #### 911 Disbursements
# Level: County Level

# Metrics: E911 Disbursements

# import grants information
df_disb19_20 = pd.read_excel('C:\\Users\\mairsb\\OneDrive - Florida Department of Management Services\\07-Strategic Planning\\02-Projects\\Government Impact Dashboard Project\\Implementation\\Data Assembly\\Divisions\\DivTel - 911 Disbursements\\FY2019-2020\\County Monthly disbu - Copy.xlsx')


# df_disb19_20.columns

# df_disb19_20.tail(55)


# slice of last few rows that have totals
df_disb19_20 = df_disb19_20.iloc[:68, :]


# only get total columns, others are unneccessary
df_disb19_20 = df_disb19_20[['Unnamed: 0',
                             'fy19/20 Total', 'fy19/20 Total.1',
                             'fy19/20 Total.2', 'fy19/20 Total.3',
                             'fy19/20 Total.4'
                             ]]

# change column names to what's in the first row
df_disb19_20.columns = df_disb19_20.iloc[0]

# slice off the first row

df_disb19_20 = df_disb19_20[1:]

# sum various categories of spend
df_disb19_20['E911 Disbursements'] = (
                                     df_disb19_20['Wireless'] +
                                     df_disb19_20['Nonwireless'] +
                                     df_disb19_20['Prepaid Wireless'] +
                                     df_disb19_20['Supplemental'] +
                                     df_disb19_20['Special']
                                     )

# add 'County' to county field for merge prep
df_disb19_20['County'] = df_disb19_20['County'] + ' County'

# check to make sure there are no misspellings
# list(set(df_disb19_20['County'].to_list()).difference(df_geo_county['County'].to_list()))

# group and agg is not really necessary, but it can't hurt

df_disb19_20_agg = df_disb19_20.groupby(['County']).agg(
                                                        {'E911 Disbursements':
                                                         'sum'
                                                         }
                                                          ).reset_index()
# we're good, go ahead and merge
df_county19_20 = pd.merge(left=df_county19_20, left_on='County',
                          right=df_disb19_20_agg, right_on='County',
                          how='left')
# df_county21_22.head()

# In[4]:

# In[3]:

# #### SLERS Towers
# Metrics: Number of SLERS Towers

# import SLERS towers data
# locations of towers don't change, so we just use 21-22 data for all years
df_towers = pd.read_csv('Divisions/Divtel- SLERS/FY2021-2022/DivTel SLERS Towers_11_10_2020.csv')

# list columns, won't need all of them
# df_towers.columns

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
df_county19_20 = pd.merge(left=df_county19_20, left_on='County',
                          right=df_towers_agg, right_on='County',
                          how='left')

# df_county19_20.head()

# In[4]:

# #### 911 Grants
# Level: County Level
# Metrics: Grants Awarded

# import grants information
df_grants19_20 = pd.read_excel('Divisions/Divtel - 911 Grants/FY2019-2020/DivTel 911 Grants FY 19-20.xlsx')

# df_grants19_20.columns

# df_grants19_20.head()

# Grants Data
df_grants19_20 = df_grants19_20[['County Name', 'FinalAward']]

# rename
df_grants19_20.rename(columns={'FinalAward': 'Grants Awarded',
                               'County Name': 'County'
                               },
                      inplace=True)

# add county to county name
df_grants19_20['County'] = df_grants19_20['County'] + ' County'

# group and agg to city level
df_grants19_20_agg = df_grants19_20.groupby(['County'
                                             ]).agg({
                                                     'Grants Awarded':
                                                     'sum'
                                                     }).reset_index()

# check sum of grants against dashboard
# df_grants19_20_agg['Grants Awarded'].sum()

# see if any counties won't make the merge
# list(set(df_grants19_20_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# Merge grants and e-rate disbursements to generate a county-level data frame
df_county19_20 = pd.merge(left=df_county19_20, right=df_grants19_20_agg,
                          left_on='County', right_on='County', how='left')

# df_county19_20.head()

# In[5]:

# #### E-Rate Discounts, FY 2020-2021
# Metrics: E-Rate Disbursements

# import e-rate data
df_erate = pd.read_csv('Divisions/Divtel- E-rate Organizer/FY2019-2020/DivTel E-rate FY 2019-2020.csv')

# Clean up the E-Rate data that is reported as $ strings
df_erate = df_erate[['County', 'Total Authorized Disbursement']]

# get rid of dollar signs, we can format in tableau
df_erate['E-Rate Disbursements'] = df_erate['Total Authorized Disbursement'
                                            ].str.lstrip('$')

# add county name with space to match with df_geo-county
df_erate['County'] = df_erate['County'] + ' County'

# replace all found replacements to ensure merge with df_geo_county works
df_erate.replace(to_replace=to_replace, inplace=True)

# strip commas so agg will work
df_erate.replace({',': ''}, regex=True, inplace=True)

# change type to float so agg will work
df_erate['E-Rate Disbursements'] = df_erate['E-Rate Disbursements'
                                            ].str.strip(' ').astype(float)

# group and agg to city level
df_erate_agg = df_erate.groupby(['County'
                                 ]).agg({'E-Rate Disbursements':
                                         'sum'}).reset_index()

# df_erate_agg['E-Rate Disbursements'].sum()

# check for counties that won't merge cleanly
# list(set(df_erate_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# merge with df_geo_county to attach county names
df_county19_20 = pd.merge(left=df_county19_20, right=df_erate_agg,
                          left_on='County', right_on='County', how='left')

# df_county19_20.head()

# In[6]:

# #### 911 Circuits
# Metrics: 911 Circuits

# import psap data
# doesn't chage Y2Y
df_psap19_20 = pd.read_excel('Divisions/Divtel- PSAP/FY2021-2022/DivTel PSAP_Impacts_YTD.xlsx')

# df_psap19_20.columns

# add county to county name
# city data is too sparse to use
df_psap19_20['County'] = df_psap19_20['County'] + ' County'

# need county and 1 column to get count
df_psap19_20 = df_psap19_20[['County', 'PSAP name ']]

# replace county names that didn't make it into the merge
df_psap19_20.replace(to_replace=to_replace, inplace=True)

# group and agg to county level for merge at bottom
df_psap19_20_agg = df_psap19_20.groupby(['County']
                                        ).agg({
                                               'PSAP name ': 'count'
                                               }).reset_index()

# rename to desired metric name
df_psap19_20_agg.rename(columns={'PSAP name ': '911 Circuits'}, inplace=True)
# ready for merge

# check against dashboard to make sure we don't lose any
# df_psap19_20_agg['911 Circuits'].sum()

# check to see if any counties will get left out of the merge
# list(set(df_psap19_20_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# merge with psap county data
df_county19_20 = pd.merge(left=df_county19_20, left_on='County',
                          right=df_psap19_20_agg, right_on='County',
                          how='left')

# df_county19_20.head()

# In[7]:

# ### DSGI
# Metrics: Total Pharma Spend, Total Medical Spend

# 4 sheets with every combination of (medical,pharmacy) and (retirees,emps)
# skip footer =1 because DSGI, so kindly, included grand totals
# medical retirees
# home zip need to be a string, not a number or leading zeroes will be dropped
# causing issue with the merge, so we do a dtype=
df_himis0_2019 = pd.read_excel('Divisions/DSGI-HIMIS/FY2019-2020/DSGI HIMIS FY 19-20.xlsx',
                               sheet_name='2019 Medical Retiree',
                               skipfooter=1, dtype={'Home Zip': str})

# rename to match and be capitalized
df_himis0_2019.rename(columns={'Home Zip Code': 'Home Zip'}, inplace=True)

# get total spend for medical retirees
df_himis0_2019['Total Spend-MR'] = (df_himis0_2019['E - Enrollee'] +
                                    df_himis0_2019['Dependents'])

# we only want the total, I think
df_himis0_2019 = df_himis0_2019[['Home Zip', 'Total Spend-MR']]

# medical employees
# home zip need to be a string, not a number or leading zeroes will be dropped
# causing issue with the merge, so we do dtype= again
df_himis1_2019 = pd.read_excel('Divisions/DSGI-HIMIS/FY2019-2020/DSGI HIMIS FY 19-20.xlsx',
                               sheet_name='2019 Medical Employee',
                               dtype={'Home Zip': str})


# get total spend for medical employees
df_himis1_2019['Total Spend-ME'] = (df_himis1_2019['E - Enrollee'] +
                                    df_himis1_2019['Dependents'])

# we only want the total, I think
df_himis1_2019 = df_himis1_2019[['Home Zip', 'Total Spend-ME']]

# merge the 2 medical sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_medical_2019 = pd.merge(left=df_himis0_2019, left_on='Home Zip',
                           right=df_himis1_2019, right_on='Home Zip',
                           how='outer')

# total medical spend: retirees and employees, and their dependents
df_medical_2019['Total Medical Spend'] = (df_medical_2019['Total Spend-ME'] +
                                          df_medical_2019['Total Spend-MR'])

# pharma retirees
df_himis2_2019 = pd.read_excel('Divisions/DSGI-HIMIS/FY2019-2020//DSGI HIMIS FY 19-20.xlsx',
                               sheet_name='2019 Pharmacy Retiree',
                               dtype={'Home Zip': str})

df_himis2_2019['Total Spend-PR'] = (df_himis2_2019['E - Enrollee'] +
                                    df_himis2_2019['Dependents'])

# we only want the total, I think
df_himis2_2019 = df_himis2_2019[['Home Zip', 'Total Spend-PR']]

# pharma employees
df_himis3_2019 = pd.read_excel('Divisions/DSGI-HIMIS/FY2019-2020/DSGI HIMIS FY 19-20.xlsx',
                               sheet_name='2019 Pharmacy Employee',
                               dtype={'Home Zip': str})

df_himis3_2019['Total Spend-PE'] = (df_himis3_2019['E - Enrollee'] +
                                    df_himis3_2019['Dependents'])

# we only want the total, I think
df_himis3_2019 = df_himis3_2019[['Home Zip', 'Total Spend-PE']]

# merge the 2 pharma sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_pharma_2019 = pd.merge(left=df_himis2_2019, left_on='Home Zip',
                          right=df_himis3_2019, right_on='Home Zip',
                          how='outer')

# total pharma spend: retirees and employees, and their dependents
df_pharma_2019['Total Pharma Spend'] = (df_pharma_2019['Total Spend-PE'] +
                                        df_pharma_2019['Total Spend-PR'])

# merge medical and pharma into one dataset
df_himis_2019 = pd.merge(left=df_medical_2019, left_on='Home Zip',
                         right=df_pharma_2019, right_on='Home Zip',
                         how='outer')

# add up total HIMIS spend
df_himis_2019['Total HIMIS Spend'] = (df_himis_2019['Total Medical Spend'] +
                                      df_himis_2019['Total Pharma Spend'])

# get total employee spend
df_himis_2019['Total Employee Spend'] = (df_himis_2019['Total Spend-ME'] +
                                         df_himis_2019['Total Spend-PE'])

# get columns to make sure we get all we need during agg
# df_himis_2019.columns

# prepare to merge with df_geo_zip to get city names
# by grouping and agg'ing
df_himis_agg_2019 = df_himis_2019.groupby(['Home Zip'
                                           ]).agg({'Total Spend-MR': 'sum',
                                                   'Total Spend-ME': 'sum',
                                                   'Total Medical Spend': 'sum',
                                                   'Total Spend-PR': 'sum',
                                                   'Total Spend-PE': 'sum',
                                                   'Total Pharma Spend': 'sum',
                                                   'Total HIMIS Spend': 'sum'
                                                   }).reset_index()
# rename zip at this point to match df_geo_zip
df_himis_agg_2019.rename(columns={'Home Zip': 'Zip Code'}, inplace=True)

# change GeoFile zip code to str (object) type so merge will work
df_geo_zip['Zip Code'] = df_geo_zip['Zip Code'].astype(str)

# merge with df_geo_zip to attach city names
df_himis_agg_2019 = pd.merge(left=df_himis_agg_2019, left_on='Zip Code',
                             right=df_geo_zip, right_on='Zip Code',
                             how='left')

# remove rows where city is null--they aren't florida zip codes
# we filtered out non-FL zips from df_geo_zips when we did retirement
df_himis_agg_2019.dropna(axis=0, how='any', subset=(['City']), inplace=True)

df_himis_agg_2019.reset_index(inplace=True)

df_himis_agg_2019.replace(to_replace=to_replace, inplace=True)

# ready to group and agg in preparation for merge with df_geo_city
df_himis_agg_2019 = df_himis_agg_2019.groupby(['City']).agg({'Total Medical Spend':
                                                             'sum',
                                                             'Total Pharma Spend':
                                                             'sum',
                                                             'Total HIMIS Spend':
                                                             'sum'
                                                             }).reset_index()

# rename city to avoid duplication after the merge
df_himis_agg_2019.rename(columns={'city': 'City'}, inplace=True)

# merge with df_geo_city to attach district, county, etc information
# at the bottom; last step


# check totals of metrics
# df_himis_agg_2019['Total Medical Spend'].sum()


# check totals of metrics
# df_himis_agg_2019['Total Pharma Spend'].sum()


# check to see which cities won't make the merge
# list(set(df_himis_agg_2019['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# merge with DSGI HIMIS data
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_himis_agg_2019, right_on='City',
                        how='left')

df_city19_20.rename(columns={'Total MFMP Spend': 'MyFloridaMarketPlace',
                             'Total HIMIS Spend': 'Total Claims',
                             'Total Medical Spend': 'Medical Claims',
                             'Total Pharma Spend': 'Pharmacy Claims',
                             'Total Employee Spend': 'Total Employee Claims'},
                    inplace=True)

# df_city19_20.head()

# In[8]:

# ### DSS
# Metrics: Fleet Maintenance Cost, Fuel Cost, Private Prison Contract Spend, 
# Federal Surplus Savings

# #### FLEET
# Metrics: Total Operating Cost, Maintenance Cost, Fuel Cost

# read in fleet data
df_fleet19_20 = pd.read_excel('Divisions/DSS-Fleet/FY2019-2020/DSS FLEET FY 19-20.xlsx')

# show columns for slicing
# df_fleet19_20.columns

# df_fleet19_20.head()

# missing_data(df_fleet19_20)

# check for consistency with dashboard
# df_fleet19_20['TOTAL MAINTENANCE COST'].sum()

# replace dashes in TOTAL FUEL COSTS with zero
df_fleet19_20.replace('--', 0, inplace=True)
# convert to float
df_fleet19_20['TOTAL FUEL COSTS'] = df_fleet19_20['TOTAL FUEL COSTS'].astype(float)
# check for consistency with dashboard
# df_fleet19_20['TOTAL FUEL COSTS'].sum()

# check for consistency with dashboard
# df_fleet19_20['ASSET'].count()

# change CITY to City
df_fleet19_20.rename(columns={'CITY': 'City'},
                     inplace=True)
# change city to upper
df_fleet19_20['City'] = df_fleet19_20['City'].str.upper()
# slice columns
df_fleet19_20 = df_fleet19_20.loc[:, ['TOTAL OPERATING COST',
                                      'TOTAL MAINTENANCE COST',
                                      'TOTAL FUEL COSTS', 'City',
                                      'ASSET']]

df_fleet19_20.replace(to_replace=to_replace, inplace=True)

# aggregate to city level here so tableau doesn't have to
# only need city column, others will be duplicated in merge with final
# other geo columns are there, pre-agg, if we need them later
df_fleet19_20_agg = df_fleet19_20.groupby(['City']).agg({'TOTAL OPERATING COST':
                                                         'sum',
                                                         'TOTAL MAINTENANCE COST':
                                                         'sum',
                                                         'TOTAL FUEL COSTS':
                                                         'sum',
                                                         'ASSET': 'count'
                                                         }).reset_index()

# check to see which cities won't make the merge
# list(set(df_fleet19_20['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# need to merge existing df and fleet data, do this on city
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_fleet19_20_agg, right_on='City',
                        how='left')

df_city19_20.rename(columns={'TOTAL FUEL COSTS': 'Fuel Cost',
                             'TOTAL MAINTENANCE COST': 'Fleet Maintenance Cost',
                             'TOTAL OPERATING COST': 'Total Operating Cost',
                             'ASSET': 'Fleet Assets'},
                    inplace=True)

df_city19_20.head()

# In[9]:

# #### LESO
# Metrics: Federal Surplus Savings (dollars), Number of Items 

# this one comes in monthly sheets only, will need to concat
xl = pd.ExcelFile('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx')
xl.sheet_names

# import current fiscal year's leso data, concat months together
JUL_2019 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='JULY19')

AUG_2019 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='AUG 19')

SEP_2019 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='SEP 2019')

OCT_2019 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='OCT 2019')

NOV_2019 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='NOV 2019')

DEC_2019 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='DEC 2019')

JAN_2020 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='JAN 2020')

FEB_2020 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='FEB 2020')

MAR_2020 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='MARCH 2020')

APR_2020 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='APRIL 2020')

MAY_2020 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='MAY 2020')

JUN_2020 = pd.read_excel('Divisions/DSS-LESO/FY2019-2020/DSS LESO FY 19-20.xlsx',
                         sheet_name='JUNE 2020')

# concat sheets together vertically
# months to come
years19_20 = [JUL_2019, AUG_2019, SEP_2019, OCT_2019, NOV_2019, DEC_2019,
              JAN_2020, FEB_2020, MAR_2020, APR_2020, MAY_2020, JUN_2020]

df_leso19_20 = pd.concat(years19_20, ignore_index=True, axis=0)

# need to drop months with rows that are blanks
df_leso19_20.dropna(subset=['Law Enforcement\nAgency Name'],
                    axis=0, how='all', inplace=True)


df_leso19_20 = df_leso19_20.reset_index()

# fix dtypes
df_leso19_20['Initial Acquisition\nCost (IAC)'] = df_leso19_20['Initial Acquisition\nCost (IAC)'].astype(float)

df_leso19_20['Service Charge'] = df_leso19_20['Service Charge'].astype(float)

df_leso19_20.dtypes
# for easy copy pasting

df_leso19_20.rename(columns={'Law Enforcement\nAgency Name': 'LE Agency Name'},
                    inplace=True)

df_leso19_20.replace(to_replace=to_replace, inplace=True)

df_leso19_20['Savings'] = df_leso19_20['Initial Acquisition\nCost (IAC)'] - df_leso19_20['Service Charge']

# change LE to city, since thats what it is, now
df_leso19_20.rename(columns={'LE Agency Name': 'City'}, inplace=True)

# only get columns we need
df_leso19_20 = df_leso19_20[['Total Quantity\nof Items', 'Savings', 'City']]

# check to see which cities won't make the merge
# list(set(df_leso19_20['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# group and agg to the city level
df_leso_agg19_20 = df_leso19_20.groupby(['City']
                                        ).agg({'Savings': 'sum',
                                               'Total Quantity\nof Items':
                                               'sum'}
                                              ).reset_index()

# merge with LESO city-level data
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_leso_agg19_20, right_on='City',
                        how='left')

df_city19_20.rename(columns={'Total Quantity\nof Items':
                             'LESO Items',
                             'Savings': 'LESO Savings',
                             },
                    inplace=True)

# df_city19_20.head()

# In[10]:

# #### SASP
# Metrics: SASP Savings, SASP Items

# import and list columns
df_sasp19_20 = pd.read_excel('Divisions/DSS-SASP/FY2019-2020/DSS SASP FY 19-20.xlsx',
                             sheet_name='all')

# df_sasp19_20.columns

# preview dataset
# df_sasp19_20.head()

# only get columns we need
df_sasp19_20 = df_sasp19_20[['CUSTOMER', 'LINE ITEMS', 'Savings']]

df_sasp19_20.rename(columns={'CUSTOMER': 'Donee Name'
                             },
                    inplace=True)

# import donee locations to attach to first dataset
df_donees = pd.read_csv('Divisions/DSS-SASP/FY2019-2020/Donee City/Donee City FY 19-20.csv')

# df_donees.columns

# df_donees.head()

df_donees.rename(columns={'Donee Account Name': 'Donee Name'
                          },
                 inplace=True)

# group account names from donee file so rows aren't duplicated
df_donees = df_donees.groupby(by=['Donee Name']).agg({'City':
                                                      'first'
                                                      }).reset_index()

# check for cities that didn't make it
# list(set(df_sasp19_20['Donee Name'].to_list()).difference(list(set(df_donees['Donee Name'].to_list()))))

# merge to attach city names to sasp data
df_sasp19_20 = pd.merge(left=df_sasp19_20, left_on='Donee Name',
                        right=df_donees, right_on='Donee Name',
                        how='left')

# df_sasp19_20.head()

# group and agg to the city level
df_sasp19_20_agg = df_sasp19_20.groupby(['City']).agg({
                                                       'LINE ITEMS': 'count',
                                                       'Savings':
                                                       'sum',
                                                       }).reset_index()

df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_sasp19_20_agg, right_on='City',
                        how='left')

df_city19_20.rename(columns={'Savings': 'SASP Savings',
                             'LINE ITEMS': 'SASP Items'
                             },
                    inplace=True)

# df_city19_20['SASP Savings'].sum()

# df_city19_20.head()

# In[11]:

# #### Private Prison Monitoring
# Metrics: Total Private Prison Spend

# read private prison data
# it is already agg'd, so no need to
df_ppm19_20 = pd.read_excel('Divisions/DSS-Private Prisons/FY2019-2020/DSS Private Prisons FY 19-20.xlsx',
                            sheet_name=7, header=1)

df_ppm19_20 = df_ppm19_20.loc[:28, ['Facility',
                                    'Reimbursement to Vendor w/Deductions']]

df_ppm19_20.loc[2, 'Facility'] = 'BLACKWATER'

# need to join facility names with addresses frm dms website
df_ppm_location = pd.read_excel('Geo Files for Merging/ppm_locations.xlsx')

# join to attach addresses
# inner join to get rid of extra unnecessary rows
df_ppm19_20_city = pd.merge(left=df_ppm19_20, left_on='Facility',
                            right=df_ppm_location, right_on='Facility',
                            how='inner')

# no longer need name of facility - for now
df_ppm19_20_city = df_ppm19_20_city.iloc[:, 1:]

# rename to something a little digestible
df_ppm19_20_city.rename(columns={'Reimbursement to Vendor w/Deductions':
                                 'Total Private Prison Spend'}, inplace=True)

# df_ppm19_20_city['Total Private Prison Spend'].sum()

# check to see which cities won't make the merge
# list(set(df_ppm19_20_city['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# merge with PPM data
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_ppm19_20_city, right_on='City',
                        how='left')

df_city19_20.rename(columns={'Total Private Prison Spend':
                             'Private Prison Contract Spend'},
                    inplace=True)

# df_city19_20.head()

# In[12]:

# ### MyFloridaMarketPlace
# Metrics: Total MFMP Spend, Number of Vendors

filepath = 'Divisions/MFMP/FY2019-2020/DMS Government Impact Dashboard Report FY2019-2020.xlsx'

df_mfmp19_20 = pd.read_excel(filepath, dtype={'sum(Invoice Spend)': str})

# list columns, only need a couple
# df_mfmp19_20.columns

# filter out non-FL impact transactions
df_mfmp19_20 = df_mfmp19_20[df_mfmp19_20['Supplier Location - PO State'] == 'FL'].reset_index()

# number of transactions left is substantial ~180K
# len(df_mfmp19_20)

# df_mfmp19_20.dtypes

# select only the columns we need
df_mfmp19_20 = df_mfmp19_20.loc[:, ['Supplier Location - PO City', 'sum(Invoice Spend)',
                                    'Supplier - Company Name']]

# rename columns
df_mfmp19_20.rename(columns={'Supplier Location - PO City': 'City',
                             'sum(Invoice Spend)': 'MFMP Spend',
                             'Supplier - Company Name': 'Company Name'},
                    inplace=True)

# capitalize city for eventual merge
df_mfmp19_20['City'] = df_mfmp19_20['City'].str.upper()

# go ahead and do all replacements, adding new ones
df_mfmp19_20.replace(to_replace=to_replace, inplace=True)

# drop the ones that were ambiguous or not in FL
df_mfmp19_20 = df_mfmp19_20[df_mfmp19_20['City'].isin(to_drop) == False]

# change MFMP spend to numeric column so agg will work
df_mfmp19_20['MFMP Spend'] = df_mfmp19_20['MFMP Spend'].str.replace(',', '', regex=True)
df_mfmp19_20['MFMP Spend'] = df_mfmp19_20['MFMP Spend'].astype('float')

# we have some extra vendors in our db
# df_mfmp19_20['Company Name'].value_counts()

# group and agg to city level
df_mfmp19_20_agg = df_mfmp19_20.groupby(['City']).agg({'MFMP Spend': 'sum',
                                                       'Company Name':
                                                       'nunique'}).reset_index()
# rename metric to desired name
df_mfmp19_20_agg.rename(columns={
                                 'Company Name': 'Vendors'
                                 },
                        inplace=True)

# check metric totals for consistency with dashboard
# df_mfmp19_20_agg['MFMP Spend'].sum()

# ended up with 500 more vendors somehow
# df_mfmp19_20_agg['Vendors'].sum()

# check to see which cities won't make the merge
# list(set(df_mfmp19_20_agg['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# merge master with mfmp
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_mfmp19_20_agg, right_on='City',
                        how='left')

df_city19_20.head()

# In[13]:

# ### OSD
# Metrics: Number of Minority-Owned Businesses Registered

df_osd19_20 = pd.read_excel('Divisions/OSD/FY2021-2022/OSD Certified Vendor File.xlsx')

# df_osd19_20.columns

# df_osd19_20.head()
# many dates are not of this fiscal year

# missing_data(df_osd19_20)

# only grab registrations active for FY 19-20
# generate dates for entire FY
rng = pd.date_range('07-01-2019', '06-30-2020', periods=365).to_frame()

# place in datframe
df_FY19_20 = pd.DataFrame(rng[0].astype(str)).reset_index()

# split the date component and the time component
df_FY19_20['Time'] = df_FY19_20[0].str.split(' ')

# grab the date portion
df_FY19_20['Time'] = df_FY19_20['Time'].str[0]

# place in a list
wrong_format = df_FY19_20['Time'].to_list()

# must change to our precious format
FY19_20 = []

# change to match OSD date format
for i in wrong_format:
    temp = i.split('-')
    FY19_20.append(temp[1]+'/'+temp[2]+'/'+temp[0])

# if date is in our range, grab it


def inYear(row):
    if (row['Effective On'] in FY19_20) or (row['Expire On'] in FY19_20):
        return '1'
    else:
        return '0'


# apply a marker to our df that we can use to filter it
df_osd19_20['Indic'] = df_osd19_20.apply(inYear, axis=1)

df_osd19_20 = df_osd19_20[df_osd19_20['Indic'] == '1']

# we should be getting at least 4 businesses
# df_osd19_20.head()

# conver to upper to match df_geo
df_osd19_20['City'] = df_osd19_20['City'].str.upper()

# replace cities that won't make the merge
# because they don't appear in df_geo_city
df_osd19_20.replace(to_replace=to_replace, inplace=True)

# drop known-issue cities
df_osd19_20 = df_osd19_20[df_osd19_20['City'].isin(to_drop) == False]

# group and agg to the city level
df_osd19_20_agg = df_osd19_20.groupby(['City']).agg({'Name':
                                                     'count'}
                                                    ).reset_index()

# rename to match other years
df_osd19_20_agg.rename(columns={'Name':
                                'Minority-Owned Businesses Registered'},
                       inplace=True)

# check for cities that didn't make it
# list(set(df_osd19_20['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# merge on city with main fy 19-20 dataset
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_osd19_20_agg, right_on='City',
                        how='left')

# df_city19_20.head()

# In[14]:

# ### PeopleFirst
# Metrics: Employees, Vacancies, Positions, Annualized Salary

filepath = 'Divisions/People First - SPS/FY2019-2020/PF State Positions Locations.xlsx'

df_pf19_20 = pd.read_excel(filepath)

# df_pf19_20.columns

# df_pf19_20.head()

# before we do anything, join with location information in other file they sent
filepath = 'Divisions/People First - SPS/FY2019-2020/PF FY 19-20.xlsx'

df_pf19_20_b = pd.read_excel(filepath)

# df_pf19_20_b.columns

# df_pf19_20_b.head()

# see which we can join on, ie are in both datasets
[column for column in df_pf19_20.columns if column in df_pf19_20_b]

# drop the ones that were ambiguous or not in FL
# len(df_pf19_20_b[(df_pf19_20_b['Employee Type'] == 4) | (df_pf19_20_b['Employee Type'] == 5)])

# want to 'attach' info in _b, such as Base Rate of Pay, etc to left df
df_pf19_20 = pd.merge(left=df_pf19_20_b, left_on='Pos Num (8 Digits)',
                      right=df_pf19_20, right_on='Pos Num (8 Digits)',
                      how='left')

# only keep the columns we need
df_pf19_20 = df_pf19_20[['Location City', 'Vacant', 'Pos Num (8 Digits)',
                         'Base Rate Of Pay', 'St Health Cov Code',
                         'Pay Type Code', 'Pos FTE', 'Emp FTE',
                         'Employee Type']]

# split off OPS to be a separate metric
df_ops = df_pf19_20[(df_pf19_20['Employee Type'] == 4) | (df_pf19_20['Employee Type'] == 5)]

df_ops = df_ops.reset_index()

# len(df_ops)

# make sure cities are upper for the merge
df_ops['Location City'] = df_ops['Location City'].str.upper()

df_ops = df_ops[df_ops['Vacant'] != 'Y']

# len(df_ops)

df_ops['Location City'].fillna('UNDEFINED', inplace=True)

df_ops.replace(to_replace={' ': 'UNDEFINED'}, inplace=True)

df_ops_agg = df_ops.groupby(['Location City']).agg({'Pos Num (8 Digits)':
                                                    'nunique'}).reset_index()

df_ops_agg.rename(columns={'Pos Num (8 Digits)': 'OPS Employees'}, inplace=True)

# df_ops_agg['OPS Employees'].sum()

df_ops_agg.rename(columns={'Location City': 'City'}, inplace=True)

# ready for merge with master for the year
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_ops_agg, right_on='City',
                        how='left')

# filter down to OPS employees only 
df_pf19_20 = df_pf19_20[(df_pf19_20['Employee Type'] == 1) | (df_pf19_20['Employee Type'] == 2)]

# len(df_pf19_20[(df_pf19_20['Employee Type'] == 4) | (df_pf19_20['Employee Type'] == 5)])

# missing_data(df_pf19_20)
# not too bad, base rate missing are vacancies

# convert base rate of pay into annual salary
def convertSalary(row):
    # B is biweekly, M is monthly
    if row['Pay Type Code'] == 'M':
        return row['Base Rate Of Pay']*12
    elif row['Pay Type Code'] == 'B':
        return row['Base Rate Of Pay']*26


# convert base rate of pay into annual salary, for emp type 4 and 5 (hourly)
def convertSalary2(row):
    # B is biweekly, M is monthly
    if row['Employee Type'] == '4':
        return row['Base Rate Of Pay']*row['Emp FTE']*2000
    if row['Employee Type'] == '5':
        return row['Base Rate Of Pay']*row['Emp FTE']*2000
    else:
        return convertSalary(row)


df_pf19_20['Annualized Salary'] = df_pf19_20.apply(convertSalary2, axis=1)

# df_pf19_20['Annualized Salary'].sum()

# go ahead and do all replacements, adding new ones
df_pf19_20.replace(to_replace=to_replace, inplace=True)

# drop the ones that were ambiguous or not in FL
df_pf19_20 = df_pf19_20[df_pf19_20['Location City'].isin(to_drop) == False]

# check to see which cities won't make the merge
# list(set(df_pf19_20['Location City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# replace vacancy indicator with 1 for agg'ing
df_pf19_20.replace("Y", 1, inplace=True)

# len(df_pf19_20)

df_pf19_20['Location City'].fillna('UNDEFINED', inplace=True)

df_pf19_20.replace(to_replace={' ': 'UNDEFINED'}, inplace=True)

df_pf19_20_agg = df_pf19_20.groupby(['Location City']).agg({'Vacant': 'sum',
                                                            'Annualized Salary':
                                                            'sum',
                                                            'Pos Num (8 Digits)':
                                                            'nunique',
                                                            'Emp FTE':
                                                            'sum'}).reset_index()

# df_pf19_20_agg.head()

# change name to city so columns won't duplicate on merge
df_pf19_20_agg.rename(columns={'Location City': 'City',
                               'Pos Num (8 Digits)': 'Positions',
                               'Emp FTE': 'Employees',
                               'Vacant': 'Vacancies'}, inplace=True)

# make sure cities are upper for the merge
df_pf19_20_agg['City'] = df_pf19_20_agg['City'].str.upper()

# df_pf19_20_agg.head()

# ready for merge with master for the year
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_pf19_20_agg, right_on='City',
                        how='left')

# df_pf19_20_agg['Vacancies'].sum()

# df_pf19_20_agg['Positions'].sum()

# df_pf19_20_agg['Employees'].sum()

# df_pf19_20_agg['Annualized Salary'].sum()

# df_city19_20.head()

# In[15]:

# ### REDM
# Metrics: Owned Sq. Footage, Facilities Owned, Maintenance (Operating) Cost,
# Utility Bills, Structure Value, Land Value

df_owned19_20 = pd.read_excel('Divisions/REDM-SOLARIS/FY2019-2020/REDM SOLARIS FY 19-20.xlsx')

# df_owned19_20.columns

# df_owned19_20.head()

# get dms blds only per secretary
df_owned19_20 = df_owned19_20[df_owned19_20['Agency Name'] == 'Department of Management Services']

# get necessary columns only
df_owned19_20 = df_owned19_20[['FL-SOLARIS Facility #',
                               'Facility City', 'Gross Sq Ft',
                               'Taxroll Land Value',
                               'Taxroll Structure Value',
                               'Total Utility Cost',
                               'Operating Cost']]

# Rename columns to match with the required metric values
df_owned19_20.rename(columns={'Gross Sq Ft': 'Owned Square Footage',
                              'Taxroll Land Value': 'Land Value',
                              'Taxroll Structure Value': 'Structure Value',
                              'FL-SOLARIS Facility #': 'ID',
                              'Facility City': 'City',
                              'Total Utility Cost': 'Utility Bills Paid',
                              'Operating Cost': 'Building Maintenance'
                              },
                     inplace=True)

# capitalize facility city
df_owned19_20['City'] = df_owned19_20['City'].str.upper()

# replace county names that didn't make it into the merge
df_owned19_20.replace(to_replace=to_replace, inplace=True)

# ambiguous or other state's cities must be dropped
df_owned19_20 = df_owned19_20[df_owned19_20['City'].isin(to_drop) == False]

# Aggregating and renaming metrics from the Owned file
df_owned19_20_agg = df_owned19_20.groupby(['City'
                                           ]).agg({'ID': 'count',
                                                   'Owned Square Footage':
                                                   'sum',
                                                   'Land Value': 'sum',
                                                   'Structure Value':
                                                   'sum',
                                                   'Utility Bills Paid': 'sum',
                                                   'Building Maintenance': 'sum'
                                                   }).reset_index()
                                                   
df_owned19_20_agg.rename(columns={'ID': 'Facilities Owned'}, inplace=True)
# ready for merge

# check owned to see if any cities won't make the merge
# empty set is good
# list(set(df_owned19_20_agg['City'].str.upper().to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# Merge with df_geo_city to attach location information at city level
df_city19_20 = pd.merge(left=df_city19_20, right=df_owned19_20_agg,
                        left_on='City', right_on='City',
                        how='left')

df_city19_20.head()

# In[16]:

# ### RET

# #### Payments and Payees
# Metrics: Benefits Paid, Number of Payees

# import historical retirement data
df_ret19 = pd.read_excel('Divisions/RET-IRIS/old FYs/20211220 dashboard data for FY19 FY20 FY21.xlsx',
                         dtype={'Fiscal Year': str, 'Florida Zip Code': str})
# trim off rows from other states
# set type of fiscal year to str to get whole #s

# df_ret19.columns

# df_ret19.dtypes

# check our work
# df_ret19.head()

# exclude FYs 20-21, 21-22
df_ret19 = df_ret19[df_ret19['Fiscal Year'] == '2020'].reset_index()

# check our work
# df_ret19.head()

# get rid of unnecessary columns
df_ret19 = df_ret19.iloc[:, 2:]

# check for missing data
# missing_data(df_ret19)

# make replacements
df_ret19.replace(to_replace=to_replace, inplace=True)

# track down cities that won't make the merge
# list(set(df_ret19['Florida Zip Code'].to_list()).difference(list(set(df_geo_zip['Zip Code'].to_list()))))

# drop zips that aren't in state, all were looked up
drop_zips = ['75791',
             '38221',
             '31646',
             '07070',
             '07024',
             '28904',
             '30076',
             '75039',
             '46590']

for Zip in drop_zips:
    df_ret19 = df_ret19[df_ret19['Florida Zip Code'] != Zip]

# track down cities that won't make the merge
# list(set(df_ret19['Florida Zip Code'].to_list()).difference(list(set(df_geo_zip['Zip Code'].to_list()))))

# drop city, its full of garbage
# don't need fy, every row will be fy 19 for now
df_ret19.drop(['City Name', 'Fiscal Year'], axis=1, inplace=True)

df_ret19.rename(columns={'Florida Zip Code': 'Zip Code'}, inplace=True)

# df_ret19.dtypes

# attach city names by merging with df_geo_zip
df_ret19 = pd.merge(left=df_ret19, left_on='Zip Code',
                    right=df_geo_zip, right_on='Zip Code',
                    how='left')

# check our work
# df_ret19.head()

# group and agg to the city level
df_ret19_agg = df_ret19.groupby(['City']).agg({'Payment Amount':
                                               'sum',
                                               'Number of Payees':
                                               'sum'
                                               }).reset_index()

# create overall df for fy 19-20 data
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_ret19_agg, right_on='City',
                        how='left')

# df_city19_20.head(510)

# #### Employers and Contributions
# 
# Metrics: Number of Employers, Employer Contributions

# import third sheet of ret data
df_ret19_2 = pd.read_excel('Divisions/RET-IRIS/old FYs/20211220 dashboard data for FY19 FY20 FY21.xlsx',
                           sheet_name=2, dtype={'City Name': str,
                                                'Fiscal Year': str})

# cut down to only columns we need
df_ret19_2 = df_ret19_2[['Agency Name', 'City Name',
                         'Employer Contribution Amount',
                         'Fiscal Year']]

# rename columns
df_ret19_2.rename(columns={'Fiscal Year': 'FY',
                           'City Name': 'City'}, inplace=True)

# exclude FYs 20-21, 21-22
df_ret19_2 = df_ret19_2[df_ret19_2['FY'] == '2020'].reset_index()

# exclude fy 20-21, 21-22 for now
# df_ret19_2.head()

# make replacements to fix known cityname issues
df_ret19_2.replace(to_replace=to_replace, inplace=True)

# track down cities that won't make the merge
# list(set(df_ret19_2['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# we have these already in to_drop, so drop
df_ret19_2 = df_ret19_2[df_ret19_2['City'].isin(to_drop) == False]

# track down cities that won't make the merge
# list(set(df_ret19_2['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# check missing data
# missing_data(df_ret19_2)
# almost perfect

# group and agg to city level
df_ret19_2_agg = df_ret19_2.groupby(['City']).agg({'Agency Name':
                                                   'nunique',
                                                   'Employer Contribution Amount':
                                                   'sum'
                                                   }).reset_index()
# FY will get dropped out bc we didnt specify it

# merge on city with main fy 19-20 dataset
df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_ret19_2_agg, right_on='City',
                        how='left')

# rename to accurately represent aggregations
df_city19_20.rename(columns={'Agency Name': 'Number of Employers Using Retirement System'},
                    inplace=True)

df_city19_20.rename(columns={'Payment Amount': 'Benefits Paid to Retirees',
                             'Employer Contribution Amount':
                             'Employer Contributions'},
                    inplace=True)

# df_city19_20['Number of Payees'].sum()

# df_city19_20['Number of Employers Using Retirement System'].sum()

# df_city19_20['Benefits Paid to Retirees'].sum()

# df_city19_20['Employer Contributions'].sum()

# df_city19_20.head()

# In[17]:

# ### STMS
# Metrics: STMS Travel Spend

# separate fy 18-10 from the rest
df_stms19_20 = pd.read_excel('Divisions/STMS/FY2019-2020/STMS FY 19-20.xlsx')

# len(list(set(df_stms19_20['Form ID: * Destination'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# create list of cities to search for
city_list = df_geo_city['City'].to_list()


def cityFinder(row):
    # iterate over every name in the city list
    for city in city_list:
        # if our city appears anywhere, return it
        if str(city) in str(row['Form ID: * Destination']).upper():
            return city
        # else leave it alone
        else:
            pass


df_stms19_20['City'] = df_stms19_20.apply(cityFinder, axis=1)

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


df_stms19_20['City2'] = df_stms19_20.apply(countyFinder, axis=1)

df_stms19_20['City'].fillna(df_stms19_20['City2'].str.upper(), inplace=True)

df_stms19_20['City'].fillna(df_stms19_20['Form ID: * Destination'].str.upper(), inplace=True)

# len(list(set(df_stms19_20['City'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# make replacements we have collected, maybe will make a difference
df_stms19_20.replace(to_replace=to_replace, inplace=True)

# drop the ones that were ambiguous or not in FL
df_stms19_20 = df_stms19_20[df_stms19_20['City'].isin(to_drop) == False]

# len(list(set(df_stms19_20['City'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# df_stms19_20.columns

# df_stms19_20.head()

df_stms19_20_agg = df_stms19_20.groupby(['City']).agg({'Total Amount': 'sum'
                                                       }).reset_index()

df_stms19_20_agg = df_stms19_20_agg[['City', 'Total Amount']]

df_stms19_20_agg.rename(columns={'Total Amount': 'Travel Spend',
                                 },
                        inplace=True)

df_city19_20 = pd.merge(left=df_city19_20, left_on='City',
                        right=df_stms19_20_agg, right_on='City',
                        how='left')

# df_city19_20.head()

# In[18]:

# ### Year Processing

# add FY to the county data frame after all merges for that year
df_county19_20['FY'] = '19-20'

# add FY to the data frame after all merges for that year
df_city19_20['FY'] = '19-20'

# df_city19_20.head()

# df_city19_20.dtypes


# ## Export

# ### County

# df_county19_20.dtypes

df_county19_20.to_excel('Fiscal Years/County Level FY 2019-2020.xlsx')

# ### City

# df_city19_20.dtypes


df_city19_20.to_excel('Fiscal Years/City Level FY 2019-2020.xlsx')

# In[19]:

# ## Time
executionTime = (time.time() - startTime)
# convert to mins
mins = str(round(executionTime/60))
secs = str(round(executionTime % 60, 2))

print('Execution time: {} minutes, {} seconds: '.format(mins, secs))
