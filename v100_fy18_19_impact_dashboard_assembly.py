# -*- coding: utf-8 -*-
"""
Created on Fri May  6 12:55:45 2022

@author: MairsB
"""

#!/usr/bin/env python
# coding: utf-8

# # FY 18-19 Impact Dashboard Data Assembly

# In[1]:

# ## Import Libraries and Files

# time how long script takes
import time
startTime = time.time()

# import standard data manipulation libraries
import pandas as pd
import numpy as np
# import other libraries as necessary

# Read in the geographic file of cities,counties and legislative districts
filepath = 'Geo Files for Merging/GeoFile-Copy1.csv'
df_geo_city = pd.read_csv(filepath)

# drop duplicates with exact same city name
df_geo_city.drop_duplicates(subset='City', keep='first', inplace=True)

# initiliaze city data frame
df_city18_19 = pd.DataFrame(data=df_geo_city)
# we will join required datasets to this df

# get separate county dataset by getting list of FL counties
# tableau already has shape info for counties, so we only need the name
df_geo_county = df_geo_city[['County']].drop_duplicates(keep='first')

# initiliaze county data frames for each FY
df_county18_19 = pd.DataFrame(data=df_geo_county)
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
# this dict was built by differencing lists of df_geo cities and dms data with:
# list(set(<dataframe_column>.to_list()).difference(list(set(df_geo_city['City'].to_list()))))
# this generates a unique list of cities that don't match df_geo
# can also be used for zip codes by replacing df_geo_city to df_geo_zip['Zip Code']

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
    
# ## FY 2018-2019

# ### DivTel
# Metrics: Internet Circuits, Value of Internet Circuits

# #### Internet Circuits
# Metrics: Number of Internet Circuits, Value of Internet Circuits

df_csab18_19 = pd.read_excel('Divisions/Divtel- CSAB- Internet Circuits/DivTel CSAB Inventory Retail FY 18-19.xlsx',
                            dtype={'ZIPCODE': str})
# df_csab18_19.columns

# missing_data(df_csab18_19)

# Get the necessary columns from CSAB data
df_csab18_19 = df_csab18_19[['ZIPCODE', 'CIRCUITS', 'CHARGE']]

# check which zips aren't in our flzips geofile
# list(set(df_csab18_19['ZIPCODE'].to_list()).difference(list(set(df_geo_zip['Zip Code'].to_list()))))

# join with flzips to attach city names
df_csab18_19 = pd.merge(left=df_csab18_19, left_on='ZIPCODE',
                        right=df_geo_zip, right_on='Zip Code',
                        how='left')

# check for cities that won't merge cleanly
# list(set(df_csab18_19['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# make replacements to ensure merge with df_geo_city is smooth
df_csab18_19.replace(to_replace=to_replace, inplace=True)

# drop known issue cities
df_csab18_19 = df_csab18_19[df_csab18_19['City'].isin(to_drop) == False]

# check for cities that won't merge cleanly
# list(set(df_csab18_19['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# aggregate CSAB data to the city level in preparation for merge
df_csab18_19_agg = df_csab18_19.groupby(df_csab18_19['City'
                                                     ].str.upper()
                                        ).agg({'CIRCUITS': 'sum',
                                               'CHARGE': 'sum'
                                               }).reset_index()

# rename columns
df_csab18_19_agg.rename(columns={'CIRCUITS': 'Internet Circuits',
                                 'CHARGE': 'Value of Internet Circuits'
                                 },
                        inplace=True)

# check sum of assets and retail value against DB
# df_csab18_19_agg['Internet Circuits'].sum()

# check sum of assets and retail value against DB
df_csab18_19_agg['Value of Internet Circuits'].sum()


# circuits are duplicated, because this FY they transferred ownership
# so we cut all the numbers in half, won't need to do for any other year
import math
def cut_in_half(row):
    return math.ceil(row / 2)


df_csab18_19_agg['Internet Circuits'] = df_csab18_19_agg['Internet Circuits'].apply(cut_in_half)

# better
# df_csab18_19_agg['Internet Circuits'].sum()

# Merge csab data with master set
df_city18_19 = pd.merge(left=df_city18_19, right=df_csab18_19_agg,
                        left_on='City', right_on='City',
                        how='left')

# check our work
# df_city18_19.head()

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
df_towers_agg = df_towers.groupby(['County']).agg({'Name': 'count'}).reset_index()

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
df_county18_19 = pd.merge(left=df_county18_19, left_on='County',
                          right=df_towers_agg, right_on='County',
                          how='left')

# df_county18_19.head()

# In[3.5]:

# #### 911 Disbursements
# Level: County Level

# Metrics: E911 Disbursements

# import grants information
df_disb18_19 = pd.read_excel('C:\\Users\\mairsb\\OneDrive - Florida Department of Management Services\\07-Strategic Planning\\02-Projects\\Government Impact Dashboard Project\\Implementation\\Data Assembly\\Divisions\\DivTel - 911 Disbursements\\FY2018-2019\\E911 County Monthly  - Copy.xlsx',
                             skipfooter=1)

# df_disb18_19.columns

# df_disb18_19.tail(55)

# slice off the first row

df_disb18_19 = df_disb18_19[1:]

# change name to county
df_disb18_19.rename(columns={'Unnamed: 0': 'County',
                             'Total': 'E911 Disbursements'}, inplace=True)

# add 'County' to county field for merge prep
df_disb18_19['County'] = df_disb18_19['County'] + ' County'

# check to make sure there are no misspellings
# list(set(df_disb18_19['County'].to_list()).difference(df_geo_county['County'].to_list()))

# group and agg is not really necessary, but it can't hurt

df_disb18_19_agg = df_disb18_19.groupby(['County']).agg(
                                                        {'E911 Disbursements':
                                                         'sum'
                                                         }
                                                          ).reset_index()
# we're good, go ahead and merge
df_county18_19 = pd.merge(left=df_county18_19, left_on='County',
                          right=df_disb18_19_agg, right_on='County',
                          how='left')
# df_county21_22.head()

# In[4]:

# #### 911 Grants
# Level: County Level
# Metrics: Grants Awarded

# import grants information
df_grants18_19 = pd.read_excel('Divisions/Divtel - 911 Grants/FY2018-2019/DivTel 911 Grants FY 18-19.xlsx')

# df_grants18_19.columns

# df_grants18_19.head()

# Grants Data
df_grants18_19 = df_grants18_19[['County Name', 'FinalAward']]

# rename
df_grants18_19.rename(columns={'FinalAward': 'Grants Awarded',
                               'County Name': 'County'
                               },
                      inplace=True)

# add county to county name
df_grants18_19['County'] = df_grants18_19['County'] + ' County'

# group and agg to city level
df_grants18_19_agg = df_grants18_19.groupby(['County'
                                             ]).agg({
                                                     'Grants Awarded':
                                                     'sum'
                                                     }).reset_index()

# check sum of grants against dashboard
# df_grants18_19_agg['Grants Awarded'].sum()

# see if any counties won't make the merge
# list(set(df_grants18_19_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# Merge grants and e-rate disbursements to generate a county-level data frame
df_county18_19 = pd.merge(left=df_county18_19, right=df_grants18_19_agg,
                          left_on='County', right_on='County', how='left')

# df_county18_19.head()

# In[5]:

# #### E-Rate Discounts, FY 2020-2021
# Metrics: E-Rate Disbursements

# import e-rate data
df_erate = pd.read_excel('Divisions/Divtel- E-rate Organizer/FY2018-2019/DivTel E-rate FY 2018-2019.xlsx',
                         dtype={'Total Authorized Disbursement': str})

# Clean up the E-Rate data that is reported as $ strings
df_erate = df_erate[['County', 'Total Authorized Disbursement']]

# get rid of dollar signs, we can format in tableau
df_erate['E-Rate Disbursements'] = df_erate['Total Authorized Disbursement'
                                            ].str.lstrip('$')

# add county name with space to match with df_geo-county
df_erate['County'] = df_erate['County'] + ' County'

# replace all found replacements to ensure merge with df_geo_county works
df_erate.replace(to_replace=to_replace, inplace=True)
df_erate.replace(to_replace={'Dade County': 'Miami-Dade County'}, inplace=True)

# strip commas so agg will work
df_erate.replace({',': ''}, regex=True, inplace=True)

# change type to float so agg will work
df_erate['E-Rate Disbursements'] = df_erate['E-Rate Disbursements'].str.strip(' ').astype(float)

# group and agg to city level
df_erate_agg = df_erate.groupby(['County'
                                 ]).agg({'E-Rate Disbursements':
                                         'sum'}).reset_index()

# df_erate_agg['E-Rate Disbursements'].sum()

# check for counties that won't merge cleanly
# list(set(df_erate_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# merge with df_geo_county to attach county names
df_county18_19 = pd.merge(left=df_county18_19, right=df_erate_agg,
                          left_on='County', right_on='County', how='left')

# df_county18_19.head()

# In[6]:

# #### 911 Circuits
# Metrics: 911 Circuits

# import psap data
# doesn't chage Y2Y
df_psap18_19 = pd.read_excel('Divisions/Divtel- PSAP/FY2021-2022/DivTel PSAP_Impacts_YTD.xlsx')

# add county to county name
# city data is too sparse to use
df_psap18_19['County'] = df_psap18_19['County'] + ' County'

# df_psap18_19.columns

# need county and 1 column to get count
df_psap18_19 = df_psap18_19[['County', 'PSAP name ']]

# replace county names that didn't make it into the merge
df_psap18_19.replace(to_replace=to_replace, inplace=True)

# group and agg to county level for merge at bottom
df_psap18_19_agg = df_psap18_19.groupby(['County']
                                        ).agg({
                                               'PSAP name ': 'count'
                                               }).reset_index()

# rename to desired metric name
df_psap18_19_agg.rename(columns={'PSAP name ': '911 Circuits'}, inplace=True)
# ready for merge

# check against dashboard to make sure we don't lose any
# df_psap18_19_agg['911 Circuits'].sum()

# check to see if any counties will get left out of the merge
# list(set(df_psap18_19_agg['County'].to_list()).difference(list(set(df_geo_county['County'].to_list()))))

# merge with psap county data
df_county18_19 = pd.merge(left=df_county18_19, left_on='County',
                          right=df_psap18_19_agg, right_on='County',
                          how='left')

# df_county18_19.head()

# In[7]:

# ### DSGI
# Metrics: Total Pharma Spend, Total Medical Spend

# 4 sheets with every combination of (medical,pharmacy) and (retirees,employees)
# start withmedical retirees
df_himis0_2018 = pd.read_excel('Divisions/DSGI-HIMIS/FY2018-2019/DSGI HIMIS FY 18-19.xlsx',
                               # because grand totals at bottom
                               skipfooter=1,
                               # home zip need to be a string, not a number or leading zeroes will be dropped
                               dtype={'Home Zip Code': str})
# df_himis0_2018.columns

# rename to match and be capitalized
df_himis0_2018.rename(columns={'Home Zip Code': 'Home Zip'}, inplace=True)

# get total spend for medical retirees
df_himis0_2018['Total Spend-MR'] = (df_himis0_2018['E - Enrollee'] +
                                    df_himis0_2018['Dependents'])

# we only want the total, I think
df_himis0_2018 = df_himis0_2018[['Home Zip', 'Total Spend-MR']]

# do medical employees next
df_himis1_2018 = pd.read_excel('Divisions/DSGI-HIMIS/FY2018-2019/DSGI HIMIS FY 18-19.xlsx',
                               sheet_name='2018 Medical Employees',
                               # home zip need to be a string, not a number or leading zeroes will be dropped
                               dtype={'Home Zip': str}
                               )

# get total spend for medical employees
df_himis1_2018['Total Spend-ME'] = (df_himis1_2018['E - Enrollee'] +
                                    df_himis1_2018['Dependents'])

# we only want the total, I think
df_himis1_2018 = df_himis1_2018[['Home Zip', 'Total Spend-ME']]

# check dtypes b4 merge
df_himis0_2018['Home Zip'] = df_himis0_2018['Home Zip'].astype(str)

# merge the 2 medical sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_medical_2018 = pd.merge(left=df_himis0_2018, left_on='Home Zip',
                           right=df_himis1_2018, right_on='Home Zip',
                           how='outer')

# total medical spend: retirees and employees, and their dependents
df_medical_2018['Total Medical Spend'] = (df_medical_2018['Total Spend-ME'] +
                                          df_medical_2018['Total Spend-MR'])

# pharma retirees
df_himis2_2018 = pd.read_excel('Divisions/DSGI-HIMIS/FY2018-2019/DSGI HIMIS FY 18-19.xlsx',
                               sheet_name='2018 Pharmacy Retiree',
                               dtype={'Home Zip': str})

# add ppl + dependents to get total spend
df_himis2_2018['Total Spend-PR'] = (df_himis2_2018['E - Enrollee'] +
                                    df_himis2_2018['Dependents'])

# we only want the total
df_himis2_2018 = df_himis2_2018[['Home Zip', 'Total Spend-PR']]

# pharma employees
df_himis3_2018 = pd.read_excel('Divisions/DSGI-HIMIS/FY2018-2019/DSGI HIMIS FY 18-19.xlsx',
                               sheet_name='2018 Pharmacy Employees',
                               # coerce zip to str type
                               dtype={'Home Zip': str})

# add ppl + dependents to get total spend
df_himis3_2018['Total Spend-PE'] = (df_himis3_2018['E - Enrollee'] +
                                    df_himis3_2018['Dependents'])

# we only want the total, I think
df_himis3_2018 = df_himis3_2018[['Home Zip', 'Total Spend-PE']]

# merge the 2 pharma sheets together, get total med spend
df_pharma_2018 = pd.merge(left=df_himis2_2018, left_on='Home Zip',
                          right=df_himis3_2018, right_on='Home Zip',
                          # outer join is appropriate to preserve all zip codes
                          how='outer')

# total pharma spend: retirees and employees, and their dependents
df_pharma_2018['Total Pharma Spend'] = (df_pharma_2018['Total Spend-PE'] +
                                        df_pharma_2018['Total Spend-PR'])

# merge medical and pharma into one dataset
df_himis_2018 = pd.merge(left=df_medical_2018, left_on='Home Zip',
                         right=df_pharma_2018, right_on='Home Zip',
                         how='outer')

# add up total HIMIS spend
df_himis_2018['Total HIMIS Spend'] = (df_himis_2018['Total Medical Spend'] +
                                      df_himis_2018['Total Pharma Spend'])
# get total employee spend
df_himis_2018['Total Employee Spend'] = (df_himis_2018['Total Spend-ME'] +
                                         df_himis_2018['Total Spend-PE'])

# get columns to make sure we get all we need during agg
# df_himis_2018.columns

# prepare to merge with df_geo_zip to get city names
# by grouping and agg'ing
df_himis_agg_2018 = df_himis_2018.groupby(['Home Zip'
                                           ]).agg({'Total Spend-MR': 'sum',
                                                   'Total Spend-ME': 'sum',
                                                   'Total Medical Spend': 'sum',
                                                   'Total Spend-PR': 'sum',
                                                   'Total Spend-PE': 'sum',
                                                   'Total Pharma Spend': 'sum',
                                                   'Total HIMIS Spend': 'sum'
                                                   }).reset_index()

# rename zip at this point to match df_geo_zip
df_himis_agg_2018.rename(columns={'Home Zip': 'Zip Code'}, inplace=True)

# change GeoFile zip code to str (object) type so merge will work
df_geo_zip['Zip Code'] = df_geo_zip['Zip Code'].astype(str)

# merge with df_geo_zip to attach city names
df_himis_agg_2018 = pd.merge(left=df_himis_agg_2018, left_on='Zip Code',
                             right=df_geo_zip, right_on='Zip Code',
                             how='left')

# remove rows where city is null--they aren't florida zip codes
# we filtered out non-FL zips from df_geo_zips when we did retirement
df_himis_agg_2018.dropna(axis=0, how='any', subset=(['City']), inplace=True)
# reset index just in case
df_himis_agg_2018.reset_index(inplace=True)

# replace city names that won't make the merge
df_himis_agg_2018.replace(to_replace=to_replace, inplace=True)

# ready to group and agg in preparation for merge with df_geo_city
df_himis_agg_2018 = df_himis_agg_2018.groupby(['City']).agg({'Total Medical Spend':
                                                             'sum',
                                                             'Total Pharma Spend':
                                                             'sum',
                                                             'Total HIMIS Spend':
                                                             'sum'}).reset_index()

# rename city to avoid duplication after the merge
df_himis_agg_2018.rename(columns={'city': 'City'}, inplace=True)

# check totals of metrics
# df_himis_agg_2018['Total Medical Spend'].sum()

# check totals of metrics
# df_himis_agg_2018['Total Pharma Spend'].sum()

# check to see which cities won't make the merge
# list(set(df_himis_agg_2018['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# merge with df_geo_city to attach district, county, etc information
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_himis_agg_2018, right_on='City',
                        how='left')

# rename columns to desired names
df_city18_19.rename(columns={'Total MFMP Spend': 'MyFloridaMarketPlace',
                             'Total HIMIS Spend': 'Total Claims',
                             'Total Medical Spend': 'Medical Claims',
                             'Total Pharma Spend': 'Pharmacy Claims',
                             'Total Employee Spend': 'Total Employee Claims'},
                    inplace=True)

# df_city18_19.head()

# In[8]:

# ### DSS
# Metrics: Fleet Maintenance Cost, Fuel Cost, Private Prison Contract Spend, Federal Surplus Savings, SASP Savings

# #### FLEET
# Metrics: Maintenance Cost, Fuel Cost

# read in fleet data
df_fleet18_19 = pd.read_excel('Divisions/DSS-Fleet/FY2018-2019/DSS FLEET FY 18-19.xlsx')

# show columns for slicing
# df_fleet18_19.columns

# df_fleet18_19.head()

# missing_data(df_fleet18_19)

# check for consistency with dashboard
# df_fleet18_19['TOTAL MAINTENANCE COST'].sum()

# replace dashes in TOTAL FUEL COSTS with zero
df_fleet18_19.replace('--', 0, inplace=True)
# convert to float
df_fleet18_19['TOTAL FUEL COSTS'] = df_fleet18_19['TOTAL FUEL COSTS'].astype(float)
# check for consistency with dashboard
# df_fleet18_19['TOTAL FUEL COSTS'].sum()

# check for consistency with dashboard
# df_fleet18_19['ASSET'].count()

# change CITY to City
df_fleet18_19.rename(columns={'CITY': 'City'},
                     inplace=True)
# change city to upper
df_fleet18_19['City'] = df_fleet18_19['City'].str.upper()
# slice columns
df_fleet18_19 = df_fleet18_19.loc[:, ['TOTAL OPERATING COST',
                                      'TOTAL MAINTENANCE COST',
                                      'TOTAL FUEL COSTS', 'City',
                                      'ASSET']]

df_fleet18_19.replace(to_replace=to_replace, inplace=True)

# aggregate to city level here so tableau doesn't have to
# only need city column, others will be duplicated in merge with final
# other geo columns are there, pre-agg, if we need them later
df_fleet18_19_agg = df_fleet18_19.groupby(['City']).agg({'TOTAL OPERATING COST':
                                                         'sum',
                                                         'TOTAL MAINTENANCE COST':
                                                         'sum',
                                                         'TOTAL FUEL COSTS':
                                                         'sum',
                                                         'ASSET': 'count'
                                                         }).reset_index()

# check to see which cities won't make the merge
# list(set(df_fleet18_19['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# need to merge existing df and fleet data, do this on city
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_fleet18_19_agg, right_on='City',
                        how='left')

df_city18_19.rename(columns={'TOTAL FUEL COSTS': 'Fuel Cost',
                             'TOTAL MAINTENANCE COST': 'Fleet Maintenance Cost',
                             'TOTAL OPERATING COST': 'Total Operating Cost',
                             'ASSET': 'Fleet Assets'},
                    inplace=True)

# df_city18_19.head()

# In[9]:

# #### LESO
# Metrics: Federal Surplus Savings (dollars), Number of Items

# this one comes in monthly sheets only, will need to concat
xl = pd.ExcelFile('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx')
# xl.sheet_names

# import current fiscal year's leso data, concat months together
JUL_2018 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='JUL 18')
AUG_2018 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='AUG 2018')
SEP_2018 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='SEPT 2018')
OCT_2018 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='OCT 2018')
NOV_2018 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='NOV 2018')
DEC_2018 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='DEC 2018')
JAN_2019 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='JAN 2019')
FEB_2019 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='FEB 2019')
MAR_2019 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='MAR 2019')
APR_2019 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='APR 2019')
MAY_2019 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='MAY 2017')
JUN_2019 = pd.read_excel('Divisions/DSS-LESO/FY2018-2019/DSS LESO FY 18-19.xlsx',
                         sheet_name='JUN 2018')

# concat sheets together vertically
# months to come
years18_19 = [JUL_2018, AUG_2018, SEP_2018, OCT_2018, NOV_2018, DEC_2018,
              JAN_2019, FEB_2019, MAR_2019, APR_2019, MAY_2019, JUN_2019]

df_leso18_19 = pd.concat(years18_19, ignore_index=True, axis=0)

# need to drop months with rows that are blanks
df_leso18_19.dropna(subset=['Law Enforcement\nAgency Name'],
                    axis=0, how='all', inplace=True)

df_leso18_19 = df_leso18_19.reset_index()

df_leso18_19.drop(df_leso18_19.index[177], inplace=True)
# looks good, now the fun part

# df_leso18_19.head()

# fix dtypes
df_leso18_19['Initial Acquisition\nCost (IAC)'] = df_leso18_19['Initial Acquisition\nCost (IAC)'].astype(float)

df_leso18_19['Service Charge'] = df_leso18_19['Service Charge'].astype(float)

# df_leso18_19.dtypes
# for easy copy pasting

df_leso18_19.rename(columns={'Law Enforcement\nAgency Name': 'LE Agency Name'},
                    inplace=True)

df_leso18_19.replace(to_replace=to_replace, inplace=True)

df_leso18_19['Savings'] = df_leso18_19['Initial Acquisition\nCost (IAC)'] - df_leso18_19['Service Charge']

# change LE to city, since thats what it is, now
df_leso18_19.rename(columns={'LE Agency Name': 'City'}, inplace=True)

# only get columns we need
df_leso18_19 = df_leso18_19[['Total Quantity\nof Items', 'Savings', 'City']]

# check to see which cities won't make the merge
# list(set(df_leso18_19['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# group and agg to the city level
df_leso_agg18_19 = df_leso18_19.groupby(['City']
                                        ).agg({'Savings': 'sum',
                                               'Total Quantity\nof Items':
                                               'sum'}
                                              ).reset_index()

# merge with LESO city-level data
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_leso_agg18_19, right_on='City',
                        how='left')

df_city18_19.rename(columns={'Total Quantity\nof Items':
                             'LESO Items',
                             'Savings': 'LESO Savings',
                             },
                    inplace=True)

# df_city18_19.head()

# In[10]:

# #### SASP
# Metrics: SASP Savings, SASP Items
# import and list columns
df_sasp18_19 = pd.read_excel('Divisions/DSS-SASP/FY2018-2019/DSS SASP FY 18-19.xlsx',
                             sheet_name='all')
# df_sasp18_19.columns

# preview dataset
# df_sasp18_19.head()

# only get columns we need
df_sasp18_19 = df_sasp18_19[['CUSTOMER', 'LINE ITEMS', 'savings']]

df_sasp18_19.rename(columns={'CUSTOMER': 'Donee Name'
                             },
                    inplace=True)

# import donee locations to attach to first dataset
df_donees = pd.read_csv('Divisions/DSS-SASP/FY2018-2019/Donee City/Donee City FY 18-19.csv')

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
# list(set(df_sasp18_19['Donee Name'].to_list()).difference(list(set(df_donees['Donee Name'].to_list()))))

# merge to attach city names to sasp data
df_sasp18_19 = pd.merge(left=df_sasp18_19, left_on='Donee Name',
                        right=df_donees, right_on='Donee Name',
                        how='left')

# df_sasp18_19.head()

# group and agg to the city level
df_sasp18_19_agg = df_sasp18_19.groupby(['City']).agg({
                                                       'LINE ITEMS': 'count',
                                                       'savings':
                                                       'sum',
                                                       }).reset_index()

df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_sasp18_19_agg, right_on='City',
                        how='left')

df_city18_19.rename(columns={'savings': 'SASP Savings',
                             'LINE ITEMS': 'SASP Items'
                             },
                    inplace=True)

# df_city18_19['SASP Savings'].sum()

# df_city18_19.head()

# In[11]:

# #### Private Prison Monitoring
# Metrics: Private Prison Contract Spend

# read private prison data
# it is already agg'd, so no need to
df_ppm18_19 = pd.read_excel('Divisions/DSS-Private Prisons/FY2018-2019/DSS Private Prisons FY 18-19.xlsx',
                            sheet_name=7,
                            header=1,
                            skipfooter=16,
                            dtype={'Reimbursement to Vendor w/Deductions': float}
                            )
# df_ppm18_19.columns

# first 28 rows useless to us
df_ppm18_19 = df_ppm18_19.loc[:28, ['Facility', 'Reimbursement to Vendor w/Deductions']]

df_ppm18_19.loc[2, 'Facility'] = 'BLACKWATER'

# need to join facility names with addresses frm dms website
df_ppm_location = pd.read_excel('Geo Files for Merging/ppm_locations.xlsx')

# join to attach addresses
# inner join to get rid of extra unnecessary rows
df_ppm18_19_city = pd.merge(left=df_ppm18_19, left_on='Facility',
                            right=df_ppm_location, right_on='Facility',
                            how='inner')

# no longer need name of facility - for now
df_ppm18_19_city = df_ppm18_19_city.iloc[:, 1:]

# rename to something a little digestible
df_ppm18_19_city.rename(columns={'Reimbursement to Vendor w/Deductions':
                                 'Total Private Prison Spend'}, inplace=True)
# ready for merge at end

# df_ppm18_19_city['Total Private Prison Spend'].sum()

# check to see which cities won't make the merge
# list(set(df_ppm18_19_city['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# merge with PPM data
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_ppm18_19_city, right_on='City',
                        how='left')

df_city18_19.rename(columns={'Total Private Prison Spend':
                             'Private Prison Contract Spend'},
                    inplace=True)

# df_city18_19.head()




# In[12]:

# ### MyFloridaMarketPlace
# Metrics: Total MFMP Spend, Number of Vendors

filepath = 'Divisions/MFMP/FY2018-2019/DMS Government Impact Dashboard Report FY2018-2019.xlsx'
df_mfmp18_19 = pd.read_excel(filepath, dtype={'sum(Invoice Spend)': str})

# list columns, only need a couple
# df_mfmp18_19.columns

# filter out non-FL impact transactions
df_mfmp18_19 = df_mfmp18_19[df_mfmp18_19['Supplier Location - PO State'] == 'FL'].reset_index()

# number of transactions left is substantial ~180K
# len(df_mfmp18_19)

# df_mfmp18_19.dtypes

# select only the columns we need
df_mfmp18_19 = df_mfmp18_19.loc[:, ['Supplier Location - PO City', 'sum(Invoice Spend)',
                                    'Supplier - Company Name']]

# rename columns
df_mfmp18_19.rename(columns={'Supplier Location - PO City': 'City',
                             'sum(Invoice Spend)': 'MFMP Spend',
                             'Supplier - Company Name': 'Company Name'},
                    inplace=True)

# capitalize city for eventual merge
df_mfmp18_19['City'] = df_mfmp18_19['City'].str.upper()

# go ahead and do all replacements, adding new ones
df_mfmp18_19.replace(to_replace=to_replace, inplace=True)

# drop the ones that were ambiguous or not in FL
df_mfmp18_19 = df_mfmp18_19[df_mfmp18_19['City'].isin(to_drop) == False]

# change MFMP spend to numeric column so agg will work
df_mfmp18_19['MFMP Spend'] = df_mfmp18_19['MFMP Spend'].str.replace(',', '', regex=True)

df_mfmp18_19['MFMP Spend'] = df_mfmp18_19['MFMP Spend'].astype('float')

# we have some extra vendors in our db
# df_mfmp18_19['Company Name'].value_counts()

# group and agg to city level
df_mfmp18_19_agg = df_mfmp18_19.groupby(['City']).agg({'MFMP Spend': 'sum',
                                                       'Company Name':
                                                       'nunique'}).reset_index()
# rename metric to desired name
df_mfmp18_19_agg.rename(columns={
                                 'Company Name': 'Vendors'
                                 },
                        inplace=True)

# check metric totals for consistency with dashboard
# df_mfmp18_19_agg['MFMP Spend'].sum()

# ended up with 500 more vendors somehow
# df_mfmp18_19_agg['Vendors'].sum()

# check to see which cities won't make the merge
# list(set(df_mfmp18_19_agg['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# merge master with mfmp
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_mfmp18_19_agg, right_on='City',
                        how='left')

# df_city18_19.head()

# In[13]:

# ### OSD
# Metrics: Number of Minority-Owned Businesses Registered
df_osd18_19 = pd.read_excel('Divisions/OSD/FY2021-2022/OSD Certified Vendor File.xlsx')

# df_osd18_19.columns

# df_osd18_19.head()
# many dates are not of this fiscal year

# missing_data(df_osd18_19)

# only grab registrations active for FY 19-20
# generate dates for entire FY
rng = pd.date_range('07-01-2018', '06-30-2019', periods=365).to_frame()

# place in datframe
df_FY18_19 = pd.DataFrame(rng[0].astype(str)).reset_index()

# split the date component and the time component
df_FY18_19['Time'] = df_FY18_19[0].str.split(' ')

# grab the date portion
df_FY18_19['Time'] = df_FY18_19['Time'].str[0]

# place in a list
wrong_format = df_FY18_19['Time'].to_list()

# must change to our precious format
FY18_19 = []

# change to match OSD date format
for i in wrong_format:
    temp = i.split('-')
    FY18_19.append(temp[1]+'/'+temp[2]+'/'+temp[0])
# if date is in our range, grab it


def inYear(row):
    if (row['Effective On'] in FY18_19) or (row['Expire On'] in FY18_19):
        return '1'
    else:
        return '0'


# apply a marker to our df that we can use to filter it
df_osd18_19['Indic'] = df_osd18_19.apply(inYear, axis=1)

df_osd18_19 = df_osd18_19[df_osd18_19['Indic'] == '1']

# df_osd18_19.head()

# no records from this FY
# is expected, system only goes back 2 yrs acc to b roberts @ osd

# In[13]:

# ### PeopleFirst
# Metrics: Employees, Vacancies, Positions, Annualized Salary

filepath = 'Divisions/People First - SPS/FY2018-2019/PF State Positions Locations.xlsx'

df_pf18_19 = pd.read_excel(filepath)

# df_pf18_19.columns

# df_pf18_19.head()

# before we do anything, join with location information in other file they sent
filepath = 'Divisions/People First - SPS/FY2018-2019/PF FY 18-19.xlsx'

df_pf18_19_b = pd.read_excel(filepath)

# df_pf18_19_b.columns

# df_pf18_19_b['Pos FTE'].sum()

# df_pf18_19_b.head()

# see which we can join on, ie are in both datasets
[column for column in df_pf18_19.columns if column in df_pf18_19_b]

# want to 'attach' info in _b, such as Base Rate of Pay, etc to left df
df_pf18_19 = pd.merge(left=df_pf18_19_b, left_on='Pos Num (8 Digits)',
                      right=df_pf18_19, right_on='Pos Num (8 Digits)',
                      how='left')

# df_pf18_19.columns

# df_pf18_19['Pos Num (8 Digits)'].sum()

# 494 rows with no location city
# df_pf18_19[df_pf18_19['Location City'] == ' ']

# only keep the columns we need
df_pf18_19 = df_pf18_19[['Location City', 'Vacant', 'Pos Num (8 Digits)',
                         'Base Rate Of Pay', '2019_St Health Cov Code',
                         'Pay Type Code', 'Pos FTE', 'Emp FTE',
                         'Employee Type']]

# split off OPS to be a separate metric
df_ops = df_pf18_19[(df_pf18_19['Employee Type'] == 4) | (df_pf18_19['Employee Type'] == 5) ]

df_ops = df_ops.reset_index()

# len(df_ops)

# make sure cities are upper for the merge
df_ops['Location City'] = df_ops['Location City'].str.upper()

df_ops['Location City'].fillna('UNDEFINED', inplace=True)

df_ops.replace(to_replace={' ': 'UNDEFINED'}, inplace=True)

df_ops_agg = df_ops.groupby(by=['Location City']).agg({'Pos Num (8 Digits)':
                                                       'nunique'
                                                       }).reset_index()

df_ops_agg.rename(columns={'Pos Num (8 Digits)': 'OPS Employees'},
                  inplace=True)

# df_ops_agg['OPS Employees'].sum()

df_ops_agg.rename(columns={'Location City': 'City'}, inplace=True)

# ready for merge with master for the year
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_ops_agg, right_on='City',
                        how='left')

# filter down to OPS employees only 
df_pf18_19 = df_pf18_19[(df_pf18_19['Employee Type'] == 1) | (df_pf18_19['Employee Type'] == 2)]

# df_pf18_19['Pos Num (8 Digits)'].sum()

# missing_data(df_pf18_19)
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


df_pf18_19['Annualized Salary'] = df_pf18_19.apply(convertSalary2, axis=1)

# df_pf18_19['Annualized Salary'].sum()

# df_pf18_19['Pos FTE'].sum()

# go ahead and do all replacements, adding new ones
df_pf18_19.replace(to_replace=to_replace, inplace=True)

# df_pf18_19['Pos FTE'].sum()

# drop the ones that were ambiguous or not in FL
df_pf18_19 = df_pf18_19[df_pf18_19['Location City'].isin(to_drop) == False]

# df_pf18_19['Pos FTE'].sum()

# check to see which cities won't make the merge
# list(set(df_pf18_19['Location City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# replace vacancy indicator with 1 for agg'ing
df_pf18_19.replace("Y", 1, inplace=True)

# df_pf18_19['Pos Num (8 Digits)'].sum()

df_pf18_19['Location City'].fillna('UNDEFINED', inplace=True)

df_pf18_19.replace(to_replace={' ': 'UNDEFINED'}, inplace=True)

df_pf18_19_agg = df_pf18_19.groupby(['Location City']).agg({'Vacant': 'sum',
                                                            'Annualized Salary':
                                                            'sum',
                                                            'Pos Num (8 Digits)':
                                                            'nunique',
                                                            'Emp FTE':
                                                            'sum'}).reset_index()

# df_pf18_19_agg['Pos Num (8 Digits)'].sum()

# df_pf18_19_agg.head()

# change name to city so columns won't duplicate on merge
df_pf18_19_agg.rename(columns={'Location City': 'City',
                               'Pos Num (8 Digits)': 'Positions',
                               'Emp FTE': 'Employees',
                               'Vacant': 'Vacancies'}, inplace=True)

# df_pf18_19_agg['Positions'].sum()

# make sure cities are upper for the merge
df_pf18_19_agg['City'] = df_pf18_19_agg['City'].str.upper()

# df_pf18_19_agg['Positions'].sum()

# df_pf18_19_agg.head()

# ready for merge with master for the year
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_pf18_19_agg, right_on='City',
                        how='left')

# df_city18_19['Positions'].sum()

# df_city18_19['Employees'].sum()

# df_city18_19['Vacancies'].sum()

# df_city18_19['Annualized Salary'].sum()

# df_city18_19['OPS Employees'].sum()

# df_city18_19.head()

# df_city18_19.Positions.sum()

# In[14]:

# ### REDM
# Metrics: Owned Sq. Footage, Facilities Owned, Maintenance (Operating) Cost, Utility Bills, Structure Value, Land Value

df_owned18_19 = pd.read_excel('Divisions/REDM-SOLARIS/FY2018-2019//REDM SOLARIS FY 18-19.xlsx')

# df_owned18_19.columns

# df_owned18_19.head()

# get dms blds only per secretary
df_owned18_19 = df_owned18_19[df_owned18_19['Agency Name'] ==
                              'Department of Management Services']

# get necessary columns only
df_owned18_19 = df_owned18_19[['FL-SOLARIS Facility #',
                               'Facility City',
                               'Gross Sq Ft',
                               'Taxroll Land Value',
                               'Taxroll Structure Value',
                               'Total Utility Cost',
                               'Operating Cost'
                               ]]

# Rename columns to match with the required metric values
df_owned18_19.rename(columns={'Gross Sq Ft': 'Owned Square Footage',
                              'Taxroll Land Value': 'Land Value',
                              'Taxroll Structure Value': 'Structure Value',
                              'FL-SOLARIS Facility #': 'ID',
                              'Facility City': 'City',
                              'Total Utility Cost': 'Utility Bills Paid',
                              'Operating Cost': 'Building Maintenance'
                              },
                     inplace=True)

# capitalize facility city
df_owned18_19['City'] = df_owned18_19['City'].str.upper()

# replace county names that didn't make it into the merge
df_owned18_19.replace(to_replace=to_replace, inplace=True)

# ambiguous or other state's cities must be dropped
df_owned18_19 = df_owned18_19[df_owned18_19['City'].isin(to_drop) == False]

# Aggregating and renaming metrics from the Owned file
df_owned18_19_agg = df_owned18_19.groupby(['City'
                                           ]).agg({'ID': 'count',
                                                   'Owned Square Footage':
                                                   'sum',
                                                   'Land Value': 'sum',
                                                   'Structure Value':
                                                   'sum',
                                                   'Utility Bills Paid': 'sum',
                                                   'Building Maintenance': 'sum'
                                                   }).reset_index()
df_owned18_19_agg.rename(columns={'ID': 'Facilities Owned'}, inplace=True)
# ready for merge

# check owned to see if any cities won't make the merge
# empty set is good
# list(set(df_owned18_19_agg['City'].str.upper().to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# Merge with df_geo_city to attach location information at city level
df_city18_19 = pd.merge(left=df_city18_19, right=df_owned18_19_agg,
                        left_on='City', right_on='City',
                        how='left')

# df_city18_19.head()




# In[15]:

# ### RET
# Metrics: Benefits Paid, Number of Payees, Number of Employers, Employer Contributions

# #### Payments and Payees
# Metrics: Benefits Paid, Number of Payees

# import historical retirement data
df_ret18 = pd.read_excel('Divisions/RET-IRIS/old FYs/20220121 dashboard data for FY18.xlsx',
                         # trim off rows from other states
                         header=58,
                         # trim grand total from the end
                         skipfooter=2,
                         dtype={'Unnamed: 1': str}
                         )

# df_ret18.columns

# rename to get column titles we need
df_ret18.rename(columns={'Unnamed: 1': 'Zip Code',
                         'Unnamed: 2': 'City',
                         2710712.26: 'Payment Amount',
                         122: 'Number of Payees',
                         2018: 'FY'
                          }, inplace=True)

# check our work
# df_ret18.head()

# check for missing data
# missing_data(df_ret18)
# excellent

# get rid of unnecessary columns
df_ret18 = df_ret18.iloc[:, 1:5]

# make replacements
df_ret18.replace(to_replace=to_replace, inplace=True)

# track down cities that won't make the merge
# list(set(df_ret18['Zip Code'].to_list()).difference(list(set(df_geo_zip['Zip Code'].to_list()))))

# drop zips that aren't in state, all were looked up
drop_zips = ['75791',
             '38221',
             '31646',
             '07070',
             '07024',
             '28904',
             '30076',
             '75039',
             '46590',
             '33343',
             '32271',
             '34722',
             '33939',
             '36542']

for Zip in drop_zips:
    df_ret18 = df_ret18[df_ret18['Zip Code'] != Zip]

# track down cities that won't make the merge
# list(set(df_ret18['Zip Code'].to_list()).difference(list(set(df_geo_zip['Zip Code'].to_list()))))

# drop city, its full of garbage
df_ret18.drop(['City'], axis=1, inplace=True)

# attach city names by merging with df_geo_zip
df_ret18 = pd.merge(left=df_ret18, left_on='Zip Code',
                    right=df_geo_zip, right_on='Zip Code',
                    how='left')

# check our work
# df_ret18.head()

# group and agg to the city level
df_ret18_agg = df_ret18.groupby(['City']).agg({'Payment Amount':
                                               'sum',
                                               'Number of Payees':
                                               'sum'
                                               }).reset_index()

# create overall df for fy 19-20 data
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_ret18_agg, right_on='City',
                        how='left')

df_city18_19.rename(columns={'Payment Amount':
                             'Benefits Paid to Retirees'
                             },
                    inplace=True)

# df_city18_19.head()


# #### Employers and Contributions
# Metrics: Number of Employers, Employer Contributions

# import third sheet of ret data
df_ret18_2 = pd.read_excel('Divisions/RET-IRIS/old FYs/20220121 dashboard data for FY18.xlsx',
                           sheet_name='FL Agency Employer Contribution',
                           # remove grand total from bottom
                           skipfooter=2
                           )

# df_ret18_2.head()

# missing_data(df_ret18_2)

# cut down to only columns we need
df_ret18_2 = df_ret18_2[['Agency Name', 'City Name',
                         'Employer Contribution Amount',
                         ]]

# rename columns
df_ret18_2.rename(columns={'City Name': 'City'
                           },
                  inplace=True
                  )

# check work
# df_ret18_2.head()

# strip spaces off the right side
df_ret18_2['City'] = df_ret18_2['City'].str.rstrip(' ')

# make replacements to fix known cityname issues
df_ret18_2.replace(to_replace=to_replace, inplace=True)

# track down cities that won't make the merge
# list(set(df_ret18_2['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# we have these already in to_drop, so drop
df_ret18_2 = df_ret18_2[df_ret18_2['City'].isin(to_drop) == False]

# track down cities that won't make the merge
# list(set(df_ret18_2['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# group and agg to city level
df_ret18_2_agg = df_ret18_2.groupby(['City']).agg({'Agency Name':
                                                   'nunique',
                                                   'Employer Contribution Amount':
                                                   'sum',
                                                   }).reset_index()
# FY will get dropped out bc we didnt specify it

# merge on city with main fy 19-20 dataset
df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_ret18_2_agg, right_on='City',
                        how='left')

# rename to accurately represent aggregations
df_city18_19.rename(columns={'Agency Name': 'Number of Employers Using Retirement System',
                             'Employer Contribution Amount': 'Employer Contributions'
                             },
                    inplace=True)

# df_city18_19.head()

# In[17]:

# ### STMS
# 
# Metrics: STMS Travel Spend

# separate fy 18-10 from the rest
df_stms18_19 = pd.read_excel('Divisions/STMS/FY2018-2019/STMS FY 18-19.xlsx')

# len(list(set(df_stms18_19['Form ID: * Destination'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

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


df_stms18_19['City'] = df_stms18_19.apply(cityFinder, axis=1)

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


df_stms18_19['City2'] = df_stms18_19.apply(countyFinder, axis=1)

df_stms18_19['City'].fillna(df_stms18_19['City2'].str.upper(), inplace=True)

df_stms18_19['City'].fillna(df_stms18_19['Form ID: * Destination'].str.upper(), inplace=True)

# check for replacement candidates
# len(list(set(df_stms18_19['City'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# make replacements we have collected, maybe will make a difference
df_stms18_19.replace(to_replace=to_replace, inplace=True)

# drop the ones that were ambiguous or not in FL
df_stms18_19 = df_stms18_19[df_stms18_19['City'].isin(to_drop) == False]

# again
# len(list(set(df_stms18_19['City'].to_list()).difference(list(set(df_geo_city['City'].to_list())))))

# again
# list(set(df_stms18_19['City'].to_list()).difference(list(set(df_geo_city['City'].to_list()))))

# df_stms18_19.columns

# df_stms18_19.head()

df_stms18_19_agg = df_stms18_19.groupby(['City']).agg({'Total Amount': 'sum'
                                                       }).reset_index()

df_stms18_19_agg = df_stms18_19_agg[['City', 'Total Amount']]

df_stms18_19_agg.rename(columns={'Total Amount': 'Travel Spend',
                                 },
                        inplace=True)

df_city18_19 = pd.merge(left=df_city18_19, left_on='City',
                        right=df_stms18_19_agg, right_on='City',
                        how='left')

# df_city18_19.head()




# In[18]:

# ### Year Processing

df_city18_19['FY'] = '18-19'

df_county18_19['FY'] = '18-19'


# ## Export

# ### County

# df_county18_19.dtypes

df_county18_19.to_excel('Fiscal Years/County Level FY 2018-2019.xlsx')

# df_city18_19.dtypes


# ### City

# df_city18_19.dtypes

# df_city18_19.head()

df_city18_19.to_excel('Fiscal Years/City Level FY 2018-2019.xlsx')

# In[19]:

# ## Time
executionTime = (time.time() - startTime)
# convert to mins
mins = str(round(executionTime/60))
secs = str(round(executionTime % 60, 2))

print('Execution time: {} minutes, {} seconds: '.format(mins, secs))
