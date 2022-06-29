# -*- coding: utf-8 -*-
"""
Created on Mon May 23 16:32:10 2022

@author: mairsb
"""

# In[0]:
import pandas as pd
import numpy as np

fiscal_year = 'FY2021-2022'
month = 'May'
year = '2022'

# temp until I can fix the directory structure at start of next fy

temp_folder = month + ' ' + year

# we will merge to this df
df_out = pd.DataFrame(data=['18-19', '19-20', '20-21', '21-22'],
                      columns=['FY'])

# goal is to get totals of spending OUTSIDE of florida to match todd's numbers
# lol
# also don't forget to add this to github

# In[1]:

# ### DSGI
# Metrics: Total Pharma Spend, Total Medical Spend

# FY 21-22
# 4 sheets with every combination of (medical,pharmacy) and (retirees, emps)
# skip footer =1 because DSGI, so kindly, included grand totals
# medical retirees
# home zip need to be a string, not a number or leading zeroes will be dropped
# causing issue with the merge, so we do a dtype=
filepath = 'Divisions/DSGI-HIMIS/FY2021-2022/June 2022/July 2021 to Apr 2022 Claims Final.xlsx'

df_himis0 = pd.read_excel(filepath,
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
df_himis1 = pd.read_excel(filepath,
                          sheet_name='Medical Active',
                          skipfooter=1, dtype={'Home Zip': str})


# get total spend for medical employees
df_himis1['Total Spend-ME'] = (df_himis1['E - Enrollee'] +
                               df_himis1['Dependents'])

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
df_himis2 = pd.read_excel(filepath,
                          sheet_name='Pharmacy Retiree',
                          skipfooter=1, dtype={'Home Zip': str})

# add
df_himis2['Total Spend-PR'] = (df_himis2['E - Enrollee'] +
                               df_himis2['Dependents'])

# we only want the total, I think
df_himis2 = df_himis2[['Home Zip', 'Total Spend-PR']]

# pharma employees
df_himis3 = pd.read_excel(filepath,
                          sheet_name='Pharmacy Active',
                          skipfooter=1, dtype={'Home Zip': str})

df_himis3['Total Spend-PE'] = (df_himis3['E - Enrollee'] +
                               df_himis3['Dependents'])

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

# strip whitespace from city field to avoid unnecessary issues when merging
df_himis['Home Zip'] = df_himis['Home Zip'].str.strip()

# rename zip at this point to match df_geo_zip
df_himis.rename(columns={'Home Zip': 'Zip Code',
                         'Total Medical Spend': 'Non-FL Medical Claims',
                         'Total Pharma Spend': 'Non-FL Pharma Claims',
                         'Total HIMIS Spend': 'Non-FL Claims'},
                    inplace=True)

# we want NON FL zips
# read & coerce zip code to str
filepath = 'Geo Files for Merging/flzips.xlsx'
df_geo_zip = pd.read_excel(filepath, dtype={'Zip Code': str})


# use an anti-join to filter out FL zips
def anti_join(tableA, tableB, on):
    # if joining on index, make it into a column
    if tableB.index.name is not None:
        dummy = tableB.reset_index()[on]
    else:
        dummy = tableB[on]

    # create a dummy columns of 1s
    if isinstance(dummy, pd.Series):
        dummy = dummy.to_frame()

    dummy.loc[:, 'dummy_col'] = 1

    # preserve the index of tableA if it has one
    if tableA.index.name is not None:
        idx_name = tableA.index.name
        tableA = tableA.reset_index(drop=False)
    else:
        idx_name = None

    # do a left-join
    merged = tableA.merge(dummy, on=on, how='left')

    # keep only the non-matches
    output = merged.loc[merged.dummy_col.isna(), tableA.columns.tolist()]

    # reset the index (if applicable)
    if idx_name is not None:
        output = output.set_index(idx_name)

    return(output)


df_himis_nfl = anti_join(df_himis, df_geo_zip, 'Zip Code')

df_himis_nfl = df_himis_nfl[['Non-FL Medical Claims',
                             'Non-FL Pharma Claims',
                             'Non-FL Claims']]

# prepare to merge with df_geo_zip to get city names
# by grouping and agg'ing
df_himis_agg_nfl = df_himis_nfl.agg({
                                     'Non-FL Medical Claims': 'sum',
                                     'Non-FL Pharma Claims': 'sum',
                                     'Non-FL Claims': 'sum'
                                    }).reset_index().transpose()

# mk first row column names
df_himis_agg_nfl.columns = df_himis_agg_nfl.iloc[0]
df_himis_agg_nfl = df_himis_agg_nfl[1:]

# check totals of metrics
# df_himis_agg['Total Medical Spend'].sum()

# check totals of metrics
# df_himis_agg['Total Pharma Spend'].sum()

df_himis_agg_nfl['FY'] = '21-22'

df_himis21_22 = df_himis_agg_nfl

# FY 20-21

df_himis0_2020 = pd.read_excel('Divisions/DSGI-HIMIS/FY2020-2021/DSGI HIMIS FY 20-21.xlsx',
                               sheet_name='2020 Medical Retiree',
                               skipfooter=1, dtype={'Home Zip': str})

# df_himis0.columns

# get total spend for medical retirees
df_himis0_2020['Total Spend-MR'] = (df_himis0_2020['E - Enrollee'] +
                                    df_himis0_2020['Dependents'])

# we only want the total, I think
df_himis0_2020 = df_himis0_2020[['Home Zip', 'Total Spend-MR']]

# medical employees
# home zip need to be a string, not a number or leading zeroes will be dropped
# causing issue with the merge, so we do dtype= again
df_himis1_2020 = pd.read_excel('Divisions/DSGI-HIMIS/FY2020-2021/DSGI HIMIS FY 20-21.xlsx',
                               sheet_name='2020 Medical Employee',
                               dtype={'Home Zip': str})


# get total spend for medical employees
df_himis1_2020['Total Spend-ME'] = (df_himis1_2020['E - Enrollee'] +
                                    df_himis1_2020['Dependents'])

# we only want the total, I think
df_himis1_2020 = df_himis1_2020[['Home Zip', 'Total Spend-ME']]

# merge the 2 medical sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_medical = pd.merge(left=df_himis0_2020, left_on='Home Zip',
                      right=df_himis1_2020, right_on='Home Zip',
                      how='outer')

# total medical spend: retirees and employees, and their dependents
df_medical['Total Medical Spend'] = (df_medical['Total Spend-ME'] +
                                     df_medical['Total Spend-MR'])

# pharma retirees
df_himis2_2020 = pd.read_excel('Divisions/DSGI-HIMIS/FY2020-2021/DSGI HIMIS FY 20-21.xlsx',
                               sheet_name='2020 Pharmacy Retiree',
                               dtype={'Home Zip': str})

# add
df_himis2_2020['Total Spend-PR'] = (df_himis2_2020['E - Enrollee'] +
                               df_himis2_2020['Dependents'])

# we only want the total, I think
df_himis2_2020 = df_himis2_2020[['Home Zip', 'Total Spend-PR']]

# pharma employees
# pharma employees
df_himis3_2020 = pd.read_excel('Divisions/DSGI-HIMIS/FY2020-2021/DSGI HIMIS FY 20-21.xlsx',
                               sheet_name='2020 Pharmacy Employee',
                               dtype={'Home Zip': str})

df_himis3_2020['Total Spend-PE'] = (df_himis3_2020['E - Enrollee'] +
                                    df_himis3_2020['Dependents'])

# we only want the total, I think
df_himis3_2020 = df_himis3_2020[['Home Zip', 'Total Spend-PE']]

# merge the 2 pharma sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_pharma20_21 = pd.merge(left=df_himis2_2020, left_on='Home Zip',
                     right=df_himis3_2020, right_on='Home Zip',
                     how='outer')

# total pharma spend: retirees and employees, and their dependents
df_pharma20_21['Total Pharma Spend'] = (df_pharma20_21['Total Spend-PE'] +
                                   df_pharma20_21['Total Spend-PR'])

# merge medical and pharma into one dataset
df_himis20_21 = pd.merge(left=df_medical, left_on='Home Zip',
                    right=df_pharma20_21, right_on='Home Zip',
                    how='outer')

# add up total HIMIS spend
df_himis20_21['Total HIMIS Spend'] = (df_himis20_21['Total Medical Spend'] +
                                 df_himis20_21['Total Pharma Spend'])
# get total employee spend
df_himis20_21['Total Employee Spend'] = (df_himis20_21['Total Spend-ME'] +
                                    df_himis20_21['Total Spend-PE'])

# get columns to make sure we get all we need during agg
# df_himis20_21.columns

# strip whitespace from city field to avoid unnecessary issues when merging
df_himis20_21['Home Zip'] = df_himis20_21['Home Zip'].str.strip()

# rename zip at this point to match df_geo_zip
df_himis20_21.rename(columns={'Home Zip': 'Zip Code',
                              'Total Medical Spend': 'Non-FL Medical Claims',
                              'Total Pharma Spend': 'Non-FL Pharma Claims',
                              'Total HIMIS Spend': 'Non-FL Claims'},
                     inplace=True)

# we want NON FL zips
# read & coerce zip code to str
filepath = 'Geo Files for Merging/flzips.xlsx'
df_geo_zip = pd.read_excel(filepath, dtype={'Zip Code': str})


# use an anti-join to filter out FL zips
def anti_join(tableA, tableB, on):
    # if joining on index, make it into a column
    if tableB.index.name is not None:
        dummy = tableB.reset_index()[on]
    else:
        dummy = tableB[on]

    # create a dummy columns of 1s
    if isinstance(dummy, pd.Series):
        dummy = dummy.to_frame()

    dummy.loc[:, 'dummy_col'] = 1

    # preserve the index of tableA if it has one
    if tableA.index.name is not None:
        idx_name = tableA.index.name
        tableA = tableA.reset_index(drop=False)
    else:
        idx_name = None

    # do a left-join
    merged = tableA.merge(dummy, on=on, how='left')

    # keep only the non-matches
    output = merged.loc[merged.dummy_col.isna(), tableA.columns.tolist()]

    # reset the index (if applicable)
    if idx_name is not None:
        output = output.set_index(idx_name)

    return(output)


df_himis20_21_nfl = anti_join(df_himis20_21, df_geo_zip, 'Zip Code')

df_himis20_21_nfl = df_himis20_21_nfl[['Non-FL Medical Claims',
                                       'Non-FL Pharma Claims',
                                       'Non-FL Claims']]

# prepare to merge with df_geo_zip to get city names
# by grouping and agg'ing
df_himis20_21_agg_nfl = df_himis20_21_nfl.agg({
                                               'Non-FL Medical Claims': 'sum',
                                               'Non-FL Pharma Claims': 'sum',
                                               'Non-FL Claims': 'sum'
                                               }).reset_index().transpose()

# mk first row column names
df_himis20_21_agg_nfl.columns = df_himis20_21_agg_nfl.iloc[0]
df_himis20_21_agg_nfl = df_himis20_21_agg_nfl[1:]

# check totals of metrics
# df_himis_agg['Total Medical Spend'].sum()

# check totals of metrics
# df_himis_agg['Total Pharma Spend'].sum()

df_himis20_21_agg_nfl['FY'] = '20-21'

df_himis20_21 = df_himis20_21_agg_nfl

# FY 19-20

df_himis0_2019 = pd.read_excel('Divisions/DSGI-HIMIS/FY2019-2020/DSGI HIMIS FY 19-20.xlsx',
                               sheet_name='2019 Medical Retiree',
                               skipfooter=1, dtype={'Home Zip': str})

# df_himis0.columns

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
df_medical = pd.merge(left=df_himis0_2019, left_on='Home Zip',
                      right=df_himis1_2019, right_on='Home Zip',
                      how='outer')

# total medical spend: retirees and employees, and their dependents
df_medical['Total Medical Spend'] = (df_medical['Total Spend-ME'] +
                                     df_medical['Total Spend-MR'])

# pharma retirees
df_himis2_2019 = pd.read_excel('Divisions/DSGI-HIMIS/FY2019-2020//DSGI HIMIS FY 19-20.xlsx',
                               sheet_name='2019 Pharmacy Retiree',
                               dtype={'Home Zip': str})

# add
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
df_pharma19_20 = pd.merge(left=df_himis2_2019, left_on='Home Zip',
                          right=df_himis3_2019, right_on='Home Zip',
                          how='outer')

# total pharma spend: retirees and employees, and their dependents
df_pharma19_20['Total Pharma Spend'] = (df_pharma19_20['Total Spend-PE'] +
                                        df_pharma19_20['Total Spend-PR'])

# merge medical and pharma into one dataset
df_himis19_20 = pd.merge(left=df_medical, left_on='Home Zip',
                         right=df_pharma19_20, right_on='Home Zip',
                         how='outer')

# add up total HIMIS spend
df_himis19_20['Total HIMIS Spend'] = (df_himis19_20['Total Medical Spend'] +
                                      df_himis19_20['Total Pharma Spend'])
# get total employee spend
df_himis19_20['Total Employee Spend'] = (df_himis19_20['Total Spend-ME'] +
                                         df_himis19_20['Total Spend-PE'])

# get columns to make sure we get all we need during agg
# df_himis19_20.columns

# strip whitespace from city field to avoid unnecessary issues when merging
df_himis19_20['Home Zip'] = df_himis19_20['Home Zip'].str.strip()

# rename zip at this point to match df_geo_zip
df_himis19_20.rename(columns={'Home Zip': 'Zip Code',
                              'Total Medical Spend': 'Non-FL Medical Claims',
                              'Total Pharma Spend': 'Non-FL Pharma Claims',
                              'Total HIMIS Spend': 'Non-FL Claims'},
                     inplace=True)

# we want NON FL zips
# read & coerce zip code to str
filepath = 'Geo Files for Merging/flzips.xlsx'
df_geo_zip = pd.read_excel(filepath, dtype={'Zip Code': str})


# use an anti-join to filter out FL zips
def anti_join(tableA, tableB, on):
    # if joining on index, make it into a column
    if tableB.index.name is not None:
        dummy = tableB.reset_index()[on]
    else:
        dummy = tableB[on]

    # create a dummy columns of 1s
    if isinstance(dummy, pd.Series):
        dummy = dummy.to_frame()

    dummy.loc[:, 'dummy_col'] = 1

    # preserve the index of tableA if it has one
    if tableA.index.name is not None:
        idx_name = tableA.index.name
        tableA = tableA.reset_index(drop=False)
    else:
        idx_name = None

    # do a left-join
    merged = tableA.merge(dummy, on=on, how='left')

    # keep only the non-matches
    output = merged.loc[merged.dummy_col.isna(), tableA.columns.tolist()]

    # reset the index (if applicable)
    if idx_name is not None:
        output = output.set_index(idx_name)

    return(output)


df_himis19_20_nfl = anti_join(df_himis19_20, df_geo_zip, 'Zip Code')

df_himis19_20_nfl = df_himis19_20_nfl[['Non-FL Medical Claims',
                                       'Non-FL Pharma Claims',
                                       'Non-FL Claims']]

# prepare to merge with df_geo_zip to get city names
# by grouping and agg'ing
df_himis19_20_agg_nfl = df_himis19_20_nfl.agg({
                                               'Non-FL Medical Claims': 'sum',
                                               'Non-FL Pharma Claims': 'sum',
                                               'Non-FL Claims': 'sum'
                                               }).reset_index().transpose()

# mk first row column names
df_himis19_20_agg_nfl.columns = df_himis19_20_agg_nfl.iloc[0]
df_himis19_20_agg_nfl = df_himis19_20_agg_nfl[1:]

# check totals of metrics
# df_himis_agg['Total Medical Spend'].sum()

# check totals of metrics
# df_himis_agg['Total Pharma Spend'].sum()

df_himis19_20_agg_nfl['FY'] = '19-20'

df_himis19_20 = df_himis19_20_agg_nfl


# FY 18-19

# 4 sheets with every combination of (medical,pharmacy) and (retirees,employees)
# start withmedical retirees
df_himis0_2018 = pd.read_excel('Divisions/DSGI-HIMIS/FY2018-2019/DSGI HIMIS FY 18-19.xlsx',
                               # because grand totals at bottom
                               skipfooter=1,
                               # home zip need to be a string, not a number or leading zeroes will be dropped
                               dtype={'Home Zip': str})

# df_himis0.columns

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

# merge the 2 medical sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_medical = pd.merge(left=df_himis0_2018, left_on='Home Zip',
                      right=df_himis1_2018, right_on='Home Zip',
                      how='outer')

# total medical spend: retirees and employees, and their dependents
df_medical['Total Medical Spend'] = (df_medical['Total Spend-ME'] +
                                     df_medical['Total Spend-MR'])

# pharma retirees
df_himis2_2018 = pd.read_excel('Divisions/DSGI-HIMIS/FY2018-2019/DSGI HIMIS FY 18-19.xlsx',
                               sheet_name='2018 Pharmacy Retiree',
                               dtype={'Home Zip': str})

# add
df_himis2_2018['Total Spend-PR'] = (df_himis2_2018['E - Enrollee'] +
                                    df_himis2_2018['Dependents'])

# we only want the total, I think
df_himis2_2018 = df_himis2_2018[['Home Zip', 'Total Spend-PR']]

# pharma employees
df_himis3_2018 = pd.read_excel('Divisions/DSGI-HIMIS/FY2018-2019/DSGI HIMIS FY 18-19.xlsx',
                               sheet_name='2018 Pharmacy Employees',
                               # coerce zip to str type
                               dtype={'Home Zip': str})

df_himis3_2018['Total Spend-PE'] = (df_himis3_2018['E - Enrollee'] +
                                    df_himis3_2018['Dependents'])

# we only want the total, I think
df_himis3_2018 = df_himis3_2018[['Home Zip', 'Total Spend-PE']]

# merge the 2 pharma sheets together, get total med spend
# outer join is appropriate to preserve all zip codes
df_pharma18_19 = pd.merge(left=df_himis2_2018, left_on='Home Zip',
                          right=df_himis3_2018, right_on='Home Zip',
                          how='outer')

# total pharma spend: retirees and employees, and their dependents
df_pharma18_19['Total Pharma Spend'] = (df_pharma18_19['Total Spend-PE'] +
                                        df_pharma18_19['Total Spend-PR'])

# merge medical and pharma into one dataset
df_himis18_19 = pd.merge(left=df_medical, left_on='Home Zip',
                         right=df_pharma18_19, right_on='Home Zip',
                         how='outer')

# add up total HIMIS spend
df_himis18_19['Total HIMIS Spend'] = (df_himis18_19['Total Medical Spend'] +
                                      df_himis18_19['Total Pharma Spend'])
# get total employee spend
df_himis18_19['Total Employee Spend'] = (df_himis18_19['Total Spend-ME'] +
                                         df_himis18_19['Total Spend-PE'])

# get columns to make sure we get all we need during agg
# df_himis18_19.columns

# strip whitespace from city field to avoid unnecessary issues when merging
df_himis18_19['Home Zip'] = df_himis18_19['Home Zip'].str.strip()

# rename zip at this point to match df_geo_zip
df_himis18_19.rename(columns={'Home Zip': 'Zip Code',
                              'Total Medical Spend': 'Non-FL Medical Claims',
                              'Total Pharma Spend': 'Non-FL Pharma Claims',
                              'Total HIMIS Spend': 'Non-FL Claims'},
                     inplace=True)

# we want NON FL zips
# read & coerce zip code to str
filepath = 'Geo Files for Merging/flzips.xlsx'
df_geo_zip = pd.read_excel(filepath, dtype={'Zip Code': str})


# use an anti-join to filter out FL zips
def anti_join(tableA, tableB, on):
    # if joining on index, make it into a column
    if tableB.index.name is not None:
        dummy = tableB.reset_index()[on]
    else:
        dummy = tableB[on]

    # create a dummy columns of 1s
    if isinstance(dummy, pd.Series):
        dummy = dummy.to_frame()

    dummy.loc[:, 'dummy_col'] = 1

    # preserve the index of tableA if it has one
    if tableA.index.name is not None:
        idx_name = tableA.index.name
        tableA = tableA.reset_index(drop=False)
    else:
        idx_name = None

    # do a left-join
    merged = tableA.merge(dummy, on=on, how='left')

    # keep only the non-matches
    output = merged.loc[merged.dummy_col.isna(), tableA.columns.tolist()]

    # reset the index (if applicable)
    if idx_name is not None:
        output = output.set_index(idx_name)

    return(output)


df_himis18_19_nfl = anti_join(df_himis18_19, df_geo_zip, 'Zip Code')

df_himis18_19_nfl = df_himis18_19_nfl[['Non-FL Medical Claims',
                                       'Non-FL Pharma Claims',
                                       'Non-FL Claims']]

# prepare to merge with df_geo_zip to get city names
# by grouping and agg'ing
df_himis18_19_agg_nfl = df_himis18_19_nfl.agg({
                                               'Non-FL Medical Claims': 'sum',
                                               'Non-FL Pharma Claims': 'sum',
                                               'Non-FL Claims': 'sum'
                                               }).reset_index().transpose()

# mk first row column names
df_himis18_19_agg_nfl.columns = df_himis18_19_agg_nfl.iloc[0]
df_himis18_19_agg_nfl = df_himis18_19_agg_nfl[1:]

# check totals of metrics
# df_himis_agg['Total Medical Spend'].sum()

# check totals of metrics
# df_himis_agg['Total Pharma Spend'].sum()

df_himis18_19_agg_nfl['FY'] = '18-19'

df_himis18_19 = df_himis18_19_agg_nfl

# concat all yrs
df_himis_final = pd.concat([df_himis18_19, df_himis19_20,
                            df_himis20_21, df_himis21_22], ignore_index=True)

df_out = pd.merge(left=df_out, left_on='FY',
                  right=df_himis_final, right_on='FY',
                  how='left')

# In[3]:

# ### MFMP
# Metrics: Total MFMP Spend, Number of Vendors

# FY 21-22
# import my florida marketplace data
df_mfmp = pd.read_csv('Divisions/MFMP/FY2021-2022/June 2022/DMS Government Impact Dashboard Report FY2021-2022.csv')

# list columns, only need a couple
# df_mfmp.columns

# filter out non-FL impact transactions
df_mfmp = df_mfmp[df_mfmp['Supplier Location - PO State'] != 'FL'].reset_index()
# number of transactions left is substantial ~180K
# len(df_mfmp)

# select only the columns we need
df_mfmp = df_mfmp.loc[:, ['Invoice ID', 'Supplier - Company Name',
                          'sum(Invoice Spend)']]

# rename columns
df_mfmp.rename(columns={'sum(Invoice Spend)': 'Non-FL MFMP Spend',
                        'Supplier - Company Name': 'Non-FL Vendors',
                        'Invoice ID': 'Invoices'},
               inplace=True)

# change MFMP spend to numeric column so agg will work
df_mfmp['Non-FL MFMP Spend'] = df_mfmp['Non-FL MFMP Spend'].str.replace(',', '', regex=True)
df_mfmp['Non-FL MFMP Spend'] = df_mfmp['Non-FL MFMP Spend'].astype('float')

# we have some extra vendors in our db
# df_mfmp['Vendors'].value_counts()

# agg & take the transpose
df_mfmp_agg = df_mfmp.agg({'Non-FL MFMP Spend':
                           'sum',
                           'Non-FL Vendors':
                           'nunique',
                           'Invoices':
                           'nunique'
                           }).reset_index().transpose()

# mk first row column names
df_mfmp_agg.columns = df_mfmp_agg.iloc[0]
df_mfmp_agg = df_mfmp_agg[1:]
# check metric totals for consistency with dashboard
# df_mfmp_agg['MFMP Spend'].sum()

# ended up with 500 more vendors somehow
# df_mfmp_agg['Vendors'].sum()

# add fy
df_mfmp_agg['FY'] = '21-22'

# FY 20-21

filepath = 'Divisions/MFMP/FY2020-2021/DMS Government Impact Dashboard Report FY2020-2021.xlsx'
df_mfmp20_21 = pd.read_excel(filepath, dtype={'sum(Invoice Spend)': str})

# list columns, only need a couple
# df_mfmp.columns

# filter out non-FL impact transactions
df_mfmp20_21 = df_mfmp20_21[df_mfmp20_21['Supplier Location - PO State'] != 'FL'].reset_index()
# number of transactions left is substantial ~180K
# len(df_mfmp)

# select only the columns we need
df_mfmp20_21 = df_mfmp20_21.loc[:, ['Invoice ID', 'Supplier - Company Name',
                                    'sum(Invoice Spend)']]

# rename columns
df_mfmp20_21.rename(columns={'sum(Invoice Spend)': 'Non-FL MFMP Spend',
                             'Supplier - Company Name': 'Non-FL Vendors',
                             'Invoice ID': 'Invoices'},
                    inplace=True)

# change MFMP spend to numeric column so agg will work
df_mfmp20_21['Non-FL MFMP Spend'] = df_mfmp20_21['Non-FL MFMP Spend'].str.replace(',', '', regex=True)
df_mfmp20_21['Non-FL MFMP Spend'] = df_mfmp20_21['Non-FL MFMP Spend'].astype('float')

# we have some extra vendors in our db
# df_mfmp['Vendors'].value_counts()

# agg & take the transpose
df_mfmp20_21_agg = df_mfmp20_21.agg({'Non-FL MFMP Spend':
                                     'sum',
                                     'Non-FL Vendors':
                                     'nunique',
                                     'Invoices':
                                     'nunique'
                                     }).reset_index().transpose()

# mk first row column names
df_mfmp20_21_agg.columns = df_mfmp20_21_agg.iloc[0]
df_mfmp20_21_agg = df_mfmp20_21_agg[1:]

# check metric totals for consistency with dashboard
# df_mfmp_agg['MFMP Spend'].sum()

# ended up with 500 more vendors somehow
# df_mfmp_agg['Vendors'].sum()

# add fy
df_mfmp20_21_agg['FY'] = '20-21'

# FY 19-20

filepath = 'Divisions/MFMP/FY2019-2020/DMS Government Impact Dashboard Report FY2019-2020.xlsx'
df_mfmp19_20 = pd.read_excel(filepath, dtype={'sum(Invoice Spend)': str})

# list columns, only need a couple
# df_mfmp.columns

# filter out non-FL impact transactions
df_mfmp19_20 = df_mfmp19_20[df_mfmp19_20['Supplier Location - PO State'] != 'FL'].reset_index()
# number of transactions left is substantial ~180K
# len(df_mfmp)

# select only the columns we need
df_mfmp19_20 = df_mfmp19_20.loc[:, ['Invoice ID', 'Supplier - Company Name',
                                    'sum(Invoice Spend)']]

# rename columns
df_mfmp19_20.rename(columns={'sum(Invoice Spend)': 'Non-FL MFMP Spend',
                             'Supplier - Company Name': 'Non-FL Vendors',
                             'Invoice ID': 'Invoices'},
                    inplace=True)

# change MFMP spend to numeric column so agg will work
df_mfmp19_20['Non-FL MFMP Spend'] = df_mfmp19_20['Non-FL MFMP Spend'].str.replace(',', '', regex=True)
df_mfmp19_20['Non-FL MFMP Spend'] = df_mfmp19_20['Non-FL MFMP Spend'].astype('float')

# we have some extra vendors in our db
# df_mfmp['Vendors'].value_counts()

# agg & take the transpose
df_mfmp19_20_agg = df_mfmp19_20.agg({'Non-FL MFMP Spend':
                                     'sum',
                                     'Non-FL Vendors':
                                     'nunique',
                                     'Invoices':
                                     'nunique'
                                     }).reset_index().transpose()

# mk first row column names
df_mfmp19_20_agg.columns = df_mfmp19_20_agg.iloc[0]
df_mfmp19_20_agg = df_mfmp19_20_agg[1:]
# check metric totals for consistency with dashboard
# df_mfmp_agg['MFMP Spend'].sum()

# ended up with 500 more vendors somehow
# df_mfmp_agg['Vendors'].sum()

# add fy
df_mfmp19_20_agg['FY'] = '19-20'

# FY 18-19
filepath = 'Divisions/MFMP/FY2018-2019/DMS Government Impact Dashboard Report FY2018-2019.xlsx'
df_mfmp18_19 = pd.read_excel(filepath, dtype={'sum(Invoice Spend)': str})

# list columns, only need a couple
# df_mfmp.columns

# filter out non-FL impact transactions
df_mfmp18_19 = df_mfmp18_19[df_mfmp18_19['Supplier Location - PO State'] != 'FL'].reset_index()
# number of transactions left is substantial ~180K
# len(df_mfmp)

# select only the columns we need
df_mfmp18_19 = df_mfmp18_19.loc[:, ['Invoice ID', 'Supplier - Company Name',
                                    'sum(Invoice Spend)']]

# rename columns
df_mfmp18_19.rename(columns={'sum(Invoice Spend)': 'Non-FL MFMP Spend',
                             'Supplier - Company Name': 'Non-FL Vendors',
                             'Invoice ID': 'Invoices'},
                    inplace=True)

# change MFMP spend to numeric column so agg will work
df_mfmp18_19['Non-FL MFMP Spend'] = df_mfmp18_19['Non-FL MFMP Spend'].str.replace(',', '', regex=True)
df_mfmp18_19['Non-FL MFMP Spend'] = df_mfmp18_19['Non-FL MFMP Spend'].astype('float')

# we have some extra vendors in our db
# df_mfmp['Vendors'].value_counts()

# agg & take the transpose
df_mfmp18_19_agg = df_mfmp18_19.agg({'Non-FL MFMP Spend':
                                     'sum',
                                     'Non-FL Vendors':
                                     'nunique',
                                     'Invoices':
                                     'nunique'
                                     }).reset_index().transpose()

# mk first row column names
df_mfmp18_19_agg.columns = df_mfmp18_19_agg.iloc[0]
df_mfmp18_19_agg = df_mfmp18_19_agg[1:]
# check metric totals for consistency with dashboard
# df_mfmp_agg['MFMP Spend'].sum()

# ended up with 500 more vendors somehow
# df_mfmp_agg['Vendors'].sum()

# add fy
df_mfmp18_19_agg['FY'] = '18-19'

# concat all yrs
df_mfmp_final = pd.concat([df_mfmp18_19_agg, df_mfmp19_20_agg,
                           df_mfmp20_21_agg, df_mfmp_agg], ignore_index=True)

df_out = pd.merge(left=df_out, left_on='FY',
                  right=df_mfmp_final, right_on='FY',
                  how='left')

# In[4]:

# ### RET
# Metrics: Benefits Paid, Number of Payees, Number of Employers, Employer Contributions

# FY 2021-2022
# #### Payees
# Metrics: Number of Payees, Payment Amount
# now get payments info
# skipping 1 header, assigned zip to str to preserve any leading zeroes
# skip 1 footer, which is grand total
filepath = 'Divisions/RET-IRIS/FY2021-2022/June/Dashboard data feed with city names.xlsx'

df_ret3 = pd.read_excel(filepath,
                        sheet_name='FY Payments per zip and city',
                        header=1,
                        dtype={'Florida Zip Code': str},
                        skipfooter=1)

# df_ret3.columns

# check payment amount
# includes all states
# df_ret3['Payment Amount'].sum()

# get first 60 rows to remove other state's data
df_ret3 = df_ret3.loc[:57, ['Payment Amount', 'Number of Payees']].reset_index()

# capitalize city
df_ret3.rename(columns={
                        'Payment Amount': 'Benefits Paid to Non-FL Retirees',
                        'Number of Payees': 'Non-FL Payees'
                        },
               inplace=True)


# group and agg to city level to merge with other retirement data
df_ret3_agg = df_ret3.agg({'Benefits Paid to Non-FL Retirees':
                           'sum',
                           'Non-FL Payees': 'sum'
                           }).reset_index().transpose()

df_ret3_agg.columns = df_ret3_agg.iloc[0]
df_ret3_agg = df_ret3_agg[1:]

# df_ret3_agg['Benefits Paid to Retirees'].sum()

# df_ret3_agg['Number of Payees'].sum()

df_ret3_agg['FY'] = '21-22'

df_ret21_22_agg = df_ret3_agg

# FY 2020-2021

# #### Payees
# Metrics: Number of Payees, Payment Amount
# now get payments info
# skipping 1 header, assigned zip to str to preserve any leading zeroes
# skip 1 footer, which is grand total
df_ret20_21 = pd.read_excel('Divisions/RET-IRIS/old FYs/20211220 dashboard data for FY19 FY20 FY21.xlsx',
                            sheet_name='FL Payments per zip and city',
                            dtype={'Fiscal Year': str})

# df_ret3.columns

# get first x rows to remove other state's data
df_ret20_21 = df_ret20_21.loc[:174, ['Payment Amount', 'Number of Payees', 'Fiscal Year']].reset_index()

# exclude FYs 20-21, 21-22
df_ret20_21 = df_ret20_21[df_ret20_21['Fiscal Year'] == '2021'].reset_index()

# check payment amount
# includes all states
# df_ret3['Payment Amount'].sum()

# capitalize city
df_ret20_21.rename(columns={
                        'Payment Amount': 'Benefits Paid to Non-FL Retirees',
                        'Number of Payees': 'Non-FL Payees'
                        },
                   inplace=True)


# group and agg to city level to merge with other retirement data
df_ret20_21_agg = df_ret20_21.agg({'Benefits Paid to Non-FL Retirees':
                                   'sum',
                                   'Non-FL Payees': 'sum'
                                   }).reset_index().transpose()

df_ret20_21_agg.columns = df_ret20_21_agg.iloc[0]
df_ret20_21_agg = df_ret20_21_agg[1:]

# df_ret20_21_agg['Benefits Paid to Retirees'].sum()

# df_ret20_21_agg['Number of Payees'].sum()

df_ret20_21_agg['FY'] = '20-21'

# FY 19-20

# #### Payees
# Metrics: Number of Payees, Payment Amount
# now get payments info
# skipping 1 header, assigned zip to str to preserve any leading zeroes
# skip 1 footer, which is grand total
df_ret19_20 = pd.read_excel('Divisions/RET-IRIS/old FYs/20211220 dashboard data for FY19 FY20 FY21.xlsx',
                            sheet_name='FL Payments per zip and city',
                            dtype={'Fiscal Year': str})

# df_ret3.columns

# get first x rows to remove other state's data
df_ret19_20 = df_ret19_20.loc[:174, ['Payment Amount', 'Number of Payees', 'Fiscal Year']].reset_index()

# exclude FYs 20-21, 21-22
df_ret19_20 = df_ret19_20[df_ret19_20['Fiscal Year'] == '2020'].reset_index()

# check payment amount
# includes all states
# df_ret3['Payment Amount'].sum()

# capitalize city
df_ret19_20.rename(columns={
                        'Payment Amount': 'Benefits Paid to Non-FL Retirees',
                        'Number of Payees': 'Non-FL Payees'
                        },
                   inplace=True)


# group and agg to city level to merge with other retirement data
df_ret19_20_agg = df_ret19_20.agg({'Benefits Paid to Non-FL Retirees':
                                   'sum',
                                   'Non-FL Payees': 'sum'
                                   }).reset_index().transpose()

df_ret19_20_agg.columns = df_ret19_20_agg.iloc[0]
df_ret19_20_agg = df_ret19_20_agg[1:]

# df_ret19_20_agg['Benefits Paid to Retirees'].sum()

# df_ret19_20_agg['Number of Payees'].sum()

df_ret19_20_agg['FY'] = '19-20'

# FY 18-19

# #### Payees
# Metrics: Number of Payees, Payment Amount
# now get payments info
# skipping 1 header, assigned zip to str to preserve any leading zeroes
# skip 1 footer, which is grand total
df_ret18_19 = pd.read_excel('Divisions/RET-IRIS/old FYs/20211220 dashboard data for FY19 FY20 FY21.xlsx',
                            sheet_name='FL Payments per zip and city',
                            dtype={'Fiscal Year': str})

# df_ret3.columns

# get first x rows to remove other state's data
df_ret18_19 = df_ret18_19.loc[:174, ['Payment Amount', 'Number of Payees', 'Fiscal Year']].reset_index()

# exclude FYs 20-21, 21-22
df_ret18_19 = df_ret18_19[df_ret18_19['Fiscal Year'] == '2019'].reset_index()

# check payment amount
# includes all states
# df_ret3['Payment Amount'].sum()

# capitalize city
df_ret18_19.rename(columns={
                        'Payment Amount': 'Benefits Paid to Non-FL Retirees',
                        'Number of Payees': 'Non-FL Payees'
                        },
                   inplace=True)


# group and agg to city level to merge with other retirement data
df_ret18_19_agg = df_ret18_19.agg({'Benefits Paid to Non-FL Retirees':
                                   'sum',
                                   'Non-FL Payees': 'sum'
                                   }).reset_index().transpose()

df_ret18_19_agg.columns = df_ret18_19_agg.iloc[0]
df_ret18_19_agg = df_ret18_19_agg[1:]

# df_ret18_19_agg['Benefits Paid to Retirees'].sum()

# df_ret18_19_agg['Number of Payees'].sum()

df_ret18_19_agg['FY'] = '18-19'

# concat all yrs
df_ret_final = pd.concat([df_ret18_19_agg, df_ret19_20_agg,
                          df_ret20_21_agg, df_ret21_22_agg], ignore_index=True)

df_out = pd.merge(left=df_out, left_on='FY',
                  right=df_ret_final, right_on='FY',
                  how='left')

df_out.to_excel('needed for tableau/out_of_state_metrics.xlsx')
