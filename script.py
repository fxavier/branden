# Record the process start time. 

import datetime
from datetime import datetime

try: 
    processStart = datetime.datetime.now()
except: 
    processStart = datetime.now()

# Import packages

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import json 
from pytz import timezone
import os
from shutil import copyfile
import re
import numpy as np

dhis2auth = ('svcECHO', '1WantToSeeMyD@t@!') 

# Set the working directory

main_dir = r"\\ad.abt.local\Projects\Projects\ECHO\Data"
# main_dir = r"C:\Projects\ECHO\Data"

# Check if the directory exists before changing to it
if os.path.exists(main_dir):
    os.chdir(main_dir)
    print(os.getcwd())
else:
    print(f"Directory not found: {main_dir}")

#ad.abt.local\Projects\Projects\ECHO\Data

#Back up old data

backupDate = ''

try: 
    data = pd.read_csv("dataUpdateDatetime.csv")
    print(data)
    backupDate = data.iloc[0, 2]  # Data run data, Central Africa Time
    backupDate = backupDate[:10]
    print(backupDate)
except:
    print('No data to back up')


# Create a backup directory in the form yyyy-mm-dd

if backupDate != '': 
    backupDir = main_dir + '\\Backup\\' + backupDate
    try: 
        os.mkdir(backupDir)
        print("Directory created: " + backupDir)
    except Exception as e:
        print("Directory not created: ", e.__class__, "occurred.")   


# Back up old data

fileList = ['dataUpdateDatetime.csv', 'organisationUnits.csv', 'dataElements.csv', 'categoryOptionCombos.csv', 'indicators.csv', 'dataValues.csv']

for filename in fileList: 
    try: 
        if backupDate != '':
            copyfile(main_dir + '\\' + filename, backupDir + '\\' + filename) 
            print(filename + " backed up")
        else:
            print(f"No backup directory available for {filename}")
    except Exception as e:
        print(filename + " not backed up: ", e.__class__, "occurred.")   


# Get Organisation Unit Groups

response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnitGroups", auth=dhis2auth)
organisationUnitGroups = response.json()['organisationUnitGroups']
organisationUnitGroups = pd.DataFrame(organisationUnitGroups)
organisationUnitGroups[:5]



echoOrgUnitGroup = organisationUnitGroups.loc[organisationUnitGroups['displayName'] == 'ECHO Sites']["id"].tolist()[0]
echoOrgUnitGroup

# Get the org unit ids (individual facilities) associated with ECHO

response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnitGroups/" + echoOrgUnitGroup, auth=dhis2auth)
echoOrgUnits = response.json()['organisationUnits']
echoOrgUnits = pd.DataFrame(echoOrgUnits)
echoOrgUnits = echoOrgUnits["id"].tolist()
echoOrgUnits[:5]


# Get reference data for all org units in DHIS2 (including those not involved in ECHO)

# NOTE: I would like to retrieve latitude and longitude in this request, but this version of the API (29) does not seem to support it.
# Instead, I am retrieving it below from the geospatial API.

response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnits?paging=false&fields=id,code,displayName,path", auth=dhis2auth)
organisationUnits = response.json()['organisationUnits']
organisationUnits = pd.DataFrame(organisationUnits)
organisationUnits[:5]

# Split the path string for the ECHO sites into ECHO / Province / District / Health Facility columns

orgUnitData = organisationUnits.loc[organisationUnits['id'].isin(echoOrgUnits)].copy()  # Use .copy() to avoid SettingWithCopyWarning

orgUnitData["path"].iloc[0].split('/')[2:]
pathData = orgUnitData.path.str.split('/', expand=True)
orgUnitData["province"] = pathData[2]  # Direct assignment is safe after .copy()
orgUnitData["district"] = pathData[3]  # Direct assignment is safe after .copy()
orgUnitData = orgUnitData.rename(columns={"displayName": "health facility"})
orgUnitData = orgUnitData.drop(columns=['path'])
orgUnitDataBackup = orgUnitData
orgUnitData[:5]



# Replace the province and district IDs with names

orgUnitData[["province", "district"]] = orgUnitData[["province", "district"]].replace(organisationUnits["id"].to_list(), organisationUnits["displayName"].to_list())
orgUnitData[:5]



# Get the geo coordinates associated with ECHO

# NOTE: I would like to retrieve the latitude and longitude in the org unit query above, but this version of the 
# API (29) does not seem to support it.

response = requests.get("https://dhis2.echomoz.org/api/29/geoFeatures?ou=ou:OU_GROUP-" + echoOrgUnitGroup, auth=dhis2auth)
#print(response.text)
echoGeoFeatures = response.json() 
echoGeoFeatures = pd.DataFrame(echoGeoFeatures)
echoGeoFeatures = echoGeoFeatures[["id", "na", "co"]]
echoGeoFeatures[:5]



# Parse out longitude and latitude (note: longitude is given first)

echoGeoFeatures["longitude"] = echoGeoFeatures["co"].str.split(',', expand=True)[0].str.strip('[')
echoGeoFeatures["latitude"] = echoGeoFeatures["co"].str.split(',', expand=True)[1].str.strip(']')
echoGeoFeatures = echoGeoFeatures[['id', 'latitude', 'longitude']]
echoGeoFeatures[:5]



# Merge geo information into Organisation Unit dataframe

orgUnitData = orgUnitData.merge(echoGeoFeatures, how='left', on='id')
orgUnitData[:5]



# Re-sort the organization unit columns to put health facility last

# orgUnitData = orgUnitDataBackup
cols = orgUnitData.columns.tolist()
cols = ['id', 'code', 'province', 'district', 'health facility', 'latitude', 'longitude']
orgUnitData = orgUnitData[cols]
orgUnitData[:5]



# Get Data Element Group Sets

response = requests.get("https://dhis2.echomoz.org/api/29/dataElementGroupSets?paging=false", auth=dhis2auth)
dataElementGroupSets = response.json()['dataElementGroupSets']
dataElementGroupSets = pd.DataFrame(dataElementGroupSets)
dataElementGroupSets



# Restrict to the data element group set for ECHO export data

echoExportDataElementGroupSet = dataElementGroupSets.loc[dataElementGroupSets['displayName'] == 'ECHO EXPORT']["id"].tolist()[0]
echoExportDataElementGroupSet


# Identify the Data Element Groups that are part of that set

response = requests.get("https://dhis2.echomoz.org/api/29/dataElementGroupSets/" + echoExportDataElementGroupSet, auth=dhis2auth)
exportDataElementGroups = response.json()['dataElementGroups']
exportDataElementGroups = pd.DataFrame(exportDataElementGroups)
exportDataElementGroups



# Get reference info on all data element groups

response = requests.get("https://dhis2.echomoz.org/api/dataElementGroups?paging=false", auth=dhis2auth)
dataElementGroups = response.json()['dataElementGroups']
dataElementGroups = pd.DataFrame(dataElementGroups)
dataElementGroups[:5]


# Identify the data element group for ECHO Targets

targetDataElementGroup = dataElementGroups.loc[dataElementGroups['displayName'] == 'ECHO MOZ | Targets']["id"].tolist()[0]
targetDataElementGroup


# Get reference info on all data elements

response = requests.get("https://dhis2.echomoz.org/api/dataElements?fields=id,displayName,displayShortName,dataElementGroups&paging=false", auth=dhis2auth)
dataElements = response.json()['dataElements']
dataElements = pd.DataFrame(dataElements)
dataElements[:5]



# Convert the data element group dictionaries to a list

separator = ';'
dataElementGroupString = []

for key, value in dataElements["dataElementGroups"].items():
    temp = value
    keylist = []
    for entry in temp:
            keylist.append(entry["id"])
    dataElementGroupString.append(separator.join(keylist))
    
dataElements["dataElementGroups"] = dataElementGroupString
dataElements[:5]


# Replace the data element group IDs with names

dataElements[["dataElementGroups"]] = dataElements[["dataElementGroups"]].replace(dataElementGroups["id"].to_list(), dataElementGroups["displayName"].to_list(), regex=True)
dataElements[:5]


# Get information on Category Option Combos
# NOTE: I believe attribute option combos also join to this list.

response = requests.get("https://dhis2.echomoz.org/api/categoryOptionCombos?paging=false", auth=dhis2auth)
categoryOptionCombos = response.json()['categoryOptionCombos']
categoryOptionCombos = pd.DataFrame(categoryOptionCombos)
categoryOptionCombos[:5]



# Get Indicator Group Sets

response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroupSets?paging=false", auth=dhis2auth)
indicatorGroupSets = response.json()['indicatorGroupSets']
indicatorGroupSets = pd.DataFrame(indicatorGroupSets)
indicatorGroupSets



# Make a dataframe of all Indicator Group Sets that contain the word "export"

df = indicatorGroupSets
export_indicator_group_sets = df.loc[df['displayName'].str.contains("export", flags=re.IGNORECASE)][['id', 'displayName']]
export_indicator_group_sets['type'] = 'indicator'
export_indicator_group_sets


# Change the ids for export indicator group sets to a list

echoExportIndicatorGroupSet = list(export_indicator_group_sets['id'])
echoExportIndicatorGroupSet



# Identify indicator groups that are part of the export set

exportIndicatorGroups = pd.DataFrame()
for item in echoExportIndicatorGroupSet:
    response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroupSets/" + item, auth=dhis2auth)
    response_df = response.json()['indicatorGroups']
    response_df = pd.DataFrame(response_df)
    exportIndicatorGroups = pd.concat([exportIndicatorGroups, response_df], ignore_index=True)

exportIndicatorGroups[:5]



# Get reference information on all indicator groups

response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroups?paging=false", auth=dhis2auth)
indicatorGroups = response.json()['indicatorGroups']
indicatorGroups = pd.DataFrame(indicatorGroups)
indicatorGroups[:5]



# Identify the TX_CURR export group

txCurrIndicatorGroup = indicatorGroups[indicatorGroups['displayName'].str.contains('EXPORT TX_CURR')]["id"].tolist()[0]
txCurrIndicatorGroup

# Get reference information on all indicators

response = requests.get("https://dhis2.echomoz.org/api/29/indicators?paging=false", auth=dhis2auth)
response = requests.get("https://dhis2.echomoz.org/api/29/indicators.json?fields=id,displayName,displayShortName,numerator,denominator,indicatorGroups&paging=false", auth=dhis2auth)
indicators = response.json()['indicators']
indicators = pd.DataFrame(indicators)
indicators = indicators.set_index("id")
indicators[:5]


# Convert the indicator group dictionaries to a list

separator = ';'
indicatorGroupString = []

for key, value in indicators["indicatorGroups"].items():
    temp = value #key.items()
    #print([key,value])
    keylist = []
    for entry in temp:
            keylist.append(entry["id"])
    indicatorGroupString.append(separator.join(keylist))
    
indicators["indicatorGroups"] = indicatorGroupString
indicators[:5]
    


# Replace the indicator group IDs with names

indicators[["indicatorGroups"]] = indicators[["indicatorGroups"]].replace(indicatorGroups["id"].to_list(), indicatorGroups["displayName"].to_list(), regex=True)
indicators = indicators.reset_index()
indicators[:5]

# Replace the IDs in the numerator and denominator columns with names (takes ~2 minutes)

start = datetime.now()

indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(["#"], [""], regex=True) # Remove the hash marks 
indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(["\."], [", "], regex=True) # Replace the period with a comma and space
indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(dataElements["id"].to_list(), dataElements["displayName"].to_list(), regex=True)
indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(categoryOptionCombos["id"].to_list(), categoryOptionCombos["displayName"].to_list(), regex=True)

elapsed_time = (datetime.now() - start) 
print('Updating indicator formulas: ', elapsed_time)

indicators[:5]



# Generate a list of months to pull

months = ['01','02','03','04','05','06','07','08','09','10','11','12']
years = list(range(2019, datetime.now().year+1))
years = [str(i) for i in years]

initialPeriodList = [sub1 + sub2 for sub1 in years for sub2 in months] 
print('Initial Month List: ', initialPeriodList)
firstMonth = '201909' # HARD-CODE for start of ECHO Dashboards
currentMonth = str(datetime.now().year) + months[datetime.now().month - 1]
periodList = list(filter(lambda x: x >= firstMonth and x < currentMonth, initialPeriodList))
print('Relevant Month List: ', periodList)


# Generate a list of quarters to pull

quarters = ['Q1', 'Q2', 'Q3', 'Q4']
initialQuarterList = [sub1 + sub2 for sub1 in years for sub2 in quarters]
print('Initial Quarter List: ', initialQuarterList)
firstQuarter = '2019Q4' # HARD-CODE for start of ECHO Dashboards
currentQuarterNumber = (datetime.now().month-1)//3 + 1
currentQuarter = str(datetime.now().year) + 'Q' + str(currentQuarterNumber)
quarterList = list(filter(lambda x: x >= firstQuarter and x < currentQuarter, initialQuarterList))
print('Relevant Quarter List: ', quarterList)


# Create a list of all periods that should have targets

scaffoldPeriods = [x for x in (initialPeriodList + initialQuarterList) if x >= '2020']  # Targets begin in January 2020
scaffoldPeriods[:5]


# Create a year reference table

periods = pd.DataFrame(scaffoldPeriods)
periods.columns = ['period']
periods['year'] = periods['period'].str[0:4]
periods['type'] = np.where(periods['period'].str[4]=='Q', 'Q', 'M')
periods.tail(20)



# Query monthly indicators from the analytics API

dataUpdateDatetime = datetime.utcnow()

# Create a data frame to hold all values
allDataValues = pd.DataFrame()
allIndicatorValues = pd.DataFrame()
exportIndicatorGroups['results'] = 0

dataRetrievalStart = datetime.now()

for period in periodList: 
    print('Retrieving Period: ' + period)
    start = datetime.now()
    
    for indicatorGroup in list(exportIndicatorGroups['id']):
        response = requests.get("https://dhis2.echomoz.org/api/29/analytics?dimension=pe:" + period + "&dimension=dx:IN_GROUP-" + indicatorGroup + "&dimension=ou:OU_GROUP-" + echoOrgUnitGroup, auth=dhis2auth)
        if response.status_code == 200 and response.text != '{}':           # Valid response, and not empty 
            dataValues = response.json()['rows']
            dataValues = pd.DataFrame(dataValues)
            allIndicatorValues = pd.concat([allIndicatorValues, dataValues], ignore_index=True, sort=False)
              
        indicatorHeaders = response.json()['headers']

        exportIndicatorGroups.loc[exportIndicatorGroups['id'] == indicatorGroup, 'results'] = \
            exportIndicatorGroups.loc[exportIndicatorGroups['id'] == indicatorGroup, 'results'] + len(dataValues.index)
        
    elapsed_time = (datetime.now() - start) 
    print(elapsed_time)
    
dataRetrievalElapsedTime = (datetime.now() - dataRetrievalStart) 
print('Indicator Monthly Retrieval: ', dataRetrievalElapsedTime)

indicatorHeaders = pd.DataFrame(indicatorHeaders)
allIndicatorValues.columns = indicatorHeaders['column']

allIndicatorValues[:5]



# Review results retrieved so far

exportIndicatorGroups[:5]



# Identify indicator groups that retrieved no monthly results

noMonthlyResults = list(exportIndicatorGroups.loc[lambda exportIndicatorGroups: exportIndicatorGroups['results'] == 0]['id'])
noMonthlyResults



# Query quarterly indicators from the API, for indicators that returned no monthly results (only)
# NOTE: if you retrieve quarterly results for indicators for which you already have monthly results,
# you will get summarized values that will inflate numbers in Tableau.

if len(noMonthlyResults) > 0 : 

    allIndicatorQuarterlyValues = pd.DataFrame()

    dataRetrievalStart = datetime.now()

    for period in quarterList: 
        print('Retrieving Quarter: ' + period)
        start = datetime.now()

        for indicatorGroup in noMonthlyResults:
            response = requests.get("https://dhis2.echomoz.org/api/29/analytics?dimension=pe:" + period + "&dimension=dx:IN_GROUP-" + indicatorGroup + "&dimension=ou:OU_GROUP-" + echoOrgUnitGroup, auth=dhis2auth)
            if response.status_code == 200 and response.text != '{}':           # Valid response, and not empty 
                dataValues = response.json()['rows']
                dataValues = pd.DataFrame(dataValues)
                allIndicatorQuarterlyValues = pd.concat([allIndicatorQuarterlyValues, dataValues], ignore_index=True, sort=False)

            exportIndicatorGroups.loc[exportIndicatorGroups['id'] == indicatorGroup, 'results'] = \
                exportIndicatorGroups.loc[exportIndicatorGroups['id'] == indicatorGroup, 'results'] + len(dataValues.index)

        elapsed_time = (datetime.now() - start) 
        print(elapsed_time)

    indicatorHeaders = response.json()['headers']
    indicatorHeaders = pd.DataFrame(indicatorHeaders)
    allIndicatorQuarterlyValues.columns = indicatorHeaders['column']

    allIndicatorValues = pd.concat([allIndicatorValues, allIndicatorQuarterlyValues], ignore_index=True, sort=False)

    dataRetrievalElapsedTime = (datetime.now() - dataRetrievalStart) 
    print('Indicator Quarterly Retrieval: ', dataRetrievalElapsedTime)

allIndicatorValues[:5]



# Query data elements on a monthly basis from the API

allDataElementValues = pd.DataFrame()
exportDataElementGroups['results'] = 0

dataRetrievalStart = datetime.now()

if len(exportDataElementGroups) > 0: 
    for period in periodList: 
        print('Retrieving Period: ' + period)
        start = datetime.now()

        for dataElementGroup in list(exportDataElementGroups['id']):
            # Working with data element groups
            x = "https://dhis2.echomoz.org/api/29/analytics?dimension=pe:" + period + "&dimension=dx:DE_GROUP-" + dataElementGroup + "&dimension=co&dimension=ou:OU_GROUP-" + echoOrgUnitGroup
            response = requests.get("https://dhis2.echomoz.org/api/29/analytics?dimension=pe:" + period + "&dimension=dx:DE_GROUP-" + dataElementGroup + "&dimension=co&dimension=ou:OU_GROUP-" + echoOrgUnitGroup, auth=dhis2auth)
            if response.status_code == 200 and response.text != '{}':           # Valid response, and not empty 
                dataValues = response.json()['rows']
                dataValues = pd.DataFrame(dataValues)
                allDataElementValues = pd.concat([allDataElementValues, dataValues], ignore_index=True, sort=False)
                
            dataElementHeaders = response.json()['headers']       
        
            exportDataElementGroups.loc[exportDataElementGroups['id'] == dataElementGroup, 'results'] = \
                exportDataElementGroups.loc[exportDataElementGroups['id'] == dataElementGroup, 'results'] + len(dataValues.index)   

        elapsed_time = (datetime.now() - start) 
        print(elapsed_time)
        
    dataElementHeaders = pd.DataFrame(dataElementHeaders)
    allDataElementValues.columns = dataElementHeaders['column']        

dataRetrievalElapsedTime = (datetime.now() - dataRetrievalStart) 
print('Data Element Retrieval (Monthly): ', dataRetrievalElapsedTime)

allDataElementValues[:5]




# Review total data element results returned

exportDataElementGroups



# Identify data element groups that retrieved no monthly results

noMonthlyResults = list(exportDataElementGroups.loc[lambda exportDataElementGroups: exportDataElementGroups['results'] == 0]['id'])
noMonthlyResults



# Query targets on a annual basis from the API
# Note: This will retrieve targets that have been set at the facility level. The targets for community index testing are set at the district level, and wll need to be retrieved differently.

targetDataValues = pd.DataFrame()
dataRetrievalStart = datetime.now()

for period in years: 
    print('Retrieving Targets for Year: ' + period)
    start = datetime.now()

    # Working with data element groups
    response = requests.get("https://dhis2.echomoz.org/api/29/analytics?dimension=pe:" + period + "&dimension=dx:DE_GROUP-" + targetDataElementGroup + "&dimension=co&dimension=ou:OU_GROUP-" + echoOrgUnitGroup, auth=dhis2auth)
    if response.status_code == 200 and response.text != '{}':           # Valid response, and not empty 
        dataValues = response.json()['rows']
        dataValues = pd.DataFrame(dataValues)
        targetDataValues = pd.concat([targetDataValues, dataValues], ignore_index=True, sort=False)

    dataElementHeaders = response.json()['headers']       

    elapsed_time = (datetime.now() - start) 
    print(elapsed_time)

indicatorHeaders = response.json()['headers']
indicatorHeaders = pd.DataFrame(indicatorHeaders)
targetDataValues.columns = indicatorHeaders['column']

dataRetrievalElapsedTime = (datetime.now() - dataRetrievalStart) 
print('Data Element Retrieval (Annual Targets): ', dataRetrievalElapsedTime)

targetDataValues[:5]


# Target values will be divided by 12 to determine monthly amounts. For TX_CURR and PrEP_CURR, specify that we will divide by 1 instead 
# (i.e., will not split it by month, but will use the same value each month).

# NOTE: This field is still called "txCurr", but now also has a row for PrEP_CURR. Rename it when possible.

txCurrTargetDataElements = dataElements.loc[dataElements['displayName'].str.contains(r'target.*_curr', flags=re.IGNORECASE, regex=True)]["id"].tolist()
txCurrTargetDataElements = pd.DataFrame(txCurrTargetDataElements, columns = ['Data'])
txCurrTargetDataElements["Divisor"] = 1  
txCurrTargetDataElements["type"] = 'M'  # Monthly
txCurrTargetDataElements[:5]



# Target values for TX_PVLS will be divided by 4, since they are quarterly

txPlvsTargetDataElements = dataElements.loc[dataElements['displayName'].str.contains(r'target.*tx_pvls', flags=re.IGNORECASE, regex=True)]["id"].tolist()
txPlvsTargetDataElements = pd.DataFrame(txPlvsTargetDataElements, columns = ['Data'])
txPlvsTargetDataElements["Divisor"] = 4  
txPlvsTargetDataElements["type"] = 'Q'  # Quarterly
txPlvsTargetDataElements



# Set all data values except for TX_CURR and PrEP_CURR to divide by 12 months

specialTargetDataElements = txCurrTargetDataElements.append(txPlvsTargetDataElements)
targetDataValues = targetDataValues.merge(specialTargetDataElements, how="left", on='Data')

targetDataValues["Divisor"] = targetDataValues["Divisor"].fillna(12)
targetDataValues["type"] = targetDataValues["type"].fillna('M')
targetDataValues[:5]



# Divide the annual value into a monthly or quarterly value, if appropriate, and drop the Divisor column

targetDataValues["Value"] = (targetDataValues["Value"].astype(float) / targetDataValues["Divisor"]).astype(object)
targetDataValues = targetDataValues.drop(columns=['Divisor'])
targetDataValues[:5]



# Merge the yearly targets with the table by month, producing a scaffold with 12x as many records.

targetDataValues = targetDataValues.merge(periods, left_on=['Period', 'type'], right_on=['year', 'type'])
targetDataValues["Period"] = targetDataValues["period"]
targetDataValues = targetDataValues.drop(columns=['period', 'year'])
targetDataValues[:5]



# Union the indicator results and the data element results

if len(exportDataElementGroups) > 0: 
    allDataValues = pd.concat([allIndicatorValues, allDataElementValues], axis=0, ignore_index=True, sort=False)
else:
    allDataValues = allIndicatorValues
    allDataValues['Category option combo'] = np.nan
    
allDataValues[:5]



# Union the target results and the indicator/data element results

if len(targetDataValues) > 0: 
    allDataValues = pd.concat([allDataValues, targetDataValues], axis=0, ignore_index=True, sort=False)
    
allDataValues[:5]



# Remove duplicates, in case an indicator or data element has been requested in two different sets

allDataValues = allDataValues.drop_duplicates()
allDataValues[:5]



# Create a dataframe for the update datetimes, with columns for US/Eastern and Central Africa Time

dataUpdateDatetime = pd.DataFrame(data = [dataUpdateDatetime], columns = ['Data Update Datetime UTC'])
dataUpdateDatetime['Data Update Datetime US/Eastern'] = dataUpdateDatetime['Data Update Datetime UTC'][0].replace(tzinfo=timezone('UTC')).astimezone(tz=timezone('US/Eastern'))
dataUpdateDatetime['Data Update Datetime Mozambique'] = dataUpdateDatetime['Data Update Datetime UTC'][0].replace(tzinfo=timezone('UTC')).astimezone(tz=timezone('Africa/Harare'))
dataUpdateDatetime


# Write the .csv files

start = datetime.now()

dataUpdateDatetime.to_csv('dataUpdateDatetime.csv', index=False)
orgUnitData.to_csv('organisationUnits.csv', index=False) 
dataElements.to_csv('dataElements.csv', index=False)
categoryOptionCombos.to_csv('categoryOptionCombos.csv', index=False)
indicators.to_csv('indicators.csv', index=False)

allDataValues.to_csv('dataValues.csv', index=False)

elapsed_time = (datetime.now() - start) 
print('Writing .csv files: ', elapsed_time)



# Show total time for all processes

totalElapsedTime = (datetime.now() - processStart) 
print('Total Process Runtime: ', totalElapsedTime)


