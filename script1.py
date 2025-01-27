import os
import re
import json
import numpy as np
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from shutil import copyfile
from pytz import timezone
from datetime import datetime

# ---------------------------------------------------------------------
# 1. START TIME
# ---------------------------------------------------------------------
process_start = datetime.now()

# ---------------------------------------------------------------------
# 2. AUTH & MAIN DIRECTORY
# ---------------------------------------------------------------------
dhis2auth = ('svcECHO', '1WantToSeeMyD@t@!')  

main_dir = r"\\ad.abt.local\Projects\Projects\ECHO\Data"
# main_dir = r"C:\\Docs\\Data"
os.chdir(main_dir)
print("Working directory:", os.getcwd())

# ---------------------------------------------------------------------
# 3. BACKUP OLD DATA
# ---------------------------------------------------------------------
backupDate = ''
try:
    data = pd.read_csv("dataUpdateDatetime.csv")
    print(data)
    # Data run date, Central Africa Time is presumably column #3
    backupDate = data.iloc[0, 2]  # might fail if shape is not as expected
    backupDate = backupDate[:10]
    print("Backup date detected:", backupDate)
except Exception as e:
    print('No data to back up or error reading dataUpdateDatetime.csv:', e)

# Create a backup directory in the form yyyy-mm-dd
if backupDate != '':
    backup_dir = os.path.join(main_dir, 'Backup', backupDate)
    try:
        os.mkdir(backup_dir)
        print("Directory created:", backup_dir)
    except Exception as e:
        print("Directory not created:", e.__class__, "occurred.")

# Back up old data
file_list = [
    'dataUpdateDatetime.csv',
    'organisationUnits.csv',
    'dataElements.csv',
    'categoryOptionCombos.csv',
    'indicators.csv',
    'dataValues.csv'
]

for filename in file_list:
    source_path = os.path.join(main_dir, filename)
    dest_path = os.path.join(backup_dir, filename)
    try:
        copyfile(source_path, dest_path)
        print(filename, "backed up")
    except Exception as e:
        print(filename, "not backed up:", e.__class__, "occurred.")

# ---------------------------------------------------------------------
# 4. ORGANISATION UNIT GROUPS
# ---------------------------------------------------------------------
response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnitGroups",
                        auth=dhis2auth)
organisationUnitGroups = response.json()['organisationUnitGroups']
organisationUnitGroups = pd.DataFrame(organisationUnitGroups)
echoOrgUnitGroup = organisationUnitGroups.loc[
    organisationUnitGroups['displayName'] == 'ECHO Sites', "id"
].tolist()[0]

response = requests.get(f"https://dhis2.echomoz.org/api/29/organisationUnitGroups/{echoOrgUnitGroup}",
                        auth=dhis2auth)
echoOrgUnits = response.json()['organisationUnits']
echoOrgUnits = pd.DataFrame(echoOrgUnits)
echoOrgUnits = echoOrgUnits["id"].tolist()

# ---------------------------------------------------------------------
# 5. REFERENCE DATA FOR ALL ORG UNITS
# ---------------------------------------------------------------------
response = requests.get(
    "https://dhis2.echomoz.org/api/29/organisationUnits?paging=false&fields=id,code,displayName,path",
    auth=dhis2auth
)
organisationUnits = response.json()['organisationUnits']
organisationUnits = pd.DataFrame(organisationUnits)

orgUnitData = organisationUnits.loc[
    organisationUnits['id'].isin(echoOrgUnits)
].copy()  

pathData = orgUnitData['path'].str.split('/', expand=True)
orgUnitData["province"] = pathData[2]
orgUnitData["district"] = pathData[3]
orgUnitData = orgUnitData.rename(columns={"displayName": "health facility"})
orgUnitData.drop(columns=['path'], inplace=True)

# Replace the province and district IDs with names
orgUnitData[["province", "district"]] = orgUnitData[["province", "district"]].replace(
    organisationUnits["id"].to_list(),
    organisationUnits["displayName"].to_list()
)

# Get geocoordinates
response = requests.get(f"https://dhis2.echomoz.org/api/29/geoFeatures?ou=ou:OU_GROUP-{echoOrgUnitGroup}",
                        auth=dhis2auth)
echoGeoFeatures = pd.DataFrame(response.json())
echoGeoFeatures = echoGeoFeatures[["id", "na", "co"]]

echoGeoFeatures["longitude"] = echoGeoFeatures["co"].str.split(',', 1).str[0].str.strip('[')
echoGeoFeatures["latitude"] = echoGeoFeatures["co"].str.split(',', 1).str[1].str.strip(']')
echoGeoFeatures = echoGeoFeatures[['id', 'latitude', 'longitude']]

orgUnitData = orgUnitData.merge(echoGeoFeatures, how='left', on='id')

cols = ['id', 'code', 'province', 'district', 'health facility', 'latitude', 'longitude']
orgUnitData = orgUnitData[cols]

# ---------------------------------------------------------------------
# 6. DATA ELEMENT GROUP SETS
# ---------------------------------------------------------------------
response = requests.get("https://dhis2.echomoz.org/api/29/dataElementGroupSets?paging=false",
                        auth=dhis2auth)
dataElementGroupSets = pd.DataFrame(response.json()['dataElementGroupSets'])
echoExportDataElementGroupSet = dataElementGroupSets.loc[
    dataElementGroupSets['displayName'] == 'ECHO EXPORT', "id"
].tolist()[0]

response = requests.get(
    f"https://dhis2.echomoz.org/api/29/dataElementGroupSets/{echoExportDataElementGroupSet}",
    auth=dhis2auth
)
exportDataElementGroups = pd.DataFrame(response.json()['dataElementGroups'])

response = requests.get("https://dhis2.echomoz.org/api/dataElementGroups?paging=false",
                        auth=dhis2auth)
dataElementGroups = pd.DataFrame(response.json()['dataElementGroups'])

targetDataElementGroup = dataElementGroups.loc[
    dataElementGroups['displayName'] == 'ECHO MOZ | Targets', "id"
].tolist()[0]

# ---------------------------------------------------------------------
# 7. ALL DATA ELEMENTS
# ---------------------------------------------------------------------
response = requests.get(
    "https://dhis2.echomoz.org/api/dataElements?fields=id,displayName,displayShortName,dataElementGroups&paging=false",
    auth=dhis2auth
)
dataElements = pd.DataFrame(response.json()['dataElements'])

# Convert the data element group dictionaries to a semicolon-delimited list
sep = ';'
group_strings = []
for idx, row_value in dataElements["dataElementGroups"].iteritems():
    keylist = [entry["id"] for entry in row_value]
    group_strings.append(sep.join(keylist))
dataElements["dataElementGroups"] = group_strings

# Replace the data element group IDs with names
dataElements["dataElementGroups"] = dataElements["dataElementGroups"].replace(
    dataElementGroups["id"].to_list(),
    dataElementGroups["displayName"].to_list(),
    regex=True
)

# ---------------------------------------------------------------------
# 8. CATEGORY OPTION COMBOS
# ---------------------------------------------------------------------
response = requests.get("https://dhis2.echomoz.org/api/categoryOptionCombos?paging=false",
                        auth=dhis2auth)
categoryOptionCombos = pd.DataFrame(response.json()['categoryOptionCombos'])

# ---------------------------------------------------------------------
# 9. INDICATOR GROUP SETS
# ---------------------------------------------------------------------
response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroupSets?paging=false",
                        auth=dhis2auth)
indicatorGroupSets = pd.DataFrame(response.json()['indicatorGroupSets'])

# Filter for group sets containing 'export'
df = indicatorGroupSets
export_indicator_group_sets = df.loc[
    df['displayName'].str.contains("export", flags=re.IGNORECASE),
    ['id', 'displayName']
]
export_indicator_group_sets['type'] = 'indicator'
echoExportIndicatorGroupSet = list(export_indicator_group_sets['id'])

exportIndicatorGroups = pd.DataFrame()
for item in echoExportIndicatorGroupSet:
    resp = requests.get(f"https://dhis2.echomoz.org/api/29/indicatorGroupSets/{item}",
                        auth=dhis2auth)
    resp_df = pd.DataFrame(resp.json()['indicatorGroups'])
    # Instead of append, use pd.concat
    exportIndicatorGroups = pd.concat([exportIndicatorGroups, resp_df], ignore_index=True)

response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroups?paging=false",
                        auth=dhis2auth)
indicatorGroups = pd.DataFrame(response.json()['indicatorGroups'])

txCurrIndicatorGroup = indicatorGroups[
    indicatorGroups['displayName'].str.contains('EXPORT TX_CURR')
]["id"].tolist()[0]

# ---------------------------------------------------------------------
# 10. ALL INDICATORS
# ---------------------------------------------------------------------
response = requests.get(
    "https://dhis2.echomoz.org/api/29/indicators.json?fields=id,displayName,displayShortName,numerator,denominator,indicatorGroups&paging=false",
    auth=dhis2auth
)
indicators = pd.DataFrame(response.json()['indicators'])
indicators = indicators.set_index("id")

# Convert the indicator group dictionaries
sep = ';'
indicator_group_strings = []
for idx, row_value in indicators["indicatorGroups"].iteritems():
    keylist = [entry["id"] for entry in row_value]
    indicator_group_strings.append(sep.join(keylist))

indicators["indicatorGroups"] = indicator_group_strings

# Replace the IDs with names
indicators.reset_index(inplace=True)
indicators["indicatorGroups"] = indicators["indicatorGroups"].replace(
    indicatorGroups["id"].to_list(),
    indicatorGroups["displayName"].to_list(),
    regex=True
)

# Replace IDs in numerator/denominator with data element / cat option combo names
start = datetime.now()
indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(
    ["#"], [""], regex=True
)
indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(
    ["\."], [", "], regex=True
)
indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(
    dataElements["id"].to_list(),
    dataElements["displayName"].to_list(),
    regex=True
)
indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(
    categoryOptionCombos["id"].to_list(),
    categoryOptionCombos["displayName"].to_list(),
    regex=True
)
elapsed_time = (datetime.now() - start)
print('Updating indicator formulas took:', elapsed_time)

# ---------------------------------------------------------------------
# 11. PERIOD LISTS
# ---------------------------------------------------------------------
months = [f"{i:02d}" for i in range(1,13)]
years = [str(y) for y in range(2019, datetime.now().year+1)]
initialPeriodList = [y + m for y in years for m in months]

firstMonth = '201909'  # start of ECHO Dashboards
currentMonth = str(datetime.now().year) + months[datetime.now().month - 1]
periodList = list(filter(lambda x: x >= firstMonth and x < currentMonth, initialPeriodList))

quarters = ['Q1','Q2','Q3','Q4']
initialQuarterList = [y + q for y in years for q in quarters]
firstQuarter = '2019Q4'
currentQuarterNumber = (datetime.now().month - 1)//3 + 1
currentQuarter = str(datetime.now().year) + 'Q' + str(currentQuarterNumber)
quarterList = list(filter(lambda x: x >= firstQuarter and x < currentQuarter, initialQuarterList))

# Create a list of all periods that should have targets
scaffoldPeriods = [p for p in (initialPeriodList + initialQuarterList) if p >= '2020']

# Create a year reference table
periods = pd.DataFrame(scaffoldPeriods, columns=['period'])
periods['year'] = periods['period'].str[0:4]
periods['type'] = np.where(periods['period'].str[4]=='Q', 'Q', 'M')

# ---------------------------------------------------------------------
# 12. QUERY INDICATORS (Monthly, then Quarterly if no monthly)
# ---------------------------------------------------------------------
data_update_datetime = datetime.utcnow()

all_indicator_values_list = []
exportIndicatorGroups['results'] = 0

data_retrieval_start = datetime.now()
for per in periodList:
    print('Retrieving Period:', per)
    start_time = datetime.now()
    for indicatorGroup in exportIndicatorGroups['id']:
        url = f"https://dhis2.echomoz.org/api/29/analytics?dimension=pe:{per}&dimension=dx:IN_GROUP-{indicatorGroup}&dimension=ou:OU_GROUP-{echoOrgUnitGroup}"
        resp = requests.get(url, auth=dhis2auth)
        if resp.status_code == 200 and resp.text != '{}':
            dataValues = pd.DataFrame(resp.json()['rows'])
            all_indicator_values_list.append(dataValues)
            exportIndicatorGroups.loc[
                exportIndicatorGroups['id'] == indicatorGroup, 'results'
            ] += len(dataValues.index)
    print("Elapsed for period:", datetime.now() - start_time)

data_retrieval_elapsed_time = datetime.now() - data_retrieval_start
print('Indicator Monthly Retrieval total:', data_retrieval_elapsed_time)

if len(all_indicator_values_list) > 0:
    # Build a single DataFrame from the list
    allIndicatorValues = pd.concat(all_indicator_values_list, ignore_index=True)
    indicator_headers = pd.DataFrame(resp.json()['headers'])
    allIndicatorValues.columns = indicator_headers['column']
else:
    allIndicatorValues = pd.DataFrame()

noMonthlyResults = exportIndicatorGroups.loc[
    exportIndicatorGroups['results'] == 0, 'id'
].tolist()

# Query quarterly only for those that had no monthly results
if len(noMonthlyResults) > 0:
    all_indicator_quarterly_values_list = []
    data_retrieval_start = datetime.now()

    for per in quarterList:
        print('Retrieving Quarter:', per)
        start_time = datetime.now()
        for indicatorGroup in noMonthlyResults:
            url = f"https://dhis2.echomoz.org/api/29/analytics?dimension=pe:{per}&dimension=dx:IN_GROUP-{indicatorGroup}&dimension=ou:OU_GROUP-{echoOrgUnitGroup}"
            resp = requests.get(url, auth=dhis2auth)
            if resp.status_code == 200 and resp.text != '{}':
                dataValues = pd.DataFrame(resp.json()['rows'])
                all_indicator_quarterly_values_list.append(dataValues)
                exportIndicatorGroups.loc[
                    exportIndicatorGroups['id'] == indicatorGroup, 'results'
                ] += len(dataValues.index)
        print("Elapsed for quarter:", datetime.now() - start_time)

    if len(all_indicator_quarterly_values_list) > 0:
        allIndicatorQuarterlyValues = pd.concat(all_indicator_quarterly_values_list, ignore_index=True)
        indicator_headers = pd.DataFrame(resp.json()['headers'])
        allIndicatorQuarterlyValues.columns = indicator_headers['column']
        # Union with monthly
        allIndicatorValues = pd.concat([allIndicatorValues, allIndicatorQuarterlyValues],
                                       ignore_index=True)
    data_retrieval_elapsed_time = datetime.now() - data_retrieval_start
    print('Indicator Quarterly Retrieval total:', data_retrieval_elapsed_time)

# ---------------------------------------------------------------------
# 13. QUERY DATA ELEMENTS (Monthly)
# ---------------------------------------------------------------------
all_data_element_values_list = []
exportDataElementGroups['results'] = 0

if not exportDataElementGroups.empty:
    data_retrieval_start = datetime.now()
    for per in periodList:
        print('Retrieving Period (DE):', per)
        start_time = datetime.now()
        for deg_id in exportDataElementGroups['id']:
            url = (
                "https://dhis2.echomoz.org/api/29/analytics?dimension=pe:{p}"
                "&dimension=dx:DE_GROUP-{g}&dimension=co&dimension=ou:OU_GROUP-{ou}"
            ).format(p=per, g=deg_id, ou=echoOrgUnitGroup)
            resp = requests.get(url, auth=dhis2auth)
            if resp.status_code == 200 and resp.text != '{}':
                df = pd.DataFrame(resp.json()['rows'])
                all_data_element_values_list.append(df)
                exportDataElementGroups.loc[
                    exportDataElementGroups['id'] == deg_id, 'results'
                ] += len(df.index)
        print("Elapsed for period (DE):", datetime.now() - start_time)

    if len(all_data_element_values_list) > 0:
        allDataElementValues = pd.concat(all_data_element_values_list, ignore_index=True)
        dataElementHeaders = pd.DataFrame(resp.json()['headers'])
        allDataElementValues.columns = dataElementHeaders['column']
    else:
        allDataElementValues = pd.DataFrame()

    data_retrieval_elapsed_time = datetime.now() - data_retrieval_start
    print('Data Element Retrieval (Monthly):', data_retrieval_elapsed_time)
else:
    allDataElementValues = pd.DataFrame()

# ---------------------------------------------------------------------
# 14. QUERY TARGETS (Annually)
# ---------------------------------------------------------------------
targetDataValues_list = []
data_retrieval_start = datetime.now()

for year_val in years:
    print('Retrieving Targets for Year:', year_val)
    start_time = datetime.now()
    url = (
        "https://dhis2.echomoz.org/api/29/analytics?dimension=pe:{year}"
        "&dimension=dx:DE_GROUP-{grp}&dimension=co&dimension=ou:OU_GROUP-{ou}"
    ).format(year=year_val, grp=targetDataElementGroup, ou=echoOrgUnitGroup)
    resp = requests.get(url, auth=dhis2auth)
    if resp.status_code == 200 and resp.text != '{}':
        df = pd.DataFrame(resp.json()['rows'])
        targetDataValues_list.append(df)
    print("Elapsed for target retrieval:", datetime.now() - start_time)

if len(targetDataValues_list) > 0:
    targetDataValues = pd.concat(targetDataValues_list, ignore_index=True)
    indicator_headers = pd.DataFrame(resp.json()['headers'])
    targetDataValues.columns = indicator_headers['column']
else:
    targetDataValues = pd.DataFrame()

data_retrieval_elapsed_time = datetime.now() - data_retrieval_start
print('Data Element Retrieval (Annual Targets):', data_retrieval_elapsed_time)

# ---------------------------------------------------------------------
# 15. TARGET POST-PROCESSING (Divide annual -> monthly or quarterly)
# ---------------------------------------------------------------------
# Identify the data elements that get divided by 1 or 4
txCurrTargetDataElements = dataElements.loc[
    dataElements['displayName'].str.contains(r'target.*_curr', flags=re.IGNORECASE),
    "id"
].tolist()
txCurrTargetDataElements = pd.DataFrame(txCurrTargetDataElements, columns=['Data'])
txCurrTargetDataElements["Divisor"] = 1
txCurrTargetDataElements["type"] = 'M'

txPlvsTargetDataElements = dataElements.loc[
    dataElements['displayName'].str.contains(r'target.*tx_pvls', flags=re.IGNORECASE),
    "id"
].tolist()
txPlvsTargetDataElements = pd.DataFrame(txPlvsTargetDataElements, columns=['Data'])
txPlvsTargetDataElements["Divisor"] = 4
txPlvsTargetDataElements["type"] = 'Q'

specialTargetDataElements = pd.concat([txCurrTargetDataElements, txPlvsTargetDataElements],
                                      ignore_index=True)

if not targetDataValues.empty:
    targetDataValues = targetDataValues.merge(specialTargetDataElements, how="left", on='Data')
    targetDataValues["Divisor"] = targetDataValues["Divisor"].fillna(12)
    targetDataValues["type"] = targetDataValues["type"].fillna('M')
    targetDataValues["Value"] = (targetDataValues["Value"].astype(float) /
                                 targetDataValues["Divisor"]).astype(object)
    targetDataValues.drop(columns=['Divisor'], inplace=True)
    # Merge with the monthly/quarterly scaffold
    targetDataValues = targetDataValues.merge(periods, left_on=['Period','type'],
                                              right_on=['year','type'])
    targetDataValues["Period"] = targetDataValues["period"]
    targetDataValues.drop(columns=['period','year'], inplace=True)

# ---------------------------------------------------------------------
# 16. UNION INDICATORS & DATA ELEMENTS
# ---------------------------------------------------------------------
if not allDataElementValues.empty:
    # Add a placeholder column if needed
    if 'Category option combo' not in allIndicatorValues.columns:
        allIndicatorValues['Category option combo'] = np.nan
    allDataValues = pd.concat([allIndicatorValues, allDataElementValues],
                              axis=0, ignore_index=True)
else:
    allDataValues = allIndicatorValues
    if 'Category option combo' not in allDataValues.columns:
        allDataValues['Category option combo'] = np.nan

# Union the target results
if not targetDataValues.empty:
    allDataValues = pd.concat([allDataValues, targetDataValues],
                              axis=0, ignore_index=True)

# Remove duplicates
allDataValues.drop_duplicates(inplace=True)

# ---------------------------------------------------------------------
# 17. CREATE A DATETIME STAMP DATAFRAME
# ---------------------------------------------------------------------
update_df = pd.DataFrame([datetime.utcnow()], columns=['Data Update Datetime UTC'])
update_df['Data Update Datetime US/Eastern'] = (
    update_df['Data Update Datetime UTC'][0]
    .replace(tzinfo=timezone('UTC'))
    .astimezone(tz=timezone('US/Eastern'))
)
update_df['Data Update Datetime Mozambique'] = (
    update_df['Data Update Datetime UTC'][0]
    .replace(tzinfo=timezone('UTC'))
    .astimezone(tz=timezone('Africa/Harare'))
)

# ---------------------------------------------------------------------
# 18. WRITE CSV FILES
# ---------------------------------------------------------------------
start_time = datetime.now()

update_df.to_csv('dataUpdateDatetime.csv', index=False)
orgUnitData.to_csv('organisationUnits.csv', index=False)
dataElements.to_csv('dataElements.csv', index=False)
categoryOptionCombos.to_csv('categoryOptionCombos.csv', index=False)
indicators.to_csv('indicators.csv', index=False)
allDataValues.to_csv('dataValues.csv', index=False)

print('Writing CSV files took:', datetime.now() - start_time)

# ---------------------------------------------------------------------
# 19. TOTAL PROCESS RUNTIME
# ---------------------------------------------------------------------
total_elapsed_time = datetime.now() - process_start
print('Total Process Runtime:', total_elapsed_time)
