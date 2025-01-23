import os
import re
import json
import requests
import pandas as pd
import numpy as np
from requests.auth import HTTPBasicAuth
from shutil import copyfile
from pytz import timezone
from datetime import datetime
from typing import List, Optional

# --------------------------------------------------
# Global Constants / Configuration
# --------------------------------------------------

DHIS2_AUTH = ('svcECHO', '1WantToSeeMyD@t@!')
BASE_URL = "https://dhis2.echomoz.org/api/29"

# Adjust this path as needed
#MAIN_DIR = r"C:\Projects\ECHO\Data"
main_dir = r"\\ad.abt.local\Projects\Projects\ECHO\Data"

# Files to back up
FILES_TO_BACKUP = [
    'dataUpdateDatetime.csv',
    'organisationUnits.csv',
    'dataElements.csv',
    'categoryOptionCombos.csv',
    'indicators.csv',
    'dataValues.csv',
]

# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def get_current_time() -> datetime:
    """Return the current local time."""
    return datetime.now()

def change_directory(path: str) -> None:
    """
    Change the working directory to `path` if it exists.
    Print the outcome.
    """
    if os.path.exists(path):
        os.chdir(path)
        print(f"Changed working directory to: {os.getcwd()}")
    else:
        print(f"Directory not found: {path}")

def read_csv_file(file_path: str) -> Optional[pd.DataFrame]:
    """
    Attempt to read a CSV file into a pandas DataFrame.
    Return None if the file does not exist or any error occurs.
    """
    if not os.path.exists(file_path):
        print(f"File does not exist, skipping: {file_path}")
        return None

    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def create_backup_directory(backup_dir: str) -> None:
    """
    Create a backup directory if it doesn’t already exist.
    """
    try:
        os.mkdir(backup_dir)
        print(f"Directory created: {backup_dir}")
    except FileExistsError:
        print(f"Directory already exists, skipping creation: {backup_dir}")
    except Exception as e:
        print(f"Directory not created: {e.__class__} occurred.")

def backup_files(file_list: List[str], source_dir: str, backup_dir: str) -> None:
    """
    Copy specified files from source_dir to backup_dir.
    """
    for filename in file_list:
        src = os.path.join(source_dir, filename)
        dst = os.path.join(backup_dir, filename)
        if not os.path.exists(src):
            print(f"Source file does not exist, skipping: {src}")
            continue
        try:
            copyfile(src, dst)
            print(f"{filename} backed up to {backup_dir}")
        except Exception as e:
            print(f"{filename} not backed up: {e.__class__} occurred.")

def dhis2_get_json(endpoint: str, params: dict = None) -> dict:
    """
    Helper to get JSON data from DHIS2 at BASE_URL + endpoint.
    Raises an HTTPError if the request fails.
    """
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, auth=DHIS2_AUTH, params=params)
    response.raise_for_status()
    return response.json()

def create_period_lists() -> (List[str], List[str], List[str]):
    """
    Create and return:
     - A list of monthly periods to query
     - A list of quarterly periods to query
     - A scaffold list of all monthly + quarterly periods for targets
    """
    now = datetime.utcnow()
    months = [f"{m:02d}" for m in range(1, 13)]  # '01'..'12'
    years = list(range(2019, now.year + 1))
    str_years = [str(y) for y in years]

    # Monthly
    initial_month_list = [y + m for y in str_years for m in months]
    first_month = "201909"  # Start of ECHO Dashboards
    current_month = f"{now.year}{now.month:02d}"
    period_list = [x for x in initial_month_list if first_month <= x < current_month]

    # Quarterly
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    initial_quarter_list = [y + q for y in str_years for q in quarters]
    first_quarter = "2019Q4"  # Start of ECHO Dashboards
    # Derive current quarter
    current_quarter_number = (now.month - 1) // 3 + 1
    current_quarter = f"{now.year}Q{current_quarter_number}"
    quarter_list = [x for x in initial_quarter_list if first_quarter <= x < current_quarter]

    # Scaffold for targets: monthly + quarterly from 2020 onward
    scaffold_periods = [p for p in (initial_month_list + initial_quarter_list) if p >= "2020"]

    return period_list, quarter_list, scaffold_periods

def write_csv(df: pd.DataFrame, filename: str) -> None:
    """Convenience function to write a DataFrame to CSV."""
    try:
        df.to_csv(filename, index=False)
        print(f"Wrote {filename} successfully.")
    except Exception as e:
        print(f"Error writing {filename}: {e}")

# --------------------------------------------------
# Main Execution Flow
# --------------------------------------------------

def main():
    # Record overall start time
    process_start = get_current_time()

    # 1. Ensure we have a working directory
    change_directory(MAIN_DIR)

    # 2. Possibly read in the last update datetime to get backupDate
    backup_date = ""
    data_update_df = read_csv_file("dataUpdateDatetime.csv")
    if data_update_df is not None:
        try:
            # The example code picks out column index = 2 (3rd column).
            backup_date = str(data_update_df.iloc[0, 2])[:10]
            print(f"Backup date found: {backup_date}")
        except Exception as e:
            print(f"Error extracting backup date: {e}")

    # 3. Create backup directory, if applicable, and back up old data
    if backup_date:
        backup_dir = os.path.join(MAIN_DIR, "Backup", backup_date)
        create_backup_directory(backup_dir)
        backup_files(FILES_TO_BACKUP, MAIN_DIR, backup_dir)

    # 4. Retrieve Organisation Unit Groups & Identify ECHO Sites
    org_unit_groups = dhis2_get_json("organisationUnitGroups")['organisationUnitGroups']
    org_unit_groups_df = pd.DataFrame(org_unit_groups)

    echo_group_id = org_unit_groups_df.loc[
        org_unit_groups_df['displayName'] == 'ECHO Sites', 'id'
    ].tolist()[0]
    
    # Retrieve the org units for that group
    echo_org_units = dhis2_get_json(f"organisationUnitGroups/{echo_group_id}")['organisationUnits']
    echo_org_units_df = pd.DataFrame(echo_org_units)
    echo_org_unit_ids = echo_org_units_df['id'].tolist()

    # 5. Get reference data for all org units in DHIS2
    org_units = dhis2_get_json("organisationUnits?paging=false&fields=id,code,displayName,path")['organisationUnits']
    org_units_df = pd.DataFrame(org_units)

    # 6. Subset to ECHO org units and parse out path
    org_unit_data = org_units_df.loc[org_units_df['id'].isin(echo_org_unit_ids)].copy()
    path_data = org_unit_data['path'].str.split('/', expand=True)
    org_unit_data["province"] = path_data[2]
    org_unit_data["district"] = path_data[3]
    org_unit_data.rename(columns={"displayName": "health facility"}, inplace=True)
    org_unit_data.drop(columns=["path"], inplace=True)

    # 7. Replace the province/district IDs with their names
    org_unit_data[["province", "district"]] = org_unit_data[["province", "district"]].replace(
        org_units_df["id"].to_list(),
        org_units_df["displayName"].to_list()
    )

    # 8. Get geocoordinates for ECHO org units
    geo_features = dhis2_get_json(f"geoFeatures?ou=ou:OU_GROUP-{echo_group_id}")
    echo_geo_df = pd.DataFrame(geo_features)[["id", "na", "co"]].copy()
    echo_geo_df["longitude"] = echo_geo_df["co"].str.split(",", expand=True)[0].str.strip("[")
    echo_geo_df["latitude"] = echo_geo_df["co"].str.split(",", expand=True)[1].str.strip("]")
    echo_geo_df = echo_geo_df[["id", "latitude", "longitude"]]

    # 9. Merge geo info
    org_unit_data = org_unit_data.merge(echo_geo_df, how='left', on='id')

    # 10. Reorder columns
    cols = ["id", "code", "province", "district", "health facility", "latitude", "longitude"]
    org_unit_data = org_unit_data[cols]

    # 11. Get Data Element Group Sets (for ECHO export)
    degs = dhis2_get_json("dataElementGroupSets?paging=false")['dataElementGroupSets']
    degs_df = pd.DataFrame(degs)
    echo_export_id = degs_df.loc[degs_df["displayName"] == "ECHO EXPORT", "id"].tolist()[0]

    # 12. Identify the Data Element Groups that are part of that set
    degs_export = dhis2_get_json(f"dataElementGroupSets/{echo_export_id}")['dataElementGroups']
    export_degs_df = pd.DataFrame(degs_export)

    # 13. Get reference info on all data element groups
    de_groups_json = dhis2_get_json("dataElementGroups?paging=false")['dataElementGroups']
    data_element_groups_df = pd.DataFrame(de_groups_json)

    # 14. Identify the data element group for ECHO Targets
    target_deg_id = data_element_groups_df.loc[
        data_element_groups_df['displayName'] == 'ECHO MOZ | Targets', 'id'
    ].tolist()[0]

    # 15. Get reference info on all data elements
    data_elements_json = dhis2_get_json(
        "dataElements?fields=id,displayName,displayShortName,dataElementGroups&paging=false"
    )['dataElements']
    data_elements_df = pd.DataFrame(data_elements_json)

    # Convert the dataElementGroups from a list of dicts -> semicolon-delimited string
    def convert_deg_list(row):
        return ";".join(entry["id"] for entry in row)

    data_elements_df["dataElementGroups"] = data_elements_df["dataElementGroups"].apply(convert_deg_list)
    
    # Replace the data element group IDs with names
    data_elements_df[["dataElementGroups"]] = data_elements_df[["dataElementGroups"]].replace(
        data_element_groups_df["id"].to_list(),
        data_element_groups_df["displayName"].to_list(),
        regex=True
    )

    # 16. Get Category Option Combos
    coc_json = dhis2_get_json("categoryOptionCombos?paging=false")['categoryOptionCombos']
    coc_df = pd.DataFrame(coc_json)

    # 17. Get Indicator Group Sets
    igs_json = dhis2_get_json("indicatorGroupSets?paging=false")['indicatorGroupSets']
    igs_df = pd.DataFrame(igs_json)

    # Make a dataframe of all indicator group sets that contain the word "export"
    export_indicator_group_sets_df = igs_df.loc[
        igs_df['displayName'].str.contains("export", flags=re.IGNORECASE),
        ["id", "displayName"]
    ]
    export_indicator_group_sets_df["type"] = "indicator"

    # 18. Expand the IDs to a list, retrieve all indicator groups
    echo_export_igs_ids = export_indicator_group_sets_df["id"].tolist()

    export_indicator_groups_df = pd.DataFrame()
    for item in echo_export_igs_ids:
        igs_details = dhis2_get_json(f"indicatorGroupSets/{item}")['indicatorGroups']
        igs_details_df = pd.DataFrame(igs_details)
        export_indicator_groups_df = pd.concat([export_indicator_groups_df, igs_details_df], ignore_index=True)

    # Get reference info on all indicator groups
    indicator_groups_json = dhis2_get_json("indicatorGroups?paging=false")['indicatorGroups']
    indicator_groups_df = pd.DataFrame(indicator_groups_json)

    # Identify the TX_CURR export group
    tx_curr_group_id = indicator_groups_df[indicator_groups_df['displayName'].str.contains('EXPORT TX_CURR')]["id"].tolist()[0]

    # 19. Get reference info on all indicators
    indicators_json = dhis2_get_json(
        "indicators.json?fields=id,displayName,displayShortName,numerator,denominator,indicatorGroups&paging=false"
    )['indicators']
    indicators_df = pd.DataFrame(indicators_json)
    indicators_df.set_index("id", inplace=True)

    # Convert the indicator group dictionaries to semicolon-delimited
    def convert_indicator_groups(row):
        return ";".join(entry["id"] for entry in row)

    indicators_df["indicatorGroups"] = indicators_df["indicatorGroups"].apply(convert_indicator_groups)

    # Replace the indicator group IDs with names
    indicators_df["indicatorGroups"] = indicators_df["indicatorGroups"].replace(
        indicator_groups_df["id"].to_list(),
        indicator_groups_df["displayName"].to_list(),
        regex=True
    )

    indicators_df.reset_index(inplace=True)

    # Replace the IDs in numerator & denominator with data element names
    start_time = get_current_time()
    indicators_df[["numerator", "denominator"]] = indicators_df[["numerator", "denominator"]].replace(
        ["#"], [""], regex=True
    )  # remove the # character
    indicators_df[["numerator", "denominator"]] = indicators_df[["numerator", "denominator"]].replace(
        ["\\."], [", "], regex=True
    )  # replace period with comma+space
    indicators_df[["numerator", "denominator"]] = indicators_df[["numerator", "denominator"]].replace(
        data_elements_df["id"].to_list(),
        data_elements_df["displayName"].to_list(),
        regex=True
    )
    indicators_df[["numerator", "denominator"]] = indicators_df[["numerator", "denominator"]].replace(
        coc_df["id"].to_list(),
        coc_df["displayName"].to_list(),
        regex=True
    )
    elapsed = get_current_time() - start_time
    print(f"Updating indicator formulas: {elapsed}")

    # 20. Generate needed period lists
    monthly_periods, quarterly_periods, scaffold_periods = create_period_lists()

    # 21. Retrieve indicator data (monthly first, then quarterly for those with no results)
    all_indicator_values = pd.DataFrame()
    export_indicator_groups_df["results"] = 0

    def fetch_indicator_data(period_list: List[str], group_list: List[str], freq_label: str) -> pd.DataFrame:
        """
        Fetches analytics data for a list of periods and a list of indicator groups.
        Returns a concatenated DataFrame of results.
        """
        result_df = pd.DataFrame()
        for period in period_list:
            print(f"Retrieving Period ({freq_label}): {period}")
            period_start = get_current_time()
            for grp in group_list:
                endpoint = f"analytics?dimension=pe:{period}&dimension=dx:IN_GROUP-{grp}&dimension=ou:OU_GROUP-{echo_group_id}"
                resp_json = requests.get(f"{BASE_URL}/{endpoint}", auth=DHIS2_AUTH)
                if resp_json.status_code == 200 and resp_json.text.strip() != "{}":
                    rows = resp_json.json().get("rows", [])
                    df_temp = pd.DataFrame(rows)
                    result_df = pd.concat([result_df, df_temp], ignore_index=True)
                    # track row counts
                    export_indicator_groups_df.loc[export_indicator_groups_df['id'] == grp, 'results'] += len(df_temp.index)
            print(f"Elapsed for {period}: {get_current_time() - period_start}")
        return result_df

    data_retrieval_start = get_current_time()
    monthly_df = fetch_indicator_data(monthly_periods, export_indicator_groups_df["id"].tolist(), "monthly")
    # Identify group(s) that retrieved no monthly results
    no_monthly_results = export_indicator_groups_df.loc[export_indicator_groups_df["results"] == 0, "id"].tolist()

    # For any group with no monthly results, try quarterly
    if no_monthly_results:
        quarterly_df = fetch_indicator_data(quarterly_periods, no_monthly_results, "quarterly")
        # Combine monthly & quarterly
        if not quarterly_df.empty:
            monthly_df = pd.concat([monthly_df, quarterly_df], ignore_index=True)
    data_retrieval_elapsed = get_current_time() - data_retrieval_start
    print(f"Indicator Retrieval (Monthly & Quarterly): {data_retrieval_elapsed}")

    # Convert columns if not empty
    if not monthly_df.empty:
        # Last API response's headers structure might be used
        # We do a minimal check; if no data, skip
        last_resp = requests.get(
            f"{BASE_URL}/analytics?dimension=pe:{monthly_periods[-1]}&dimension=dx:IN_GROUP-{export_indicator_groups_df['id'].iloc[0]}&dimension=ou:OU_GROUP-{echo_group_id}",
            auth=DHIS2_AUTH
        )
        if last_resp.status_code == 200 and "headers" in last_resp.json():
            indicator_headers = pd.DataFrame(last_resp.json()["headers"])
            monthly_df.columns = indicator_headers["column"]

    all_indicator_values = monthly_df

    # 22. Retrieve data elements on a monthly basis
    all_data_element_values = pd.DataFrame()
    export_degs_df["results"] = 0

    if not export_degs_df.empty:
        data_retrieval_start = get_current_time()
        for period in monthly_periods:
            print(f"Retrieving Data Elements for Period: {period}")
            period_start = get_current_time()
            for deg_id in export_degs_df["id"].tolist():
                endpoint = (
                    f"analytics?dimension=pe:{period}"
                    f"&dimension=dx:DE_GROUP-{deg_id}"
                    f"&dimension=co"
                    f"&dimension=ou:OU_GROUP-{echo_group_id}"
                )
                resp = requests.get(f"{BASE_URL}/{endpoint}", auth=DHIS2_AUTH)
                if resp.status_code == 200 and resp.text.strip() != "{}":
                    rows = resp.json().get("rows", [])
                    temp_df = pd.DataFrame(rows)
                    all_data_element_values = pd.concat([all_data_element_values, temp_df], ignore_index=True)
                    export_degs_df.loc[export_degs_df["id"] == deg_id, "results"] += len(temp_df.index)
            print(f"Elapsed for {period}: {get_current_time() - period_start}")

        # Attempt column rename
        if not all_data_element_values.empty:
            headers_json = resp.json().get("headers", [])
            headers_df = pd.DataFrame(headers_json)
            all_data_element_values.columns = headers_df["column"]

        data_retrieval_elapsed = get_current_time() - data_retrieval_start
        print(f"Data Element Retrieval (Monthly): {data_retrieval_elapsed}")

    # 23. Identify data element groups with no monthly results (not used further in the script, but kept for reference)
    no_monthly_de_results = list(export_degs_df.loc[export_degs_df["results"] == 0, "id"])
    print(f"No monthly DE results for: {no_monthly_de_results}")

    # 24. Query targets on an annual basis
    # Targets for ECHO
    target_data_values = pd.DataFrame()
    str_years = [str(y) for y in range(2019, get_current_time().year + 1)]
    data_retrieval_start = get_current_time()

    for year_str in str_years:
        print(f"Retrieving Targets for Year: {year_str}")
        endpoint = (
            f"analytics?dimension=pe:{year_str}"
            f"&dimension=dx:DE_GROUP-{target_deg_id}"
            f"&dimension=co&dimension=ou:OU_GROUP-{echo_group_id}"
        )
        resp = requests.get(f"{BASE_URL}/{endpoint}", auth=DHIS2_AUTH)
        if resp.status_code == 200 and resp.text.strip() != "{}":
            rows = resp.json().get("rows", [])
            tmp_df = pd.DataFrame(rows)
            target_data_values = pd.concat([target_data_values, tmp_df], ignore_index=True)

    # Convert columns if we have any data
    if not target_data_values.empty:
        headers_df = pd.DataFrame(resp.json()["headers"])
        target_data_values.columns = headers_df["column"]

    data_retrieval_elapsed = get_current_time() - data_retrieval_start
    print(f"Data Element Retrieval (Annual Targets): {data_retrieval_elapsed}")

    # 25. Handle special divisions for monthly vs. quarterly vs. annual targets
    tx_curr_targets = data_elements_df.loc[
        data_elements_df['displayName'].str.contains(r'target.*_curr', flags=re.IGNORECASE, regex=True),
        'id'
    ].tolist()
    tx_curr_targets_df = pd.DataFrame(tx_curr_targets, columns=["Data"])
    tx_curr_targets_df["Divisor"] = 1
    tx_curr_targets_df["type"] = 'M'  # monthly

    # TX_PVLS -> quarterly
    tx_pvls_targets = data_elements_df.loc[
        data_elements_df['displayName'].str.contains(r'target.*tx_pvls', flags=re.IGNORECASE, regex=True),
        'id'
    ].tolist()
    tx_pvls_targets_df = pd.DataFrame(tx_pvls_targets, columns=["Data"])
    tx_pvls_targets_df["Divisor"] = 4
    tx_pvls_targets_df["type"] = 'Q'  # quarterly

    special_targets_df = pd.concat([tx_curr_targets_df, tx_pvls_targets_df], ignore_index=True)

    if not target_data_values.empty:
        # Merge to add Divisor info
        target_data_values = target_data_values.merge(special_targets_df, how="left", on="Data")
        target_data_values["Divisor"] = target_data_values["Divisor"].fillna(12)
        target_data_values["type"] = target_data_values["type"].fillna('M')

        # Divide annual value
        target_data_values["Value"] = (
            target_data_values["Value"].astype(float) / target_data_values["Divisor"]
        ).astype(object)
        target_data_values.drop(columns=["Divisor"], inplace=True)

        # Merge with a period scaffold (monthly/quarterly) from 2020 onward
        # to replicate annual target across all sub-periods
        periods_df = pd.DataFrame(scaffold_periods, columns=["period"])
        periods_df["year"] = periods_df["period"].str[:4]
        periods_df["type"] = np.where(periods_df["period"].str[4:] == "Q", "Q", "M")

        target_data_values = target_data_values.merge(
            periods_df,
            left_on=["Period", "type"],
            right_on=["year", "type"]
        )
        target_data_values["Period"] = target_data_values["period"]
        target_data_values.drop(columns=["period", "year"], inplace=True)

    # 26. Combine indicator values and data element values
    if not all_data_element_values.empty:
        # If data elements exist, we have an extra 'Category option combo' column
        # Ensure consistent column naming
        all_data_element_values["Category option combo"] = all_data_element_values.get("Category option combo", np.nan)
        combined_data = pd.concat([all_indicator_values, all_data_element_values], ignore_index=True, sort=False)
    else:
        combined_data = all_indicator_values
        # For consistency, ensure the column is present
        if "Category option combo" not in combined_data.columns:
            combined_data["Category option combo"] = np.nan

    # 27. Merge in targets
    if not target_data_values.empty:
        combined_data = pd.concat([combined_data, target_data_values], ignore_index=True, sort=False)

    # 28. Remove duplicates
    combined_data.drop_duplicates(inplace=True)

    # 29. Prepare final timestamp info
    update_datetime_utc = datetime.utcnow()
    update_data = pd.DataFrame([update_datetime_utc], columns=["Data Update Datetime UTC"])
    update_data["Data Update Datetime US/Eastern"] = (
        update_data["Data Update Datetime UTC"][0]
        .replace(tzinfo=timezone("UTC"))
        .astimezone(tz=timezone("US/Eastern"))
    )
    update_data["Data Update Datetime Mozambique"] = (
        update_data["Data Update Datetime UTC"][0]
        .replace(tzinfo=timezone("UTC"))
        .astimezone(tz=timezone("Africa/Harare"))
    )

    # 30. Write final outputs
    start_time = get_current_time()
    write_csv(update_data, "dataUpdateDatetime.csv")
    write_csv(org_unit_data, "organisationUnits.csv")
    write_csv(data_elements_df, "dataElements.csv")
    write_csv(coc_df, "categoryOptionCombos.csv")
    write_csv(indicators_df, "indicators.csv")
    write_csv(combined_data, "dataValues.csv")
    elapsed = get_current_time() - start_time
    print(f"Writing CSV files took: {elapsed}")

    # 31. Print total process time
    total_elapsed_time = get_current_time() - process_start
    print(f"Total Process Runtime: {total_elapsed_time}")

# Standard “entry point” check
if __name__ == "__main__":
    main()
