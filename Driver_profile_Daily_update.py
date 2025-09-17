from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import pygsheets
import MySQLdb
import logging
import socket
import traceback
from sqlalchemy import create_engine
import warnings

# -------------------------
# DAG default args
# -------------------------
default_args = {
    "owner": "Driver-Profile",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

# -------------------------
# Define ETL Task
# -------------------------
def driver_profile_etl(**context):
    warnings.filterwarnings("ignore")
    PROCESS_START_TIME = datetime.now()

    # MySQL connection (Airflow connection id)
    mysql_hook = MySqlHook(mysql_conn_id="mysql_reporting")
    engine = mysql_hook.get_sqlalchemy_engine()
    dbconnect = mysql_hook.get_conn()
    cursor = dbconnect.cursor()

    # -------------------------
    # Step 1: Allocation Data
    # -------------------------
    allocation_data_query = """
    SELECT 
        cl1.car_number,
        cl1.employee_id,
        cl1.Allocation_date,
        IFNULL(cl1.loc_id, map.loc_id) AS loc_id,
        cl1.jama_date,
        map.business_vertical_id,
        cubv.name AS business_vertical,
        cl1.fuel_type,
        map.revenue_type,
        map.team_id,
        map.fleet_team_manager_id,
        map.team
    FROM (
        SELECT
            fc.car_number,
            fd.employee_id,
            cl.car_id,
            DATE(cl.allocation_date) AS Allocation_date,
            IFNULL(DATE(cd.date), CURRENT_DATE()) AS jama_date,
            cl.driver_id,
            cl.id AS allocation_id,
            cd.id AS Jama_id,
            CASE
                WHEN cd.reason IN ('OLD_Rev_share', 'OLD_Leasing') THEN cl.loc_id
            END AS loc_id,
            CASE
                WHEN cd.reason IN ('OLD_Leasing','OLD_Rev_share') THEN 'leasing'
            END AS revenue_type,
            CASE
                WHEN fc.model_id IN (8, 16, 20) THEN 'EV'
                ELSE 'CNG'
            END AS fuel_type 
        FROM car_allocation cl
        LEFT JOIN car_deallocation cd 
            ON cd.id = cl.deallocation_id AND cd.car_id = cl.car_id
        LEFT JOIN fleet_car fc ON fc.id = cl.car_id
        LEFT JOIN fleet_driver fd ON fd.id = cl.driver_id
        WHERE cl.deactivation_date IS NULL 
    ) cl1
    LEFT JOIN (
        SELECT
            fct1.car_id,
            fct1.start_date,
            fct1.end_date,
            fct1.loc_id,
            CASE
                WHEN fct1.loc_id IN (
                    SELECT DISTINCT t.loc_id
                    FROM fleet_team t
                    WHERE t.business_vertical_id = 6
                ) THEN 'DTO'
                WHEN fct1.business_vertical_id = 3 THEN 'ETS'
                WHEN fct1.business_vertical_id = 4 THEN 'Intercity'
                WHEN fct1.revenue_type = 1 THEN 'Rev_share'
                WHEN fct1.revenue_type = 2 THEN 'leasing'
                WHEN team_id = 1009 THEN 'Rev_share'
                WHEN team_id = 19 THEN 'B2B'
                WHEN team_id = 1626 THEN 'man_friday'
                WHEN team_id IN (1303,1470) THEN 'Audit_team'
                WHEN team_id IN (1125,1126,1127,1128,1129,1130,1131) THEN 'New_team'
            END AS revenue_type,
            fct1.fleet_team_manager_id,
            fct1.team_id,
            fct1.team,
            fct1.business_vertical_id
        FROM (
            SELECT
                fct.car_id,
                fct.start_date,
                IFNULL(fct.end_date, CURRENT_DATE()) AS end_date,
                CASE
                    WHEN ft.business_vertical_id IN (3, 4) THEN IFNULL(ft.loc_id, fct.loc_id)
                    WHEN ft.business_vertical_id = 6 AND fcl.city_id = 1 THEN 30
                    WHEN ft.business_vertical_id = 6 AND fcl.city_id = 2 THEN 42
                    WHEN ft.business_vertical_id = 6 AND fcl.city_id = 3 THEN 28
                    WHEN ft.business_vertical_id = 6 AND fcl.city_id = 4 THEN 48
                    WHEN ft.business_vertical_id = 6 AND fcl.city_id = 5 THEN 27
                    WHEN ft.business_vertical_id = 6 AND fcl.city_id = 8 THEN 41
                    ELSE IFNULL(ft.loc_id, fct.loc_id)
                END AS loc_id,
                IFNULL(ft.revenue_type, fct.revenue_type) AS revenue_type,
                ft.business_vertical_id,
                ft.id AS team_id,
                fct.fleet_team_manager_id,
                ft.name AS team
            FROM fleet_car_team fct
            LEFT JOIN fleet_team ft ON ft.id = fct.team_id
            LEFT JOIN fleet_company_loc fcl ON fcl.id = IFNULL(ft.loc_id, fct.loc_id)
        ) fct1
    ) map 
        ON cl1.car_id = map.car_id
        AND cl1.Allocation_date >= map.start_date
        AND cl1.Allocation_date <= map.end_date
    LEFT JOIN common_utils_business_vertical cubv 
        ON cubv.id = map.business_vertical_id;
    """
    df_alloc = pd.read_sql(allocation_data_query, engine)

    # -------------------------
    # Step 2: Transformation (stacking logic)
    # -------------------------
    df = df_alloc.sort_values(by=["employee_id", "Allocation_date"])
    stacks = {}
    date_threshold = timedelta(days=1)

    for _, row in df.iterrows():
        employee_id = row["employee_id"]
        car_number = row["car_number"]
        start_date = row["Allocation_date"]
        end_date = row["jama_date"]
        loc_id = row["loc_id"]
        car_type = row["fuel_type"]
        business_vertical = row["business_vertical"]
        revenue_type = row["revenue_type"]

        if employee_id not in stacks:
            stacks[employee_id] = []

        if not stacks[employee_id]:
            stacks[employee_id].append({
                "start_date": start_date,
                "end_date": end_date,
                "loc_id": loc_id,
                "car_type": car_type,
                "car_number": car_number,
                "business_vertical": business_vertical,
                "revenue_type": revenue_type,
                "end_car": car_number,
                "end_loc_id": loc_id,
                "end_type": car_type
            })
        else:
            last_entry = stacks[employee_id][-1]
            if (start_date - last_entry["end_date"]).days <= date_threshold.days:
                last_entry["end_date"] = max(last_entry["end_date"], end_date)
                last_entry["end_car"] = car_number
                last_entry["end_loc_id"] = loc_id
                last_entry["end_type"] = car_type
                last_entry["business_vertical"] = business_vertical
                last_entry["revenue_type"] = revenue_type
            else:
                stacks[employee_id].append({
                    "start_date": start_date,
                    "end_date": end_date,
                    "loc_id": loc_id,
                    "car_type": car_type,
                    "car_number": car_number,
                    "business_vertical": business_vertical,
                    "revenue_type": revenue_type,
                    "end_car": car_number,
                    "end_loc_id": loc_id,
                    "end_type": car_type
                })

    result = []
    for employee_id, periods in stacks.items():
        for period in periods:
            result.append({
                "employee_id": employee_id,
                "start car": period["car_number"],
                "start loc id": period["loc_id"],
                "start type": period["car_type"],
                "start date": period["start_date"],
                "end car": period["end_car"],
                "end loc id": period["end_loc_id"],
                "end type": period["end_type"],
                "end date": period["end_date"],
                "start business vertical": period["business_vertical"],
                "end business vertical": period["business_vertical"],
                "start revenue_type": period["revenue_type"],
                "end revenue_type": period["revenue_type"]
            })
    final_df = pd.DataFrame(result).dropna(subset=["employee_id"])

    # -------------------------
    # Step 3: City Mapping
    # -------------------------
    sql_city = """
        SELECT
            fleet_company_loc.id AS loc_id,
            fleet_company_loc.location,
            fleet_company_loc.city_id,
            fleet_city.name as city_name
        FROM fleet_company_loc
        LEFT JOIN fleet_city ON fleet_company_loc.city_id=fleet_city.id
    """
    df_city = pd.read_sql(sql_city, engine)
    df_city = df_city.rename(columns={"loc_id":"end loc id","city_id":"City_ID","city_name":"City_name"})
    df_city = df_city[["end loc id","City_ID","City_name"]]

    final_df = pd.merge(final_df, df_city, on="end loc id", how="left")

    # -------------------------
    # Step 4: Write CSV
    # -------------------------
    file_path = "/home/scriptuser/scripts/BI_team_scripts/Recruitment_scripts/Driver_profile.csv"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    final_df.to_csv(file_path, index=False)
    print(f"Driver profile CSV written: {file_path}")


# -------------------------
# DAG Definition
# -------------------------
with DAG(
    dag_id="driver_profile_daily_update",
    default_args=default_args,
    description="Driver Profile Daily Update with transformations",
    schedule_interval="0 6 * * *",  # every day 6 AM
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["driver","profile"],
) as dag:

    task = PythonOperator(
        task_id="driver_profile_etl",
        python_callable=driver_profile_etl,
        provide_context=True,
    )
