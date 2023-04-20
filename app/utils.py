import numpy as np
import pandas as pd

from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from config.settings import DATABASE
from .db import *
from .errors import *

def send_report(is_valid, message):
    if is_valid:
        print("Validation passed. Continuing...")
    else:
        raise Exception(f"Validation failed, {message}")

def validate_input(report_settings):
    if not isinstance(report_settings, dict):
        return False, "report_settings is not a dictionary."

    if not isinstance(report_settings.get("cols_to_check"), list):
        return False, "cols_to_check is not a list."
    for col in report_settings["cols_to_check"]:
        if not isinstance(col, str):
            return False, f"Invalid column name: {col}"

    if not isinstance(report_settings.get("margin_classes"), list):
        return False, "margin_classes is not a list."
    for margin_class in report_settings["margin_classes"]:
        if not isinstance(margin_class, str):
            return False, f"Invalid margin class: {margin_class}"

    if not isinstance(report_settings.get("reports"), list):
        return False, "reports is not a list."
    for report in report_settings["reports"]:
        if not isinstance(report, dict):
            return False, "Report is not a dictionary."
        if not isinstance(report.get("name"), str):
            return False, "Report name is not a string."
        if not isinstance(report.get("table"), str):
            return False, "Report table is not a string."
        if not isinstance(report.get("date"), str):
            return False, "Report date is not a string."
        
        time_of_day = report.get("time_of_day")
        if time_of_day is not None and not isinstance(time_of_day, str):
            return False, "Report time_of_day is not a string or None."
        
        if not isinstance(report.get("valid_report"), bool):
            return False, "Report valid_report is not a boolean."

    return True, "Validation passed."

def create_alchemy_connection(database=DATABASE):
    
    """Creates a connection to a Postgres database based on settings database

    Returns:
        connection
    """
    
    engine_url = f'postgresql://{database["user"]}:{database["password"]}@{database["host"]}:{database["port"]}/{database["name"]}'
    engine = create_engine(engine_url)

    try:
        engine = create_engine(engine_url)
        connection = engine.connect()
        return connection
    except SQLAlchemyError as e:
        raise CustomError(f"SQLAlchemyError connecting to database: {str(e)}")
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        engine.dispose()
        return None
    
def create_dates():
    now = datetime(2020, 5, 12, 0, 0, 0)

    last_day = (now - timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d')
    current_day = now.strftime('%Y-%m-%d')
    max_time_of_day = (now - timedelta(hours=1)).replace(hour=19, minute=0, second=0, microsecond=0).strftime("%H:%M:%S")
    min_time_of_day = (now - timedelta(hours=1)).replace(hour=8, minute=0, second=0, microsecond=0).strftime("%H:%M:%S")

    return {"last_day": last_day, 
            "current_day": current_day, 
            "max_time_of_day": max_time_of_day, 
            "min_time_of_day": min_time_of_day}
    
def query_generator(table, margin, date, time_of_day=None):
    """Generates a query based on the report settings setup

    Args:
        table (sting): table name
        margin (string): type of margin
        date (string): date of report
        time_of_day (string, optional): time of the. Defaults to None.

    Returns:
        string: SQL select statement
    """
    
    query = (f"SELECT * "
             f"FROM {table} "
             f"WHERE margin_type = '{margin}' "
             f"AND date = '{date}';")
    
    if time_of_day is not None:
        query = query[:-1] + f" AND time_of_day = '{time_of_day}';"
    
    return query

def get_margins(report_name, table, margin, date, time_of_day=None, database=DATABASE):
    """creates a connection to the database and executes a SQL select statement
       based on the report settings setup

    Args:
        report_name (string): which report should be queried
        table (sting): table name
        margin (string): type of margin
        date (string): date of report
        time_of_day (string, optional): time of the. Defaults to None.
        database (dict, optional): dictionary with the database connection setup. Defaults to DATABASE
        
    Raises:
        Exception: if connection to the database cannot be established

    Returns:
        DataFrame: items returned from the database
    """
    
    connection = None
    
    try:    
        connection = create_alchemy_connection(database)
    
        if connection is None:
            raise CustomError("Failed to establish database connection")
        
        query = query_generator(table, margin, date, time_of_day)
        df = pd.read_sql_query(query, connection)
               
        df_name = f"{report_name}_{margin}"
        df.name = df_name
        
        return df
    
    except CustomError as e:
        print(str(e))
        return None
    except Exception as e:
        print(f"Error getting margins: {str(e)}")
        return None
    
    finally:
        if connection is not None:
            connection.close()

def fetch_reports(report_config):
    """runs through the report configuration dict and queries for each margin class
       and report type the items accordingly

    Args:
        report_config (dict): report configuration setup

    Raises:
        Exception: if a query cannot be executed successfully

    Returns:
        dict: a nested dictionary with margins and reported dataframes as keys 
    """
    reports = {}
    
    try:
        for margin in report_config['margin_classes']:
            reports[margin] = {}
            for report in report_config['reports']:
                report_name = report['name']
                table = report['table']
                date = report['date']
                time_of_day = report.get('time_of_day', None)
                
                df = get_margins(report_name, table, margin, date, time_of_day)
                
                if df is None:
                    raise Exception(f"Error fetching report '{report_name}' for margin '{margin}'")
                
                reports[margin][report_name] = df
            
        return reports
    
    except Exception as e:
        print(f"Error fetching reports: {str(e)}")
        return None

def process_reports(df1, df2, columns):
    """Takes two Pandas DataFrames and merges them based on the columns specified

    Args:
        df1 (DataFrame): dataframe which should be compared
        df2 (DataFrame): dataframe which should be compared
        columns (list): the columns which should be matched against

    Returns:
        list: two Dataframes, first which items got matched, second which did not
    """
    
    try:
        merged = pd.merge(df1,
                          df2, 
                          on=columns,
                          how='outer',
                          indicator=True)
        
        matching = merged[merged['_merge'] == 'both']
        non_matching = merged[merged['_merge'] != 'both']
        
        non_matching["source"] = np.nan
        non_matching.loc[non_matching["_merge"] == "left_only", "source"] = f"found in {df1.name}"
        non_matching.loc[non_matching["_merge"] == "right_only", "source"] = f"found in {df2.name}"
        
        non_matching['is_duplicate'] = non_matching.duplicated(subset=['clearing_member', 'account', 'margin_type', 'margin'])
        
        return [matching[['clearing_member', 'account', 'margin_type', 'margin']], 
                non_matching[['clearing_member', 'account', 'margin_type', 'margin', 'source']]]
    except Exception as e:
        print(f"Error processing reports: {str(e)}")
        return None

def check_report(df1, df2, columns):
    """Takes two Pandas DataFrames and sends out reports based on the subsequent
    requirements

    Args:
        df1 (DataFrame): dataframe which should be compared
        df2 (DataFrame): dataframe which should be compared
        columns (list): the columns which should be matched against

    """
    
    try:
        match, non_matching = process_reports(df1, df2, columns)
        
        if non_matching.empty == True:
            print("nothing to report")
            return True
        
        # Send report with logic
        # duplicateded
        # only in cc050
        # only in ci050
        
        print(f"need to report for {df1.name} and {df2.name}")
        
        # Send report via PMA
        print(non_matching)
        
    except Exception as e:
        print(f"Error checking report: {str(e)}")
        return None

# Utility function only
def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 4 * (level + 1)
        for file in files:
            print(f"{subindent}{file}")