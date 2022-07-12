import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts
from initial_values.initial_values import sap_columns_to_master_columns
from datetime import datetime
# from app import app
import sqlite3

db = extensions.db

def sql_to_eo_calendar_master():
  """
  Чтение из базы данных из таблицы ео и eo_calendar_operation_status_DB \n
  приведение полей дат в дату и сохранение в эксель downloads/calendar_eo.xlsx
  """
  con = sqlite3.connect("database/datab.db")
  # sql = "SELECT * FROM eo_DB JOIN be_DB"
  sql = "SELECT \
  eo_DB.be_code, \
  be_DB.be_description, \
  eo_DB.eo_class_code, \
  eo_class_DB.eo_class_description, \
  models_DB.eo_model_name, \
  models_DB.eo_category_spec, \
  eo_DB.eo_model_id, \
  eo_DB.sap_model_name, \
  eo_DB.sap_maker, \
  eo_DB.teh_mesto, \
  eo_DB.gar_no, \
  eo_DB.sap_gar_no, \
  eo_DB.eo_code, \
  eo_DB.eo_description, \
  eo_DB.head_type, \
  eo_DB.operation_start_date, \
  eo_DB.reported_operation_start_date, \
  eo_DB.expected_operation_period_years, \
  eo_DB.expected_operation_finish_date, \
  eo_DB.sap_planned_finish_operation_date, \
  eo_DB.expected_operation_status_code, \
  eo_DB.sap_system_status, \
  eo_DB.sap_user_status, \
  eo_DB.reported_operation_finish_date, \
  eo_DB.reported_operation_status, \
  eo_DB.reported_operation_status_date, \
  eo_DB.evaluated_operation_finish_date, \
  eo_calendar_operation_status_DB.year_2022_qty, \
  eo_calendar_operation_status_DB.year_2022_age, \
  eo_calendar_operation_status_DB.year_2022_in, \
  eo_calendar_operation_status_DB.year_2022_out, \
  eo_calendar_operation_status_DB.year_2023_qty, \
  eo_calendar_operation_status_DB.year_2023_age, \
  eo_calendar_operation_status_DB.year_2023_in, \
  eo_calendar_operation_status_DB.year_2023_out, \
  eo_calendar_operation_status_DB.year_2024_qty, \
  eo_calendar_operation_status_DB.year_2024_age, \
  eo_calendar_operation_status_DB.year_2024_in, \
  eo_calendar_operation_status_DB.year_2024_out, \
  eo_calendar_operation_status_DB.year_2025_qty, \
  eo_calendar_operation_status_DB.year_2025_age, \
  eo_calendar_operation_status_DB.year_2025_in, \
  eo_calendar_operation_status_DB.year_2025_out, \
  eo_calendar_operation_status_DB.year_2026_qty, \
  eo_calendar_operation_status_DB.year_2026_age, \
  eo_calendar_operation_status_DB.year_2026_in, \
  eo_calendar_operation_status_DB.year_2026_out, \
  eo_calendar_operation_status_DB.year_2027_qty, \
  eo_calendar_operation_status_DB.year_2027_age, \
  eo_calendar_operation_status_DB.year_2027_in, \
  eo_calendar_operation_status_DB.year_2027_out, \
  eo_calendar_operation_status_DB.year_2028_qty, \
  eo_calendar_operation_status_DB.year_2028_age, \
  eo_calendar_operation_status_DB.year_2028_in, \
  eo_calendar_operation_status_DB.year_2028_out, \
  eo_calendar_operation_status_DB.year_2029_qty, \
  eo_calendar_operation_status_DB.year_2029_age, \
  eo_calendar_operation_status_DB.year_2029_in, \
  eo_calendar_operation_status_DB.year_2029_out, \
  eo_calendar_operation_status_DB.year_2030_qty, \
  eo_calendar_operation_status_DB.year_2030_age, \
  eo_calendar_operation_status_DB.year_2030_in, \
  eo_calendar_operation_status_DB.year_2030_out, \
  eo_calendar_operation_status_DB.year_2031_qty, \
  eo_calendar_operation_status_DB.year_2031_age, \
  eo_calendar_operation_status_DB.year_2031_in, \
  eo_calendar_operation_status_DB.year_2031_out \
  FROM eo_DB \
  LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
  LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
  LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
  LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code \
  LEFT JOIN eo_calendar_operation_status_DB ON eo_DB.eo_code = eo_calendar_operation_status_DB.eo_code"



  excel_master_eo_df = pd.read_sql_query(sql, con)
  excel_master_eo_df.sort_values(['be_code','teh_mesto'], inplace=True)
  date_time_plug = '31/12/2099 23:59:59'
  date_time_plug = datetime.strptime(date_time_plug, '%d/%m/%Y %H:%M:%S')
  # excel_master_eo_df.to_csv('temp_data/excel_master_eo_df.csv')
  
  excel_master_eo_df['expected_operation_finish_date'] = pd.to_datetime(excel_master_eo_df['expected_operation_finish_date'], errors = 'coerce')

  excel_master_eo_df['evaluated_operation_finish_date'] = pd.to_datetime(excel_master_eo_df['evaluated_operation_finish_date'])
  
  excel_master_eo_df['operation_start_date'] = pd.to_datetime(excel_master_eo_df['operation_start_date'])
  excel_master_eo_df['reported_operation_start_date'] = pd.to_datetime(excel_master_eo_df['reported_operation_start_date'])
  
  
  
  excel_master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(excel_master_eo_df['sap_planned_finish_operation_date'])
  
  excel_master_eo_df['reported_operation_finish_date'] = pd.to_datetime(excel_master_eo_df['reported_operation_finish_date'])
  
  # excel_master_eo_df_subset = excel_master_eo_df.loc[excel_master_eo_df['expected_operation_finish_date'] == date_time_plug]
  # indexes = excel_master_eo_df_subset.index.values
  # excel_master_eo_df.loc[indexes, ['expected_operation_finish_date']] = ""
  
  # excel_master_eo_df_2_subset = excel_master_eo_df.loc[excel_master_eo_df['operation_start_date'] == date_time_plug]
  # indexes_2 = excel_master_eo_df_2_subset.index.values
  # excel_master_eo_df.loc[indexes_2, ['operation_start_date']] = ""

  excel_master_eo_df["operation_start_date"] = excel_master_eo_df["operation_start_date"].dt.strftime("%d.%m.%Y")

  excel_master_eo_df["reported_operation_start_date"] = excel_master_eo_df["reported_operation_start_date"].dt.strftime("%d.%m.%Y")
  
  excel_master_eo_df["expected_operation_finish_date"] = excel_master_eo_df["expected_operation_finish_date"].dt.strftime("%d.%m.%Y")

  excel_master_eo_df["evaluated_operation_finish_date"] = excel_master_eo_df["evaluated_operation_finish_date"].dt.strftime("%d.%m.%Y")
  
  
  excel_master_eo_df["sap_planned_finish_operation_date"] = excel_master_eo_df["sap_planned_finish_operation_date"].dt.strftime("%d.%m.%Y")
  
  # excel_master_eo_df["expected_operation_status_code_date"] = excel_master_eo_df["expected_operation_status_code_date"].dt.strftime("%d.%m.%Y")

  excel_master_eo_df["reported_operation_finish_date"] = excel_master_eo_df["reported_operation_finish_date"].dt.strftime("%d.%m.%Y")

  
  # excel_master_eo_df["reported_operation_status_date"] = excel_master_eo_df["reported_operation_status_date"].dt.strftime("%d.%m.%Y")


  excel_master_eo_df.to_excel('downloads/calendar_eo.xlsx', index = False)  


