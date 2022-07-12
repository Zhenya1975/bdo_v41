import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts
from initial_values.initial_values import sap_columns_to_master_columns
from datetime import datetime
# from app import app
import sqlite3

db = extensions.db


def read_date(date_input, eo_code):  
  # print(type(date_input), eo_code)
  if "timestamp" in str(type(date_input)) or 'datetime' in str(type(date_input)):
    date_output = date_input
    return date_output
  elif "str" in str(type(date_input)):
    try:
      date_output = datetime.strptime(date_input, '%d.%m.%Y')
      return date_output
    except:
      pass
    try:
      date_output = datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S')
      return date_output
    except:
      pass  
    try:
      date_output = datetime.strptime(date_input, '%Y-%m-%d')
      return date_output
    except Exception as e:
      print(f"eo_code: {eo_code}. Не удалось сохранить в дату '{date_input}, тип: {type(date_input)}'. Ошибка: ", e)
      date_output = datetime.strptime('1.1.2199', '%d.%m.%Y')
      return date_output
  
  elif "nat" in str(type(date_input)) or "NaT" in str(type(date_input)) or "float" in str(type(date_input)):
    date_output = datetime.strptime('1.1.2199', '%d.%m.%Y')
    return date_output
  else:
    print(eo_code, "не покрыто типами данных дат", type(date_input), date_input)
    date_output = datetime.strptime('1.1.2199', '%d.%m.%Y')
    return date_output


def sql_to_eo_master():
  con = sqlite3.connect("database/datab.db")
  # sql = "SELECT * FROM eo_DB JOIN be_DB"
  sql = "SELECT \
  eo_DB.be_code, \
  be_DB.be_description, \
  eo_DB.eo_class_code, \
  eo_class_DB.eo_class_description, \
  models_DB.eo_model_name, \
  eo_DB.teh_mesto, \
  eo_DB.gar_no, \
  eo_DB.eo_code, \
  eo_DB.head_type, \
  eo_DB.operation_start_date, \
  eo_DB.expected_operation_period_years, \
  eo_DB.expected_operation_finish_date, \
  eo_DB.sap_planned_finish_operation_date, \
  eo_DB.expected_operation_status_code, \
  eo_DB.expected_operation_status_code_date, \
  eo_DB.sap_system_status, \
  eo_DB.sap_user_status, \
  eo_DB.reported_operation_finish_date, \
  eo_DB.reported_operation_status \
  FROM eo_DB \
  LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
  LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
  LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
  LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code"
  
  excel_master_eo_df = pd.read_sql_query(sql, con)
  excel_master_eo_df.sort_values(['be_code','teh_mesto'], inplace=True)
  date_time_plug = '31/12/2099 23:59:59'
  date_time_plug = datetime.strptime(date_time_plug, '%d/%m/%Y %H:%M:%S')
  # excel_master_eo_df.to_csv('temp_data/excel_master_eo_df.csv')
  
  excel_master_eo_df['expected_operation_finish_date'] = pd.to_datetime(excel_master_eo_df['expected_operation_finish_date'], errors = 'coerce')
  
  excel_master_eo_df['operation_start_date'] = pd.to_datetime(excel_master_eo_df['operation_start_date'])
  
  excel_master_eo_df['expected_operation_status_code_date'] = pd.to_datetime(excel_master_eo_df['expected_operation_status_code_date'])
  
  excel_master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(excel_master_eo_df['sap_planned_finish_operation_date'])
  
  excel_master_eo_df['reported_operation_finish_date'] = pd.to_datetime(excel_master_eo_df['reported_operation_finish_date'])

  for row in excel_master_eo_df.itertuples():
    index_value = getattr(row, 'Index')
    eo_code = getattr(row, "eo_code")
    operation_start_date = read_date(getattr(row, "operation_start_date"), eo_code) 
    expected_operation_finish_date = read_date(getattr(row, "expected_operation_finish_date"), eo_code)
    reported_operation_finish_date = read_date(getattr(row, "reported_operation_finish_date"), eo_code)
    if reported_operation_finish_date != datetime.strptime('1.1.2199', '%d.%m.%Y'):
      expected_operation_finish_date = reported_operation_finish_date
      
    if eo_code != None:
      # print(eo_code, operation_start_date, expected_operation_finish_date, reported_operation_finish_date)
      
      year_list = [2022]
      for year in year_list:
        year_first_date = f'1/1/{year}'
        year_first_date = datetime.strptime(year_first_date, '%d/%m/%Y')
        year_last_date = f'31/12/{year}'
        year_last_date = datetime.strptime(year_last_date, '%d/%m/%Y')
        
        if operation_start_date < year_last_date and expected_operation_finish_date > year_last_date:
          print(eo_code, "operation_start_date: ", operation_start_date, "year_last_date: ", year_last_date, "expected_operation_finish_date: ", expected_operation_finish_date)
        
        
  #     
  #     start_2022_date = '01/01/2022'
  #     start_2022_date = datetime.strptime(start_2022_date, '%d/%m/%Y')

  #     index_value = getattr(row, 'Index')
  #     if operation_start_date < finish_2022_date and expected_operation_finish_date > finish_2022_date:
  #       excel_master_eo_df.loc[index_value, ["qty at 31.12.2022"]] = 1
  #     else:
  #       excel_master_eo_df.loc[index_value, ["qty at 31.12.2022"]] = 0

  #     # оборудование, зашедшее в 2022 году  
  #     if operation_start_date > start_2022_date and operation_start_date < finish_2022_date:
  #       excel_master_eo_df.loc[index_value, ["in 2022"]] = 1
  #     else:
  #       excel_master_eo_df.loc[index_value, ["in 2022"]] = 0
      
  #     if  expected_operation_finish_date >  start_2022_date and expected_operation_finish_date < finish_2022_date:
       
  #       excel_master_eo_df.loc[index_value, ["out 2022"]] = 1
  #     else:
  #       excel_master_eo_df.loc[index_value, ["out 2022"]] = 0
  #     ################################################################################################################  
      
      
      
      
  # excel_master_eo_df["qty at 31.12.2022"] = excel_master_eo_df["qty at 31.12.2022"].astype(int)
  # excel_master_eo_df["in 2022"] = excel_master_eo_df["in 2022"].astype(int)
  # excel_master_eo_df["out 2022"] = excel_master_eo_df["out 2022"].astype(int)


  
  # excel_master_eo_df.to_csv('temp_data/excel_master_eo_df.csv', index = False)      
  # print(excel_master_eo_df.info())
sql_to_eo_master()