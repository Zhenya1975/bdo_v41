import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts
from initial_values.initial_values import sap_columns_to_master_columns
from datetime import datetime
# from app import app
import sqlite3

db = extensions.db

def read_date(date_input, eo_code):  

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

    
def age_calc(age_date):
  con = sqlite3.connect("database/datab.db")
  cursor = con.cursor()
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
  eo_DB.reported_operation_status, \
  eo_DB.evaluated_operation_finish_date, \
  eo_DB.age, \
  eo_DB.age_date \
  FROM eo_DB \
  LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
  LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
  LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
  LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code"
  
  master_eo_df = pd.read_sql_query(sql, con)
  master_eo_df.sort_values(['be_code','teh_mesto'], inplace=True)

  master_eo_df['operation_start_date'] = pd.to_datetime(master_eo_df['operation_start_date'])
  master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(master_eo_df['sap_planned_finish_operation_date'])
  master_eo_df['evaluated_operation_finish_date'] = pd.to_datetime(master_eo_df['evaluated_operation_finish_date'])
  master_eo_df['reported_operation_finish_date'] = pd.to_datetime(master_eo_df['reported_operation_finish_date'])
  master_eo_df['sap_system_status'].fillna("plug", inplace = True)
  master_eo_df['sap_user_status'].fillna("plug", inplace = True)

  
  
  age_column_name = 'age'
  age_date_column_name = 'age_date'
  age_calc_operation_status_column_name = 'age_calc_operation_status'
  if age_date == datetime.now():
    age_column_name = 'age'
    age_date_column_name = 'age_date'
    age_calc_operation_status_column_name = 'age_calc_operation_status'
  elif age_date == datetime.strptime('31.12.2022', '%d.%m.%Y'):
    age_column_name = 'age_31122022'
    age_date_column_name = 'age_date_31122022'
    age_calc_operation_status_column_name = 'age_31122022_calc_operation_status'
  elif age_date == datetime.strptime('31.12.2023', '%d.%m.%Y'):
    age_column_name = 'age_31122023'
    age_date_column_name = 'age_date_31122023'
    age_calc_operation_status_column_name = 'age_31122023_calc_operation_status' 
  elif age_date == datetime.strptime('31.12.2024', '%d.%m.%Y'):
    age_column_name = 'age_31122024'
    age_date_column_name = 'age_date_31122024'
    age_calc_operation_status_column_name = 'age_31122024_calc_operation_status'  
  elif age_date == datetime.strptime('31.12.2025', '%d.%m.%Y'):
    age_column_name = 'age_31122025'
    age_date_column_name = 'age_date_31122025'
    age_calc_operation_status_column_name = 'age_31122025_calc_operation_status' 
  elif age_date == datetime.strptime('31.12.2026', '%d.%m.%Y'):
    age_column_name = 'age_31122026'
    age_date_column_name = 'age_date_31122026'
    age_calc_operation_status_column_name = 'age_31122026_calc_operation_status' 
  elif age_date == datetime.strptime('31.12.2027', '%d.%m.%Y'):
    age_column_name = 'age_31122027'
    age_date_column_name = 'age_date_31122027'
    age_calc_operation_status_column_name = 'age_31122027_calc_operation_status' 
  
 
  update_expected_operation_status_code_date_sql = f"UPDATE eo_DB SET expected_operation_status_code_date ='{age_date}';"
  cursor.execute(update_expected_operation_status_code_date_sql)
  con.commit()
  
  for row in master_eo_df.itertuples():
    index_value = getattr(row, 'Index')
    eo_code = getattr(row, "eo_code")
    operation_start_date = getattr(row, "operation_start_date") 
    sap_planned_finish_operation_date = getattr(row, "sap_planned_finish_operation_date") 
    evaluated_operation_finish_date = getattr(row, "evaluated_operation_finish_date")
    age_date = age_date
    sap_system_status = getattr(row, "sap_system_status")
    sap_user_status = getattr(row, "sap_user_status")
    reported_operation_finish_date = getattr(row, "reported_operation_finish_date")
    # print(eo_code, "reported_operation_finish_date: ", reported_operation_finish_date, "evaluated_operation_finish_date: ", evaluated_operation_finish_date)

    if 'nat' not in str(type(sap_planned_finish_operation_date)):
      evaluated_operation_finish_date = sap_planned_finish_operation_date
      update_evaluated_operation_finish_date_sql = f"UPDATE eo_DB SET evaluated_operation_finish_date='{evaluated_operation_finish_date}' WHERE eo_code='{eo_code}';"
      # print(update_evaluated_operation_finish_date_sql)
      cursor.execute(update_evaluated_operation_finish_date_sql)
      con.commit()
    
    if 'nat' not in str(type(reported_operation_finish_date)):
      # print(reported_operation_finish_date)
      evaluated_operation_finish_date = reported_operation_finish_date
      update_evaluated_operation_finish_date_sql = f"UPDATE eo_DB SET evaluated_operation_finish_date='{evaluated_operation_finish_date}' WHERE eo_code='{eo_code}';"
      # print(update_evaluated_operation_finish_date_sql)
      cursor.execute(update_evaluated_operation_finish_date_sql)
      con.commit()

    
    # operation_start_date = read_date(getattr(row, "operation_start_date"), eo_code) 
    # evaluated_operation_finish_date = read_date(getattr(row, "evaluated_operation_finish_date"), eo_code)
    # if 'МТКУ' not in sap_system_status and 'КОНС' not in sap_user_status:
      # print('good')
    # try:
    #   con.execute('ALTER TABLE eo_DB ADD COLUMN today_age;')
    #   con.commit()
    #   print("должна быть добавлена колонка today_age")
    # except:
    #   print("уже есть колонка today_age")
    #   pass # handle the error

    if age_date > operation_start_date and age_date < evaluated_operation_finish_date and 'МТКУ' not in sap_system_status and 'КОНС' not in sap_user_status:
      age_years = (age_date - operation_start_date).days / 365.25
      
      
      update_record_sql = f"UPDATE eo_DB SET '{age_column_name}'='{age_years}', '{age_date_column_name}' = '{age_date}', '{age_calc_operation_status_column_name}' = 1  WHERE eo_code='{eo_code}';"
    else:
      update_record_sql = f"UPDATE eo_DB SET '{age_column_name}'=0, '{age_date_column_name}' = '{age_date}', '{age_calc_operation_status_column_name}' = 0  WHERE eo_code='{eo_code}';"
    
    cursor.execute(update_record_sql)
    con.commit()
  cursor.close()
      
      