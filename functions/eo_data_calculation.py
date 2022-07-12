import pandas as pd
from extensions import extensions
from datetime import datetime
# from app import app
import sqlite3
from dateutil.relativedelta import relativedelta

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

    
def eo_data_calculation():
  """
  1. Если expected_operation_finish_date не пустое, то в evaluated_operation_finish_date присваивается expected_operation_finish_date.
  2. Если sap_planned_finish_operation_date не пустое, то в evaluated_operation_finish_date присваивается sap_planned_finish_operation_date
  3. Если reported_operation_finish_date не пустое, то в evaluated_operation_finish_date присваивается reported_operation_finish_date
  """
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
  eo_DB.operation_finish_date_calc, \
  eo_DB.operation_finish_date_sap_upd, \
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
  master_eo_df['expected_operation_finish_date'] = pd.to_datetime(master_eo_df['expected_operation_finish_date'])
  
  master_eo_df['operation_finish_date_sap_upd'] = pd.to_datetime(master_eo_df['operation_finish_date_sap_upd'])
  
  master_eo_df['evaluated_operation_finish_date'] = pd.to_datetime(master_eo_df['evaluated_operation_finish_date'])
  master_eo_df['reported_operation_finish_date'] = pd.to_datetime(master_eo_df['reported_operation_finish_date'])
  master_eo_df['sap_system_status'].fillna("plug", inplace = True)
  master_eo_df['sap_user_status'].fillna("plug", inplace = True)
  # print('засечка')
  # обновление значения в поле operation_finish_date_calc - расчетном значении даты завершения от срока службы
  master_eo_df['expected_operation_period_years_timedelta'] = pd.to_timedelta(master_eo_df['expected_operation_period_years']*365.25, unit='D')
  master_eo_df['operation_finish_date_calc'] = master_eo_df['operation_start_date'] + master_eo_df['expected_operation_period_years_timedelta']
  
  # обновление приведенной даты завершения. В приоритете - дата из поля в сап. Если его нет, то берем расчетную
  date_time_plug = '31/12/2099 23:59:59'
  date_time_plug = datetime.strptime(date_time_plug, '%d/%m/%Y %H:%M:%S')
  master_eo_df['operation_finish_date_sap_upd_temp'] = master_eo_df['sap_planned_finish_operation_date']
  master_eo_df['operation_finish_date_sap_upd_temp'].fillna(date_time_plug, inplace = True)
  

  master_eo_df_temp = master_eo_df.loc[master_eo_df['operation_finish_date_sap_upd_temp']==date_time_plug]
  indexes = list(master_eo_df_temp.index.values)
  
  # рассчитываем отклонение приведенной даты сап от даты reported date
  master_eo_df['reported_operation_finish_date'].fillna(date_time_plug, inplace = True)
  master_eo_df_reported_dates = master_eo_df.loc[master_eo_df['reported_operation_finish_date'] != date_time_plug]
  
  
  master_eo_df_reported_dates = master_eo_df_reported_dates.copy()
  master_eo_df_reported_dates['finish_date_delta'] = (master_eo_df_reported_dates['reported_operation_finish_date'] - master_eo_df_reported_dates['operation_finish_date_sap_upd']).dt.days 
  indexes = list(master_eo_df_reported_dates.index.values)
  master_eo_df['finish_date_delta'] = 0
  master_eo_df.loc[indexes, 'finish_date_delta'] = master_eo_df_reported_dates.loc[indexes, 'finish_date_delta']
  

  i=0
  lenght = len(master_eo_df)
  for row in master_eo_df.itertuples():
    i = i +1
    print(i, " из ", lenght)
    index_value = getattr(row, 'Index')
    eo_code = getattr(row, "eo_code")
    operation_start_date = getattr(row, "operation_start_date") 
    sap_planned_finish_operation_date = getattr(row, "sap_planned_finish_operation_date") 
    operation_finish_date_calc = getattr(row, "operation_finish_date_calc")
    
    operation_finish_date_sap_upd_temp = getattr(row, "operation_finish_date_sap_upd_temp")
    
    expected_operation_finish_date= getattr(row, "expected_operation_finish_date") 
    evaluated_operation_finish_date = getattr(row, "evaluated_operation_finish_date")
    sap_system_status = getattr(row, "sap_system_status")
    sap_user_status = getattr(row, "sap_user_status")
    reported_operation_finish_date = getattr(row, "reported_operation_finish_date")
    # print(eo_code, "reported_operation_finish_date: ", reported_operation_finish_date, "evaluated_operation_finish_date: ", evaluated_operation_finish_date)

    # обновление значения расчетного значения времени выбытия по значению срока службы
    operation_finish_date_calc_update_sql = f"UPDATE eo_DB SET operation_finish_date_calc='{operation_finish_date_calc}' WHERE eo_code='{eo_code}';"
    cursor.execute(operation_finish_date_calc_update_sql)
    con.commit()  

    # обновление значения operation_finish_date_sap_upd - приведенная дата завершения из сап. 
    if operation_finish_date_sap_upd_temp == date_time_plug:
      operation_finish_date_sap_upd = operation_finish_date_calc
    else:
      operation_finish_date_sap_upd = sap_planned_finish_operation_date
    operation_finish_date_sap_upd_sql = f"UPDATE eo_DB SET operation_finish_date_sap_upd='{operation_finish_date_sap_upd}' WHERE eo_code='{eo_code}';"   
   
    cursor.execute(operation_finish_date_sap_upd_sql)
    con.commit()    
    # finish_date_delta = getattr(row, "finish_date_delta")
    # finish_date_delta_sql = f"UPDATE eo_DB SET finish_date_delta = {finish_date_delta} WHERE eo_code='{eo_code}'"
    # cursor.execute(finish_date_delta_sql)
    # con.commit() 

    if 'nat' not in str(type(expected_operation_finish_date)):
      evaluated_operation_finish_date = expected_operation_finish_date
      update_evaluated_operation_finish_date_sql = f"UPDATE eo_DB SET evaluated_operation_finish_date='{evaluated_operation_finish_date}' WHERE eo_code='{eo_code}';"
      # print(update_evaluated_operation_finish_date_sql)
      cursor.execute(update_evaluated_operation_finish_date_sql)
      con.commit()  

    
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

  cursor.close()
      
      