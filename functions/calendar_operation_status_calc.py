import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Eo_calendar_operation_status_DB
from initial_values.initial_values import sap_system_status_ban_list, sap_user_status_ban_list
from datetime import datetime
from app import app
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

    
def calendar_operation_status_calc():
  with app.app_context():
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
    eo_calendar_operation_status_DB.id \
    FROM eo_DB \
    LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
    LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
    LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
    LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code \
    LEFT JOIN eo_calendar_operation_status_DB ON eo_DB.eo_code = eo_calendar_operation_status_DB.eo_code"

    master_eo_df = pd.read_sql_query(sql, con)
    master_eo_df.sort_values(['be_code','teh_mesto'], inplace=True)
  
    master_eo_df['operation_start_date'] = pd.to_datetime(master_eo_df['operation_start_date'])
    master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(master_eo_df['sap_planned_finish_operation_date'])
    master_eo_df['evaluated_operation_finish_date'] = pd.to_datetime(master_eo_df['evaluated_operation_finish_date'])
    master_eo_df['reported_operation_finish_date'] = pd.to_datetime(master_eo_df['reported_operation_finish_date'])
    date_plug = datetime.strptime('1.1.2199', '%d.%m.%Y')
    master_eo_df['sap_planned_finish_operation_date'].fillna(date_plug, inplace = True)
    master_eo_df['reported_operation_finish_date'].fillna(date_plug, inplace = True)
    master_eo_df['evaluated_operation_finish_date'].fillna(date_plug, inplace = True)
    master_eo_df['sap_system_status'].fillna("plug", inplace = True)
    master_eo_df['sap_user_status'].fillna("plug", inplace = True)


    ###################### Обновление списка ео в calendar_operation_status_eo ###########################
    sql_calendar_operation_status = "SELECT eo_calendar_operation_status_DB.eo_code FROM eo_calendar_operation_status_DB"
    calendar_operation_status_df = pd.read_sql_query(sql_calendar_operation_status, con)
    calendar_operation_status_eo_list = list(calendar_operation_status_df['eo_code'])

    calendar_operation_status_eo_list_add_df = master_eo_df.loc[~master_eo_df['eo_code'].isin(calendar_operation_status_eo_list)]
    calendar_operation_status_eo_list_add = list(calendar_operation_status_eo_list_add_df['eo_code'])
    if len(calendar_operation_status_eo_list_add)>0:
      for eo_to_add in calendar_operation_status_eo_list_add:
        insert_calendar_sql = f"INSERT INTO eo_calendar_operation_status_DB (eo_code) VALUES ('{eo_to_add}');"
        cursor.execute(insert_calendar_sql)
        con.commit() 

    # В поле  evaluated_operation_finish_date  указываем значение из expected_operation_finish_date
    master_eo_filtered_df = master_eo_df.loc[master_eo_df['evaluated_operation_finish_date'] ==date_plug]
    indexes = list(master_eo_filtered_df.index.values)
    master_eo_df.loc[indexes, ['evaluated_operation_finish_date']] = master_eo_df['expected_operation_finish_date']
    
    # выборка в которой пусто в колонке sap_planned_finish_operation_date
    master_eo_filtered_df_2 = master_eo_df.loc[master_eo_df['sap_planned_finish_operation_date'] != date_plug]
    # print(master_eo_filtered_df_2['sap_planned_finish_operation_date'])
    indexes = list(master_eo_filtered_df_2.index.values)
    dates_list_df = master_eo_df.loc[indexes, ['sap_planned_finish_operation_date']]
    master_eo_df.loc[indexes, ['evaluated_operation_finish_date']] = list(dates_list_df['sap_planned_finish_operation_date'])

    master_eo_filtered_df_3 = master_eo_df.loc[master_eo_df['reported_operation_finish_date'] != date_plug]
    indexes_3 = list(master_eo_filtered_df_3.index.values)
    dates_list_df_3 = master_eo_df.loc[indexes_3, ['reported_operation_finish_date']]
    master_eo_df.loc[indexes_3, ['evaluated_operation_finish_date']] = list(dates_list_df_3['reported_operation_finish_date'])


    

    master_eo_df.to_sql('calendar_operation_status_calc_temp', con=con, if_exists='replace', index = False)

    # обновление evaluated_operation_finish_date
    for row in master_eo_df.itertuples():
      evaluated_operation_finish_date = getattr(row, "evaluated_operation_finish_date")
      eo_code = getattr(row, "eo_code")
      update_calendar_sql = f"UPDATE eo_DB SET evaluated_operation_finish_date = '{evaluated_operation_finish_date}' WHERE eo_code = '{eo_code}';"
      cursor.execute(update_calendar_sql)
      con.commit()     
    
    # calendar_list = ['july_2022', 'august_2022', 'september_2022', 'october_2022', 'november_2022', 'december_2022', 'year_2022', 'year_2023', 'year_2024', 'year_2025', 'year_2026', 'year_2027']
    calendar_list = ['year_2022', 'year_2023', 'year_2024', 'year_2025', 'year_2026', 'year_2027', 'year_2028', 'year_2029', 'year_2030', 'year_2031']
    qty_column_name = 'july_2022_qty'
    qty_in_column_name = 'july_2022_in'
    qty_out_column_name = 'july_2022_out'
    for calendar_point in calendar_list:
      print(calendar_point)
      if calendar_point == 'july_2022':
        age_date = datetime.strptime('31.07.2022', '%d.%m.%Y')
        period_begin = datetime.strptime('01.07.2022', '%d.%m.%Y')
        qty_column_name = 'july_2022_qty'
        age_column_name = 'july_2022_age'
        qty_in_column_name = 'july_2022_in'
        qty_out_column_name = 'july_2022_out'
      elif  calendar_point == 'august_2022': 
        age_date = datetime.strptime('31.08.2022', '%d.%m.%Y')
        period_begin = datetime.strptime('01.08.2022', '%d.%m.%Y')
        qty_column_name = 'august_2022_qty'
        age_column_name = 'august_2022_age'
        qty_in_column_name = 'august_2022_in'
        qty_out_column_name = 'august_2022_out'
      elif  calendar_point == 'september_2022': 
        age_date = datetime.strptime('30.09.2022', '%d.%m.%Y')
        period_begin = datetime.strptime('01.09.2022', '%d.%m.%Y')
        qty_column_name = 'sep_2022_qty'
        age_column_name = 'sep_2022_age'
        qty_in_column_name = 'sep_2022_in'
        qty_out_column_name = 'sep_2022_out' 
      elif  calendar_point == 'october_2022': 
        age_date = datetime.strptime('31.10.2022', '%d.%m.%Y')
        period_begin = datetime.strptime('01.10.2022', '%d.%m.%Y')
        qty_column_name = 'oct_2022_qty'
        age_column_name = 'oct_2022_age'
        qty_in_column_name = 'oct_2022_in'
        qty_out_column_name = 'oct_2022_out'   
      elif  calendar_point == 'november_2022': 
        age_date = datetime.strptime('30.11.2022', '%d.%m.%Y')
        period_begin = datetime.strptime('01.11.2022', '%d.%m.%Y')
        qty_column_name = 'nov_2022_qty'
        age_column_name = 'nov_2022_age'
        qty_in_column_name = 'nov_2022_in'
        qty_out_column_name = 'nov_2022_out'
      elif  calendar_point == 'december_2022': 
        age_date = datetime.strptime('31.12.2022', '%d.%m.%Y')
        period_begin = datetime.strptime('01.12.2022', '%d.%m.%Y')
        qty_column_name = 'dec_2022_qty'
        age_column_name = 'dec_2022_age'
        qty_in_column_name = 'dec_2022_in'
        qty_out_column_name = 'dec_2022_out' 
      elif  calendar_point == 'year_2022': 
        age_date = datetime.strptime('31.12.2022', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2022', '%d.%m.%Y')
        qty_column_name = 'year_2022_qty'
        age_column_name = 'year_2022_age'
        qty_in_column_name = 'year_2022_in'
        qty_out_column_name = 'year_2022_out'
        age_column_name = 'year_2022_age' 
      elif  calendar_point == 'year_2023': 
        age_date = datetime.strptime('31.12.2023', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2023', '%d.%m.%Y')
        qty_column_name = 'year_2023_qty'
        age_column_name = 'year_2023_age'
        qty_in_column_name = 'year_2023_in'
        qty_out_column_name = 'year_2023_out'  
      elif  calendar_point == 'year_2024': 
        age_date = datetime.strptime('31.12.2024', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2024', '%d.%m.%Y')
        qty_column_name = 'year_2024_qty'
        age_column_name = 'year_2024_age'
        qty_in_column_name = 'year_2024_in'
        qty_out_column_name = 'year_2024_out'
      elif  calendar_point == 'year_2025': 
        age_date = datetime.strptime('31.12.2025', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2025', '%d.%m.%Y')
        qty_column_name = 'year_2025_qty'
        age_column_name = 'year_2025_age'
        qty_in_column_name = 'year_2025_in'
        qty_out_column_name = 'year_2025_out' 
      elif  calendar_point == 'year_2026': 
        age_date = datetime.strptime('31.12.2026', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2026', '%d.%m.%Y')
        qty_column_name = 'year_2026_qty'
        age_column_name = 'year_2026_age'
        qty_in_column_name = 'year_2026_in'
        qty_out_column_name = 'year_2026_out'  
      elif  calendar_point == 'year_2027': 
        age_date = datetime.strptime('31.12.2027', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2027', '%d.%m.%Y')
        qty_column_name = 'year_2027_qty'
        age_column_name = 'year_2027_age'
        qty_in_column_name = 'year_2027_in'
        qty_out_column_name = 'year_2027_out'
      elif  calendar_point == 'year_2028': 
        age_date = datetime.strptime('31.12.2028', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2028', '%d.%m.%Y')
        qty_column_name = 'year_2028_qty'
        age_column_name = 'year_2028_age'
        qty_in_column_name = 'year_2028_in'
        qty_out_column_name = 'year_2028_out'
      elif  calendar_point == 'year_2029': 
        age_date = datetime.strptime('31.12.2029', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2029', '%d.%m.%Y')
        qty_column_name = 'year_2029_qty'
        age_column_name = 'year_2029_age'
        qty_in_column_name = 'year_2029_in'
        qty_out_column_name = 'year_2029_out'   
      elif  calendar_point == 'year_2030': 
        age_date = datetime.strptime('31.12.2030', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2030', '%d.%m.%Y')
        qty_column_name = 'year_2030_qty'
        age_column_name = 'year_2030_age'
        qty_in_column_name = 'year_2030_in'
        qty_out_column_name = 'year_2030_out'
      elif  calendar_point == 'year_2031': 
        age_date = datetime.strptime('31.12.2031', '%d.%m.%Y')
        period_begin = datetime.strptime('01.01.2031', '%d.%m.%Y')
        qty_column_name = 'year_2031_qty'
        age_column_name = 'year_2031_age'
        qty_in_column_name = 'year_2031_in'
        qty_out_column_name = 'year_2031_out'  

      # выборка в которую попало то что находится в эксплуатации
      update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_column_name}'=0;"
      cursor.execute(update_calendar_sql)
      con.commit() 
      eo_master_temp_df = master_eo_df.loc[master_eo_df['operation_start_date'] < age_date] 
      eo_master_temp_df = eo_master_temp_df.loc[eo_master_temp_df['evaluated_operation_finish_date'] > age_date]
      eo_master_temp_df = eo_master_temp_df.loc[~eo_master_temp_df['sap_system_status'].isin(sap_system_status_ban_list)]
      eo_master_temp_df = eo_master_temp_df.loc[~eo_master_temp_df['sap_user_status'].isin(sap_user_status_ban_list)]
      eo_master_temp_df[age_column_name] = (age_date - eo_master_temp_df['operation_start_date']).dt.days / 365.25
      if len(eo_master_temp_df) > 0:
        for row in eo_master_temp_df.itertuples():
          eo_code = getattr(row, 'eo_code')
          age = getattr(row, age_column_name)
          update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_column_name}'=1, '{age_column_name}'={age}  WHERE eo_code='{eo_code}';"
          cursor.execute(update_calendar_sql)
          con.commit()  
      

      # выборка в которой в указанный период было поступление
      update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_in_column_name}'=0;"
      cursor.execute(update_calendar_sql)
      con.commit() 
      eo_master_temp_in_df = master_eo_df.loc[master_eo_df['operation_start_date'] >= period_begin]
      eo_master_temp_in_df = eo_master_temp_in_df.loc[eo_master_temp_in_df['operation_start_date'] <= age_date]
      eo_master_temp_in_df = eo_master_temp_in_df.loc[~eo_master_temp_in_df['sap_system_status'].isin(sap_system_status_ban_list)]
      eo_master_temp_in_df = eo_master_temp_in_df.loc[~eo_master_temp_in_df['sap_user_status'].isin(sap_user_status_ban_list)]
      
      
      if len(eo_master_temp_in_df) > 0:
        for row in eo_master_temp_in_df.itertuples():
          eo_code = getattr(row, 'eo_code')
          update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_in_column_name}'=1 WHERE eo_code='{eo_code}';"
          cursor.execute(update_calendar_sql)
          con.commit()  
          
      # выборка в которой в указанный период было выбытие
      update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_out_column_name}'=0;"
      cursor.execute(update_calendar_sql)
      con.commit()
      eo_master_temp_out_df = master_eo_df.loc[master_eo_df['evaluated_operation_finish_date'] >= period_begin] 
      eo_master_temp_out_df = eo_master_temp_out_df.loc[eo_master_temp_out_df['evaluated_operation_finish_date']<=age_date]
      eo_master_temp_out_df = eo_master_temp_out_df.loc[~eo_master_temp_out_df['sap_system_status'].isin(sap_system_status_ban_list)]
      eo_master_temp_out_df = eo_master_temp_out_df.loc[~eo_master_temp_out_df['sap_user_status'].isin(sap_user_status_ban_list)]
      
      if len(eo_master_temp_out_df) > 0:
        for row in eo_master_temp_out_df.itertuples():
          eo_code = getattr(row, 'eo_code')
          update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_out_column_name}'=-1 WHERE eo_code='{eo_code}';"
          cursor.execute(update_calendar_sql)
          con.commit() 
          
      
      
      
      # eo_master_temp_df.to_csv('temp_data/eo_master_temp_df.csv')

    
      # if age_date > operation_start_date and age_date < evaluated_operation_finish_date and 'МТКУ' not in sap_system_status and 'КОНС' not in sap_user_status:
      #   calendar_record = Eo_calendar_operation_status_DB.query.filter_by(eo_code = eo_code).first()
      #   if calendar_record:
      #     update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_column_name}'=1 WHERE eo_code='{eo_code}';"
      #     cursor.execute(update_calendar_sql)
      #     con.commit()  
      #   else:
      #     update_calendar_sql = f"INSERT INTO eo_calendar_operation_status_DB SET (eo_code, '{qty_column_name}') VALUES ({eo_code}, 1);"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
        
      # # ЕСЛИ ДАТА НЕ ПОПАЛА МЕЖДУ НАЧАЛОМ И КОНЦОМ ЭКСПЛУАТАЦИИ
      # else:
      #   calendar_record = Eo_calendar_operation_status_DB.query.filter_by(eo_code = eo_code).first()
      #   if calendar_record:
      #     update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_column_name}'=0 WHERE eo_code='{eo_code}';"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
      #   else:
      #     update_calendar_sql = f"INSERT INTO eo_calendar_operation_status_DB SET \
      #     (eo_code, '{qty_column_name}') VALUES \
      #     ({eo_code}, 0);"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
      
      # # Заполнение колонки поступлений
      # # если дата начала эксплуатации попадает в период    
      # if operation_start_date > period_begin and operation_start_date < age_date and 'МТКУ' not in sap_system_status and 'КОНС' not in sap_user_status:
      #   calendar_record = Eo_calendar_operation_status_DB.query.filter_by(eo_code = eo_code).first()
      #   # если запись в календарном плане уже есть, то обновляем
      #   if calendar_record:
      #     update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_in_column_name}'=1 WHERE eo_code='{eo_code}';"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
      #   else:
      #     update_calendar_sql = f"INSERT INTO eo_calendar_operation_status_DB SET (eo_code, '{qty_in_column_name}') VALUES ({eo_code}, 1);"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
      # else:
      #   calendar_record = Eo_calendar_operation_status_DB.query.filter_by(eo_code = eo_code).first()
      #   # если запись в календарном плане уже есть, то обновляем
      #   if calendar_record:
      #     update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_in_column_name}'=0 WHERE eo_code='{eo_code}';"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
      #   else:
      #     update_calendar_sql = f"INSERT INTO eo_calendar_operation_status_DB SET (eo_code, '{qty_in_column_name}') VALUES ({eo_code}, 0);"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 

      # # Заполнение колонки убытий  
      # if evaluated_operation_finish_date > period_begin and evaluated_operation_finish_date < age_date:
      #   calendar_record = Eo_calendar_operation_status_DB.query.filter_by(eo_code = eo_code).first()
      #   # если запись в календарном плане уже есть, то обновляем
      #   if calendar_record:
      #     update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_out_column_name}'=1 WHERE eo_code='{eo_code}';"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
      #   else:
      #     update_calendar_sql = f"INSERT INTO eo_calendar_operation_status_DB SET (eo_code, '{qty_out_column_name}') VALUES ({eo_code}, 1);"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
      # else:
      #   calendar_record = Eo_calendar_operation_status_DB.query.filter_by(eo_code = eo_code).first()
      #   # если запись в календарном плане уже есть, то обновляем
      #   if calendar_record:
      #     update_calendar_sql = f"UPDATE eo_calendar_operation_status_DB SET '{qty_out_column_name}'=0 WHERE eo_code='{eo_code}';"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 
      #   else:
      #     update_calendar_sql = f"INSERT INTO eo_calendar_operation_status_DB SET (eo_code, '{qty_out_column_name}') VALUES ({eo_code}, 0);"
      #     cursor.execute(update_calendar_sql)
      #     con.commit() 

    