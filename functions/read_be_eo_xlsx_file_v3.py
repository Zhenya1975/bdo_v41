import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB
from initial_values.initial_values import be_data_columns_to_master_columns
from datetime import datetime
from initial_values.initial_values import sap_user_status_cons_status_list, be_data_cons_status_list, sap_system_status_ban_list
import sqlite3

db = extensions.db

be_data_columns_to_master_columns = be_data_columns_to_master_columns

date_time_plug = '1.1.2199'
date_time_plug = datetime.strptime(date_time_plug, '%d.%m.%Y')

def create_conflict(be_eo_data_row_no, be_data_eo_code, eo_conflict_field, eo_conflict_field_current_master_data, eo_conflict_field_uploaded_data, infodata_filename, infodata_sender_email, infodata_sender_email_date):
  
  new_conflict_record = Eo_data_conflicts(be_eo_data_row_no = be_eo_data_row_no, eo_code = be_data_eo_code, eo_conflict_field = eo_conflict_field, eo_conflict_field_current_master_data = eo_conflict_field_current_master_data, eo_conflict_field_uploaded_data = eo_conflict_field_uploaded_data, eo_conflict_description = f"EO {be_data_eo_code} в поле {eo_conflict_field} значение из файла {eo_conflict_field_uploaded_data} не соответствует значению в мастер-файле {eo_conflict_field_current_master_data}", filename = infodata_filename, sender_email = infodata_sender_email, email_date = infodata_sender_email_date)
  infodata_filename = infodata_filename
  infodata_sender_email = infodata_sender_email
  # infodata_sender_email_date = infodata_sender_email_date        
  db.session.add(new_conflict_record)
  # добавляем новую запись в лог файл
  log_data_new_record = LogsDB(log_text = f"Добавлена запись о новом конфликте. В eo_code ({be_data_eo_code})в поле {eo_conflict_field} значение из файла ({eo_conflict_field_uploaded_data}) не соответствует значению в мастер-файле ({eo_conflict_field_current_master_data})", log_status = "new", be_filename = infodata_filename, be_sender_email = infodata_sender_email)
  db.session.add(log_data_new_record)
  db.session.commit()

def solve_conflict(eo_code, eo_conflict_field, conflict_field_value):
  conflict_record = Eo_data_conflicts.query.filter_by(eo_code = eo_code, eo_conflict_field = eo_conflict_field, eo_conflict_status = "active").first()
  # infodata_filename, infodata_sender_email, infodata_sender_email_date
  infodata_filename = conflict_record.filename
  infodata_sender_email = conflict_record.sender_email
  # infodata_sender_email_date = conflict_record.email_date
  conflict_record.eo_conflict_status = "resolved"
  # добавляем запись в лог-файл
  log_data_new_record = LogsDB(log_eo_code = eo_code, log_text = f"Разрешен конфликт по полю {eo_conflict_field} в eo_code ({eo_code}). Значение поля {eo_conflict_field} в мастер-данных: {conflict_field_value}", 	log_status = "new", be_filename = infodata_filename, be_sender_email = infodata_sender_email)
  db.session.add(log_data_new_record)
  db.session.commit()

def rewrite_conflict(eo_code, eo_conflict_field, conflict_field_value_be, conflict_field_value_masterdata):
  conflict_record = Eo_data_conflicts.query.filter_by(eo_code = eo_code, eo_conflict_field = eo_conflict_field, eo_conflict_status = "active").first()
  infodata_filename = conflict_record.filename
  infodata_sender_email = conflict_record.sender_email
  # infodata_sender_email_date = conflict_record.email_date
  
  conflict_record.eo_conflict_field_uploaded_data = conflict_field_value_be
  conflict_record.eo_conflict_field_current_master_data = conflict_field_value_masterdata
  log_data_new_record = LogsDB(log_text = f"В таблице конфликтов есть запись о конфликте по полю {eo_conflict_field} eo_code ({eo_code})", log_status = "new", be_filename = infodata_filename, be_sender_email = infodata_sender_email)
  db.session.add(log_data_new_record)
  db.session.commit()


def field_check_status(be_eo_data_row_no, be_data_eo_code, field_name, field_be_data, field_master_data, infodata_filename, infodata_sender_email, infodata_sender_email_date):
  field_status ={}
  field_status['field_name'] = field_name
  field_status['be_data'] = field_be_data
  field_status['master_data'] = field_master_data

  if field_be_data == field_master_data:
    field_status['values_status'] = 'equal'
  else:
    field_status['values_status'] = 'not_equal'

  # проверяем на наличие конфликта с текущим eo_code
  conflict_data = Eo_data_conflicts.query.filter_by(eo_code = be_data_eo_code, eo_conflict_field = field_name, eo_conflict_status = "active").first()

  if conflict_data:
    field_status['conflict_status'] = 'exist'
  else:
    field_status['conflict_status'] = 'not_exist'
  
# если значения равны и конфликт есть, то разрешаем конфликт
  if  field_status['values_status'] == 'equal' and  field_status['conflict_status'] == 'exist':
    solve_conflict(be_data_eo_code, field_name, field_be_data)
  
  # если значения не равны и конфликт есть, то даем перезаписываем конфликт, даем лог и идем дальше   
  elif field_status['values_status'] == 'not_equal' and  field_status['conflict_status'] == 'exist':
    rewrite_conflict(be_data_eo_code, field_name, field_be_data, field_master_data)

  # если значения не равны и конфликта нет, то создаем конфликт
  elif field_status['values_status'] == 'not_equal' and  field_status['conflict_status'] == 'not_exist': 
    create_conflict(be_eo_data_row_no, be_data_eo_code, field_name, field_master_data, field_be_data, infodata_filename, infodata_sender_email, infodata_sender_email_date)  
  
    # если значения равны и конфликта нет, то ничего не происходит  - идем дальше
  elif field_status['values_status'] == 'equal' and  field_status['conflict_status'] == 'not_exist':
    pass

  else:
    print("что-то непонятное")

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

  
  elif "nat" in str(type(date_input)) or "NaT" in str(type(date_input)) or "float" in str(type(date_input)):
    date_output = datetime.strptime('1.1.2199', '%d.%m.%Y')
    return date_output
  else:
    print(eo_code, "не покрыто типами данных дат", type(date_input), date_input)
    date_output = datetime.strptime('1.1.2199', '%d.%m.%Y')
    return date_output

def read_be_eo_xlsx():
  # with app.app_context():
  # читаем excel с данными из бизнес-единиц. Проверяем - если нет нужного листа с данными, то отдаем ошибку
  be_eo_data = pd.DataFrame()
  try:
    be_eo_raw_data = pd.read_excel('uploads/be_eo_data.xlsx', sheet_name='be_eo_data', index_col = False)
    be_eo_data = be_eo_raw_data.rename(columns=be_data_columns_to_master_columns)
    be_eo_data['eo_code'] = be_eo_data['eo_code'].astype(str)
  except Exception as e:
    print("не удалось прочитать файл uploads/be_eo_data.xlsx. Ошибка: ", e)
    log_data_new_record = LogsDB(log_text = f"не удалось прочитать файл uploads/be_eo_data.xlsx. Ошибка: , {e})", log_status = "new")
    db.session.add(log_data_new_record)
  
  # читаем данные из инфо вкладки   
  be_data_info = pd.DataFrame()
  try:
    be_data_info = pd.read_excel('uploads/be_eo_data.xlsx', sheet_name='be_eo_file_data', index_col = False, dtype=str)
  except Exception as e:
    print("не удалось прочитать данные из инфо-вкладки файла uploads/be_eo_data.xlsx. Ошибка: ", e)
    log_data_new_record = LogsDB(log_text = f"не удалось прочитать данные из инфо-вкладки файла uploads/be_eo_data.xlsx. Ошибка: , {e})", log_status = "new")
    db.session.add(log_data_new_record)
    
  infodata_filename = be_data_info.loc[be_data_info.index[0], ['filename']][0]
  infodata_sender_email = be_data_info.loc[be_data_info.index[0], ['sender_email']][0]
  infodata_sender_email_date = be_data_info.loc[be_data_info.index[0], ['e-mail_date']][0]

  # обновляем в базе поля со статусами конфликтов
  
  
  con = sqlite3.connect("database/datab.db")
  cursor = con.cursor()
  # sql = "SELECT * FROM eo_DB JOIN be_DB"
  sql = "SELECT \
  eo_DB.eo_code, \
  eo_DB.operation_finish_date_sap_upd, \
  eo_DB.reported_operation_finish_date, \
  eo_DB.operation_finish_date_conflict \
  FROM eo_DB"
  master_eo_temp_df = pd.read_sql_query(sql, con)
  date_time_plug = '31/12/2099 23:59:59'
  date_time_plug = datetime.strptime(date_time_plug, '%d/%m/%Y %H:%M:%S')
  master_eo_temp_df['operation_finish_date_sap_upd'] = pd.to_datetime(master_eo_temp_df['operation_finish_date_sap_upd'])
  
  master_eo_temp_df['reported_operation_finish_date'] = pd.to_datetime(master_eo_temp_df['reported_operation_finish_date'])
  master_eo_temp_df['reported_operation_finish_date'].fillna(date_time_plug, inplace = True)
  master_eo_temp_temp_df = master_eo_temp_df.loc[master_eo_temp_df['reported_operation_finish_date']!=date_time_plug]
  master_eo_temp_temp_df = master_eo_temp_temp_df.copy()
  master_eo_temp_temp_df['finish_date_delta'] = (master_eo_temp_temp_df['reported_operation_finish_date'] - master_eo_temp_temp_df['operation_finish_date_sap_upd']).dt.total_seconds()
  
  # master_eo_temp_temp_df.to_csv('temp_data/master_eo_temp_temp_df.csv')
  
  master_eo_temp_temp_temp_df = master_eo_temp_temp_df.loc[master_eo_temp_temp_df['finish_date_delta'] !=0]
  update_sql_clear = f"UPDATE eo_DB SET operation_finish_date_conflict =NULL ;"
  cursor.execute(update_sql_clear)
  con.commit()
  for row in master_eo_temp_temp_temp_df.itertuples():
    eo_code = getattr(row, 'eo_code')
    update_sql = f"UPDATE eo_DB SET operation_finish_date_conflict ='дата завершения эксплуатации отличается' WHERE eo_code='{eo_code}';"
    cursor.execute(update_sql)
    con.commit()
  
  # предыдущие данные в лог файле ресетим
  log_data_updated = LogsDB.query.update(dict(log_status='old'))
  db.session.commit()
  
  be_eo_column_list = list(be_eo_data.columns)
  ################################################ чтение загруженного файла ###############################################

  for row in be_eo_data.itertuples():
    ####################################### ПОЛЕ НОМЕР СТРОКИ #######################
    be_eo_data_row_no = 'row_number'
    if 'be_eo_data_row_no' in be_eo_column_list:
      be_eo_data_row_no = getattr(row, "be_eo_data_row_no")

    ####################################### ПОЛЕ eo_code #######################
    # читаем мастер-файл из базы
    be_data_eo_code = str(getattr(row, "eo_code"))
    eo_master_data=Eo_DB.query.filter_by(eo_code=be_data_eo_code).first()

    ######### если eo не обнаружена, то создается кандидат на добавление #################
    if eo_master_data == None:
      new_eo_candidate_record = Eo_candidatesDB(eo_code=be_data_eo_code)
      db.session.add(new_eo_candidate_record)
      log_data_new_record = LogsDB(log_text = f"Добавлен кандидат на добавление в мастер. eo_code: {be_data_eo_code}", log_status = "new")
      db.session.add(log_data_new_record)
      db.session.commit()
    # если в мастер-данных есть запись с текущим eo_code, то пишем данные в базу
    else:
      # Проверка по полю Гаражный номер если такая колонка есть в загружаемом файле
      if 'gar_no' in be_eo_column_list:
        master_data_gar_no = str(eo_master_data.gar_no)
        be_data_gar_no = str(getattr(row, "gar_no"))
        if be_data_gar_no != master_data_gar_no:
          field_name = "gar_no"
          field_be_data = be_data_gar_no
          field_master_data = master_data_gar_no
          # запуск функции по проверке статуса текущего поля
          # либо создается новый конфликт, либо разрeшается старый. Если данные совпадают с местером, то идем дальше
          field_check_status(
            be_eo_data_row_no,
            be_data_eo_code,
            field_name, 
            field_be_data,
            field_master_data,
            infodata_filename, 
            infodata_sender_email, 
            infodata_sender_email_date
          )
      if 'reported_operation_finish_date' in be_eo_column_list:
        be_data_reported_operation_finish_date_raw = getattr(row, "reported_operation_finish_date")
        be_data_reported_operation_finish_datetime = read_date(be_data_reported_operation_finish_date_raw, be_data_eo_code)
        be_data_reported_operation_finish_date = be_data_reported_operation_finish_datetime.date()
        
        operation_finish_date_sap_upd_date = eo_master_data.operation_finish_date_sap_upd
        
        operation_finish_date_sap_upd_date = eo_master_data.operation_finish_date_sap_upd.date()
        if be_data_reported_operation_finish_date != operation_finish_date_sap_upd_date:
          eo_master_data.operation_finish_date_conflict = "дата завершения эксплуатации отличается"
          field_name = "reported_finish_date"
          field_be_data = be_data_reported_operation_finish_date
          field_master_data = operation_finish_date_sap_upd_date
          # запуск функции по проверке статуса текущего поля
          # либо создается новый конфликт, либо разрeшается старый. Если данные совпадают с местером, то идем дальше
          field_check_status(
            be_eo_data_row_no,
            be_data_eo_code,
            field_name, 
            field_be_data,
            field_master_data,
            infodata_filename, 
            infodata_sender_email, 
            infodata_sender_email_date
          )
          db.session.commit()
        
        eo_master_data.reported_operation_finish_date = be_data_reported_operation_finish_datetime
        db.session.commit()  

      if 'operation_status' in be_eo_column_list:
        operation_status = str(getattr(row, "operation_status"))
        eo_master_data.reported_operation_status = operation_status
        db.session.commit()

  # джойним загруженую таблицу с данными из мастер-дата


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
  eo_DB.maker, \
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
  eo_DB.operation_finish_date_calc, \
  eo_DB.operation_finish_date_sap_upd, \
  eo_DB.expected_operation_status_code, \
  eo_DB.sap_system_status, \
  eo_DB.sap_user_status, \
  eo_DB.finish_date_delta, \
  eo_DB.reported_operation_status, \
  eo_DB.reported_operation_status_date, \
  eo_DB.operation_finish_date_conflict, \
  eo_DB.evaluated_operation_finish_date \
  FROM eo_DB \
  LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
  LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
  LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
  LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code"


    
  excel_master_eo_df = pd.read_sql_query(sql, con)
  excel_master_eo_df.sort_values(['be_code','teh_mesto'], inplace=True)
  date_time_plug = '31/12/2099 23:59:59'
  date_time_plug = datetime.strptime(date_time_plug, '%d/%m/%Y %H:%M:%S')
  excel_master_eo_df['evaluated_operation_finish_date'] = pd.to_datetime(excel_master_eo_df['evaluated_operation_finish_date'])
  
  excel_master_eo_df['operation_start_date'] = pd.to_datetime(excel_master_eo_df['operation_start_date'])
  
  excel_master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(excel_master_eo_df['sap_planned_finish_operation_date'])
  
  be_master_data = pd.merge(be_eo_data, excel_master_eo_df, on = 'eo_code', how = 'left')
  # 
  be_master_data = be_master_data.loc[:, ['be_eo_data_row_no', 'eo_code', 'operation_status', 'operation_start_date', 'sap_system_status', 'sap_user_status','sap_planned_finish_operation_date', 'reported_operation_finish_date','operation_finish_date_conflict', 'reported_operation_status', 'evaluated_operation_finish_date']]
  
  # выборка записией во статусом "консервация" из мастер-данных
  be_master_data_cons_sub_data = be_master_data.loc[be_master_data['sap_user_status'].isin(sap_user_status_cons_status_list)]
  
  # исключаем записи со статусом МТКУ
  be_master_data_cons_sub_data = be_master_data_cons_sub_data.loc[~be_master_data_cons_sub_data['sap_system_status'].isin(sap_system_status_ban_list)]
  indexes = list(be_master_data_cons_sub_data.index.values)
  be_master_data.loc[indexes, ['master_data_cons_status']] = "master_data_cons"

  # выборка записей из загруженного файла со статусом "консервация"
  be_cons_sub_data = be_master_data.loc[be_master_data['reported_operation_status'].isin(be_data_cons_status_list)]
  indexes = list(be_cons_sub_data.index.values)
  be_master_data.loc[indexes, ['be_data_cons_status']] = "be_data_cons"
  # кол-во на конец года
  
  be_master_data.to_csv('temp_data/be_master_data.csv')
  
  cursor.close()
      