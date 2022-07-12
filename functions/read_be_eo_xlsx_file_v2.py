import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB
from initial_values.initial_values import be_data_columns_to_master_columns
from datetime import datetime

db = extensions.db

be_data_columns_to_master_columns = be_data_columns_to_master_columns


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

def read_be_eo_xlsx():
  # with app.app_context():
  # читаем excel с данными из бизнес-единиц. Проверяем - если нет нужного листа с данными, то отдаем ошибку
  be_eo_data = pd.DataFrame()
  try:
    be_eo_raw_data = pd.read_excel('uploads/be_eo_data.xlsx', sheet_name='be_eo_data', index_col = False)
    be_eo_data = be_eo_raw_data.rename(columns=be_data_columns_to_master_columns)
    be_eo_column_list = list(be_eo_data.columns)
    # поля с датами - в формат даты
    if 'gar_no' in be_eo_column_list:
      be_eo_data['gar_no'].fillna(0, inplace = True)
      be_eo_data["gar_no"] = be_eo_data["gar_no"].astype(str)
    if "eo_code" in be_eo_column_list:
      be_eo_data["eo_code"] = be_eo_data["eo_code"].astype(str)
    # print(be_eo_data.info())
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

  # предыдущие данные в лог файле ресетим
  log_data_updated = LogsDB.query.update(dict(log_status='old'))
  db.session.commit()

  ################################################ чтение загруженного файла ###############################################
  i=0
  lenght = len(be_eo_data)
  print(be_eo_data.info())
  for row in be_eo_data.itertuples():
    i = i+1
    be_eo_data_row_no = 'xyz'
    if 'be_eo_data_row_no' in be_eo_column_list:
      be_eo_data_row_no = getattr(row, "be_eo_data_row_no")

    be_data_eo_code = getattr(row, "eo_code")
    
    # читаем мастер-файл из базы
    eo_master_data=Eo_DB.query.filter_by(eo_code=be_data_eo_code).first()

    if eo_master_data == None:
      new_eo_candidate_record = Eo_candidatesDB(eo_code=be_data_eo_code)
      db.session.add(new_eo_candidate_record)
      log_data_new_record = LogsDB(log_text = f"Добавлен кандидат на добавление в мастер. eo_code: {be_data_eo_code}", log_status = "new")
      db.session.add(log_data_new_record)
      db.session.commit()
    # если в мастер-данных есть запись с текущим eo_code, то пишем данные в базу
    else:
      # print(be_data_eo_code, i, " из ", lenght)
      be_data_gar_no = eo_master_data.gar_no
      if 'gar_no' in be_eo_column_list:
        be_data_gar_no = str(getattr(row, "gar_no"))
        eo_master_data.gar_no = be_data_gar_no

        
      reported_operation_start_datetime = eo_master_data.reported_operation_start_date
      if 'reported_operation_start_date' in be_eo_column_list:
        reported_operation_start_date_raw = getattr(row, "reported_operation_start_date")
        reported_operation_start_datetime = read_date(reported_operation_start_date_raw, be_data_eo_code)
        eo_master_data.reported_operation_start_date = reported_operation_start_datetime
        db.session.commit()
        
      
      if 'reported_operation_finish_date' in be_eo_column_list:
        be_data_reported_operation_finish_date_raw = getattr(row, "reported_operation_finish_date")
        be_data_reported_operation_finish_datetime = read_date(be_data_reported_operation_finish_date_raw, be_data_eo_code)
        eo_master_data.reported_operation_finish_date = be_data_reported_operation_finish_datetime
        db.session.commit()
  
      if 'operation_status' in be_eo_column_list:
        operation_status = str(getattr(row, "operation_status"))
        eo_master_data.reported_operation_status = operation_status
        db.session.commit()
  
      if 'operation_status_date' in be_eo_column_list:
        operation_status_date_raw = getattr(row, "operation_status_date")
        operation_status_datetime = read_date(operation_status_date_raw, be_data_eo_code)
        eo_master_data.reported_operation_status_date = operation_status_datetime
        db.session.commit()

      