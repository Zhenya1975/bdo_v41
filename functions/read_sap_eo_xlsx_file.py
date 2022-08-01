import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB
from initial_values.initial_values import sap_columns_to_master_columns
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pytz


# from app import app

db = extensions.db

sap_columns_to_master_columns = sap_columns_to_master_columns




def expected_operation_status_code(eo_code, operation_start_date, calculated_operation_finish_date, sap_planned_finish_operarion_datetime, today_datetime):
  utc=pytz.UTC
  operation_start_date = utc.localize(operation_start_date)
  calculated_operation_finish_date = utc.localize(calculated_operation_finish_date)
  

  # print(type(sap_planned_finish_operarion_datetime))
  # если в поле sap_planned_finish_operarion_datetime ничего нет, то берем данные из расчетного поля
  if 'NoneType' in str(type(sap_planned_finish_operarion_datetime)):
    sap_expected_operation_finish_date = calculated_operation_finish_date
  else:
    sap_planned_finish_operarion_datetime = utc.localize(sap_planned_finish_operarion_datetime)
    sap_expected_operation_finish_date = sap_planned_finish_operarion_datetime
  # print(eo_code, " sap_expected_operation_finish_date: ", sap_expected_operation_finish_date)
  
  if operation_start_date < today_datetime and sap_expected_operation_finish_date > today_datetime:
    operation_status_code = "operation"
  elif sap_expected_operation_finish_date <  today_datetime:
    operation_status_code = "operation_finished"
  elif operation_start_date > today_datetime:
    operation_status_code = "purchase"
  # print(operation_status_code)
  return operation_status_code


def calculate_operation_finish_date(operation_start_date_raw, operation_period_years_raw, eo_code):
  operation_start_date = datetime.strptime('1.1.2199', '%d.%m.%Y')
  if "timestamp" in str(type(operation_start_date_raw)) or 'datetime' in str(type(operation_start_date_raw)):
    operation_start_date = operation_start_date_raw
  elif "str" in str(type(operation_start_date_raw)):
    try:
      operation_start_date = datetime.strptime(operation_start_date_raw, '%d.%m.%Y')
    except:
      pass
    try:
      operation_start_date = datetime.strptime(operation_start_date_raw, '%Y-%m-%d %H:%M:%S')
    except:
      pass  
    try:
      operation_start_date = datetime.strptime(operation_start_date_raw, '%Y-%m-%d')
    except Exception as e:
      print(f"eo_code: {eo_code}. Не удалось сохранить в дату '{operation_start_date_raw}, тип: {type(operation_start_date_raw)}'. Ошибка: ", e)
  
  try:
    operation_period_years = int(operation_period_years_raw)  
  except Exception as e:
    print(f"eo_code: {eo_code}. Не удалось получить период эксплуатации '{operation_period_years_raw}, тип: {type(operation_period_years_raw)}'. Ошибка: ", e)
    operation_period_years = 1313
  calculate_operation_finish_date = operation_start_date + relativedelta(years=operation_period_years)
  return calculate_operation_finish_date

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


def read_sap_eo_xlsx():
  """
  Итерируемся по файлу uploads/sap_eo_data.xlsx \n
  Если в мастер-данных нет записи, то создается новая запись в которую добавляется только eo_code. \n
  Обновление данных: \n
   - Проверяем по колонкам есть ли такие колонки. Если есть, то изменяем данные. \n
   - происходит расчет ожидаемого статуса эксплуатации expected_operation_status = expected_operation_status_code(eo_code_excel, operation_start_date, calculated_operation_finish_date, sap_planned_finish_operation_date, today_datetime)
  
  """
  # with app.app_context():
  sap_eo_raw_data = pd.read_excel('uploads/sap_eo_data.xlsx', index_col = False, dtype=str)
  
  sap_eo_data = sap_eo_raw_data.rename(columns=sap_columns_to_master_columns)
  sap_eo_column_list = list(sap_eo_data.columns)
  # print(sap_eo_column_list)
    
  # предыдущие данные в лог файле ресетим
  log_data_updated = LogsDB.query.update(dict(log_status='old'))
  db.session.commit()

  # итерируемся по полученному файлу
  i=0
  lenght = len(sap_eo_data)
  for row in sap_eo_data.itertuples():
    i=i+1
    print(i, " из ", lenght)
    eo_code_excel = getattr(row, "eo_code")
  
    # читаем мастер-файл из базы
    eo_master_data=Eo_DB.query.filter_by(eo_code=eo_code_excel).first()
    # если данных нет, то добавляем запись.
    if eo_master_data == None:
      new_eo_master_data_record = Eo_DB(eo_code=eo_code_excel)
      db.session.add(new_eo_master_data_record)
      # добавляем новую запись в лог файл
      log_data_new_record = LogsDB(log_text = f"В eo_master_data добавлена запись eo: {eo_code_excel}", log_status = "new")
      db.session.add(log_data_new_record)
      db.session.commit()
   
    # иначе обновляем данные в мастер-файле
    else:
      if 'be_code' in sap_eo_column_list:
        eo_master_data.be_code = getattr(row, "be_code")

      if 'eo_description' in sap_eo_column_list:
        eo_master_data.eo_description = getattr(row, "eo_description")

      if 'teh_mesto' in sap_eo_column_list:
        eo_master_data.teh_mesto = getattr(row, "teh_mesto")

      if 'eo_class_code' in sap_eo_column_list:
        eo_master_data.eo_class_code = getattr(row, "eo_class_code")  

        

      if 'gar_no' in sap_eo_column_list:
        eo_master_data.gar_no = getattr(row, "gar_no")

      if 'sap_gar_no' in sap_eo_column_list:
        eo_master_data.sap_gar_no = getattr(row, "sap_gar_no")
      
      if 'head_type' in sap_eo_column_list:
        eo_master_data.head_type = getattr(row, "head_type")  

      if 'eo_model_id' in sap_eo_column_list:
        eo_master_data.eo_model_id = getattr(row, "eo_model_id")

      if 'sap_model_name' in sap_eo_column_list:
        eo_master_data.sap_model_name = getattr(row, "sap_model_name")
      
      if 'sap_maker' in sap_eo_column_list:
        eo_master_data.sap_maker = getattr(row, "sap_maker")
        
      if 'constr_type' in sap_eo_column_list:
        eo_master_data.constr_type = getattr(row, "constr_type")
        
      if 'constr_type_descr' in sap_eo_column_list:
        eo_master_data.constr_type_descr = getattr(row, "constr_type_descr")
      
      operation_start_date = eo_master_data.operation_start_date
      if 'operation_start_date' in sap_eo_column_list:
        operation_start_date_raw = getattr(row, "operation_start_date")
        operation_start_date = read_date(operation_start_date_raw, eo_code_excel)
        eo_master_data.operation_start_date = operation_start_date

      # пишем расчетное значение даты завершения эксплуатации
      expected_operation_period_years = eo_master_data.expected_operation_period_years
      if 'expected_operation_period_years' in sap_eo_column_list:
        expected_operation_period_years = getattr(row, "expected_operation_period_years")
        
      calculated_operation_finish_date = calculate_operation_finish_date(operation_start_date, expected_operation_period_years, eo_code_excel)
      eo_master_data.expected_operation_finish_date = calculated_operation_finish_date
      today_datetime = datetime.now(pytz.timezone('Europe/Moscow'))
      # today_datetime = pd.to_datetime(today_datetime)
      eo_master_data.expected_operation_status_code_date = today_datetime

      
      sap_system_status = eo_master_data.sap_system_status
      if 'sap_system_status' in sap_eo_column_list:
        sap_system_status = getattr(row, "sap_system_status")
        eo_master_data.sap_system_status = sap_system_status

      sap_user_status = eo_master_data.sap_user_status
      if 'sap_user_status' in sap_eo_column_list:
        sap_user_status = getattr(row, "sap_user_status")
        eo_master_data.sap_user_status = sap_user_status

      sap_planned_finish_operation_date = eo_master_data.sap_planned_finish_operation_date
      if 'sap_planned_finish_operation_date' in sap_eo_column_list:
        sap_planned_finish_operation_date_raw = getattr(row, "sap_planned_finish_operation_date")
        sap_planned_finish_operation_datetime = read_date(sap_planned_finish_operation_date_raw, eo_code_excel)
        
        if sap_planned_finish_operation_datetime != datetime.strptime('1.1.2199', '%d.%m.%Y'):
          # print(eo_code_excel, "sap_planned_finish_operation_datetime: ", sap_planned_finish_operation_datetime)
          eo_master_data.sap_planned_finish_operation_date = sap_planned_finish_operation_datetime

      # expected_operation_status_code
      expected_operation_status = expected_operation_status_code(eo_code_excel, operation_start_date, calculated_operation_finish_date, sap_planned_finish_operation_date, today_datetime)
      eo_master_data.expected_operation_status_code = expected_operation_status
      
      db.session.commit()
    
    db.session.commit()

    # сверяемся с файлом кандидатов на добавление.
    add_candidate_record  = Eo_candidatesDB.query.filter_by(eo_code = eo_code_excel).first()
    if add_candidate_record:
      # удаляем запись из таблицы add_candidate_record
      db.session.delete(add_candidate_record)
      log_data_new_record = LogsDB(log_text = f"Добавлена запись из списка кандидатов на добавление. eo_code: {eo_code_excel}", 	log_status = "new")
      db.session.add(log_data_new_record)
      db.session.commit()

    
    # сверяемся с файлом конфликтов.
    # по полю "Гаражный номер"
    # ищем запись в таблице конфликтов
    # potencial_gar_no_conflict = Eo_data_conflicts.query.filter_by(eo_code = eo_code_excel, eo_conflict_field = "gar_no").first()
    # если запись находим
    # if potencial_gar_no_conflict:
    #   # обновляем запись в поле eo_conflict_field_current_master_data
    #   potencial_gar_no_conflict.eo_conflict_field_current_master_data = str(getattr(row, "gar_no"))
    #   db.session.commit()
    #   # проверяем на ситуацию в конфликте после внесения изменений
    #   if str(potencial_gar_no_conflict.eo_conflict_field_current_master_data) == (potencial_gar_no_conflict.eo_conflict_field_uploaded_data):
    #     potencial_gar_no_conflict.eo_conflict_status = "resolved"
    #     log_data_new_record = LogsDB(log_text = f"Разрешен конфликт с гаражным номером в eo_code ({eo_code_excel}). Текущее значение гаражного номера в мастер-данных: {eo_master_data.gar_no}", 	log_status = "new")
    #     db.session.add(log_data_new_record)
        
    #     db.session.commit()
 
      

