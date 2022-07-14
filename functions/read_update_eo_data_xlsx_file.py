import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB
from initial_values.initial_values import be_data_columns_to_master_columns
from datetime import datetime
# from app import app


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

def read_update_eo_data_xlsx():
  """
  Итерируемся по загруженному файлу uploads/update_eo_data.xlsx \n
  Проверем есть ли искомая колонка в загруженном файле. \n
  Если есть, то обновляем значение в соответствующей колонке в мастер данных
  """
  # with app.app_context():
  # читаем excel с данными из бизнес-единиц. Проверяем - если нет нужного листа с данными, то отдаем ошибку
  update_eo_data = pd.DataFrame()
  try:
    update_eo_data_df = pd.read_excel('uploads/update_eo_data.xlsx', index_col = False)
    update_eo_column_list = list(update_eo_data_df.columns)

  except Exception as e:
    print("не удалось прочитать файл uploads/update_eo_data.xlsx. Ошибка: ", e)
    log_data_new_record = LogsDB(log_text = f"не удалось прочитать файл uploads/update_eo_data.xlsx. Ошибка: , {e})", log_status = "new")
    db.session.add(log_data_new_record)
  
    ################################################ чтение загруженного файла ###############################################
  if 'eo_description' in update_eo_column_list:
    update_eo_data_df['eo_description'].fillna('plug', inplace = True)
  if 'be_code' in update_eo_column_list:
    update_eo_data_df['be_code'].fillna('plug', inplace = True)
  if 'temp_eo_code' in update_eo_column_list:
    update_eo_data_df['temp_eo_code'].fillna('plug', inplace = True)
  if 'temp_eo_code_status' in update_eo_column_list:
    update_eo_data_df['temp_eo_code_status'].fillna('plug', inplace = True)

    
    
  if 'eo_class_code' in update_eo_column_list:
    update_eo_data_df['eo_class_code'].fillna('plug', inplace = True)
  if 'maker' in update_eo_column_list:
    update_eo_data_df['maker'].fillna('plug', inplace = True)
  if 'gar_no' in update_eo_column_list:
    update_eo_data_df['gar_no'].fillna('plug', inplace = True)
  if 'head_type' in update_eo_column_list:
    update_eo_data_df['head_type'].fillna('plug', inplace = True)
  if 'eo_model_id' in update_eo_column_list:
    update_eo_data_df['eo_model_id'].fillna(0, inplace = True)
  i=0
  lenght = len(update_eo_data)
  # print(update_eo_data_df)
  for row in update_eo_data_df.itertuples():
    eo_code = str(getattr(row, 'eo_code'))
    # проверяем, что запись есть
    eo_master_data=Eo_DB.query.filter_by(eo_code=eo_code).first()
    if eo_master_data:
      if 'eo_description' in update_eo_column_list:
        eo_description = getattr(row, 'eo_description')
        if eo_description != 'plug':
          eo_master_data.eo_description = eo_description
          db.session.commit()
      if 'be_code' in update_eo_column_list:
        be_code = getattr(row, 'be_code')
        if be_code != 'plug':
          eo_master_data.be_code = be_code
          db.session.commit()
      if 'temp_eo_code' in update_eo_column_list:
        temp_eo_code = getattr(row, 'temp_eo_code')
        if temp_eo_code != 'plug':
          eo_master_data.temp_eo_code = temp_eo_code
          db.session.commit()   
      if 'temp_eo_code_status' in update_eo_column_list:
        temp_eo_code_status = getattr(row, 'temp_eo_code_status')
        if temp_eo_code_status != 'plug':
          eo_master_data.temp_eo_code_status = temp_eo_code_status
          db.session.commit() 
          
      if 'eo_class_code' in update_eo_column_list:
        eo_class_code = getattr(row, 'eo_class_code')
        if eo_class_code != 'plug':
          eo_master_data.eo_class_code = eo_class_code
          db.session.commit()    
      if 'gar_no' in update_eo_column_list:
        gar_no = getattr(row, 'gar_no')
        if gar_no != 'plug':
          eo_master_data.gar_no = gar_no
          db.session.commit()
      if 'maker' in update_eo_column_list:
        maker = getattr(row, 'maker')
        if maker != 'plug':
          eo_master_data.maker = maker
          db.session.commit()       
      if 'head_type' in update_eo_column_list:
        head_type = getattr(row, 'head_type')
        if head_type != 'plug':
          eo_master_data.head_type = head_type
          db.session.commit()     
      if 'eo_model_id' in update_eo_column_list:
        eo_model_id = getattr(row, 'eo_model_id')
        if eo_model_id != 0:
          eo_master_data.eo_model_id = eo_model_id
          db.session.commit()
      if 'operation_start_date' in update_eo_column_list:
        operation_start_date_raw = getattr(row, "operation_start_date")
        operation_start_datetime = read_date(operation_start_date_raw, eo_code)
        if operation_start_datetime != datetime.strptime('1.1.2199', '%d.%m.%Y'):
          eo_master_data.operation_start_date = operation_start_datetime
          db.session.commit()    
      if 'expected_operation_period_years' in update_eo_column_list:
        expected_operation_period_years = getattr(row, 'expected_operation_period_years')
        if expected_operation_period_years != 0:
          eo_master_data.expected_operation_period_years = expected_operation_period_years
          db.session.commit()
      
      
      if 'expected_operation_finish_date' in update_eo_column_list:
        expected_operation_finish_date_raw = getattr(row, "expected_operation_finish_date")
        expected_operation_finish_datetime = read_date(expected_operation_finish_date_raw, eo_code)
        if expected_operation_finish_datetime != datetime.strptime('1.1.2199', '%d.%m.%Y'):
          eo_master_data.expected_operation_finish_date = expected_operation_finish_datetime
          db.session.commit() 

      if 'type_mironov' in update_eo_column_list:
        type_mironov = getattr(row, 'type_mironov')
        eo_master_data.type_mironov = type_mironov
        db.session.commit()

      if 'prodlenie_2022' in update_eo_column_list:
        prodlenie_2022 = getattr(row, 'prodlenie_2022')
        eo_master_data.prodlenie_2022 = prodlenie_2022
        db.session.commit()  

        

      if 'short_description_mironov' in update_eo_column_list:
        short_description_mironov = getattr(row, 'short_description_mironov')
        eo_master_data.short_description_mironov = short_description_mironov
        db.session.commit()

      if 'marka_modeli_mironov' in update_eo_column_list:
        marka_modeli_mironov = getattr(row, 'marka_modeli_mironov')
        eo_master_data.marka_modeli_mironov = marka_modeli_mironov
        db.session.commit()  

      if 'marka_oborudovania_mironov' in update_eo_column_list:
        marka_oborudovania_mironov = getattr(row, 'marka_oborudovania_mironov')
        eo_master_data.marka_oborudovania_mironov = marka_oborudovania_mironov
        db.session.commit()   
      

    else:
      log_data_new_record = LogsDB(log_text = f"В мастер-данных нет записи с eo_code: {eo_code}", log_status = "new")
      db.session.add(log_data_new_record)
      db.session.commit()
  