import pandas as pd
from models.models import Eo_DB
from extensions import extensions
from app import app
from datetime import datetime

db = extensions.db


def update_sap_eo_data():
  with app.app_context():
    sap_eo_data = pd.read_csv('temp_data/sap_eo_data.csv', dtype = str)
    sap_eo_data['operation_start_date'] = pd.to_datetime(sap_eo_data['operation_start_date'])
    sap_eo_data['expected_operation_finish_date'] = pd.to_datetime(sap_eo_data['expected_operation_finish_date'])
    sap_eo_data['gar_no'].fillna(0, inplace = True) 
    
    date_time_plug = '31/12/2199 23:59:59'
    date_time_plug = datetime.strptime(date_time_plug, '%d/%m/%Y %H:%M:%S')
    sap_eo_data['operation_start_date'].fillna(date_time_plug, inplace = True) 
    sap_eo_data['expected_operation_finish_date'].fillna(date_time_plug, inplace = True)
    sap_eo_data.to_csv('temp_data.csv')
    
    for row in sap_eo_data.itertuples():
      be_code = getattr(row, "be_code")
      eo_code = getattr(row, "eo_code")
      temp_eo_code = getattr(row, "temp_eo_code")
      eo_description = getattr(row, "eo_description")
      teh_mesto = getattr(row, "teh_mesto")
      gar_no = getattr(row, "gar_no")
      head_type = getattr(row, "head_type")
      operation_start_date = getattr(row, "operation_start_date")
      expected_operation_finish_date = getattr(row, "expected_operation_finish_date")
      eo_model_id = getattr(row, "eo_model_id")
      eo_class_code = getattr(row, "eo_class_code")

      actual_eo_data = Eo_DB.query.filter_by(eo_code=eo_code).first()
      # print(actual_eo_data)
      if actual_eo_data != None:
        actual_eo_data.temp_eo_code = temp_eo_code
        actual_eo_data.eo_description = eo_description
        actual_eo_data.be_code = be_code
        actual_eo_data.teh_mesto = teh_mesto
        actual_eo_data.head_type = head_type
        actual_eo_data.gar_no = gar_no
        actual_eo_data.operation_start_date = operation_start_date
        actual_eo_data.expected_operation_finish_date=expected_operation_finish_date
        actual_eo_data.eo_model_id = eo_model_id
        actual_eo_data.eo_class_code = eo_class_code
        
      else:
        # print("в бд нет записи с eo ", eo_code, " добавляем данные")
        eo_record = Eo_DB(be_code = be_code, eo_code=eo_code, temp_eo_code = temp_eo_code, eo_description = eo_description, teh_mesto=teh_mesto, head_type = head_type, gar_no = gar_no, operation_start_date=operation_start_date, expected_operation_finish_date=expected_operation_finish_date, eo_model_id=eo_model_id, eo_class_code=eo_class_code)
        
        db.session.add(eo_record)
      try:
        db.session.commit()
      except Exception as e:
        print("Не получилось добавить или обновить запись в таблице ЕО. eo_code: ", eo_code, " Ошибка: ", e)
        db.session.rollback()
          
    eo_data = Eo_DB.query.all()
    print("кол-во ео в базе: ", len(eo_data))
    return "результат импорта - в принт"


# update_sap_eo_data()