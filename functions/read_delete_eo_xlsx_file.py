import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB, Eo_calendar_operation_status_DB
from initial_values.initial_values import be_data_columns_to_master_columns
from datetime import datetime
import sqlite3

db = extensions.db

be_data_columns_to_master_columns = be_data_columns_to_master_columns

def read_delete_eo_xlsx():
  try:
    delete_eo_df = pd.read_excel('uploads/delete_eo.xlsx', sheet_name='delete_eo', index_col = False, dtype=str)
  except Exception as e:
    print("не удалось прочитать данные из файла delete_eo.xlsx. Ошибка: ", e)
    log_data_new_record = LogsDB(log_text = f"не удалось прочитать данные из файла delete_eo.xlsx. Ошибка: , {e})", log_status = "new")
    db.session.add(log_data_new_record)
    db.session.commit()
  if len(delete_eo_df) >0:
    for row in delete_eo_df.itertuples():
      eo_code = getattr(row, 'eo_code')
      status = getattr(row, 'status')
      if status == 'delete':
        con = sqlite3.connect("database/datab.db")
        cursor = con.cursor()
        delete_records_sql = f"DELETE FROM eo_DB WHERE eo_code='{eo_code}';"
        cursor.execute(delete_records_sql)
        con.commit()
        log_data_new_record = LogsDB(log_text = f"Удалена запись eo_code: {eo_code}", log_status = "new")
        db.session.add(log_data_new_record)
        db.session.commit()
        # проверка на запись в конфликтах и удаление записей, если они есть
        conflict_records = Eo_data_conflicts.query.filter_by(eo_code = eo_code).all()
        if len(list(conflict_records))>0:
          for conflict in conflict_records:
            conflict_eo_code = conflict.eo_code
            delete_records_sql = f"DELETE FROM eo_data_conflicts WHERE eo_code='{conflict_eo_code}';"
            cursor.execute(delete_records_sql)
            con.commit()
        # проверка на запись в кандидатах на добавление и удаление записей, если они есть   
        add_candidate_records = Eo_candidatesDB.query.filter_by(eo_code = eo_code).all()  
        if len(list(add_candidate_records))>0:
          for add_candidate in add_candidate_records:
            add_candidate_eo_code = add_candidate.eo_code
            delete_records_sql = f"DELETE FROM eo_candidatesDB WHERE eo_code='{add_candidate_eo_code}';"
            cursor.execute(delete_records_sql)
            con.commit()
        calendar_operation_status = Eo_calendar_operation_status_DB.query.filter_by(eo_code = eo_code).all()  
        if len(list(calendar_operation_status))>0:
          for calendar_record in calendar_operation_status:
            calendar_record_eo_code = calendar_record.eo_code
            delete_records_sql = f"DELETE FROM eo_calendar_operation_status_DB WHERE eo_code='{calendar_record_eo_code}';"
            cursor.execute(delete_records_sql)
            con.commit()
        cursor.close()    
      else:
        log_data_new_record = LogsDB(log_text = f"Нет статуса 'delete' в строке с eo_code: {eo_code}. Запись не удалена", log_status = "new")
        db.session.add(log_data_new_record)
        db.session.commit()    
          
      
      
