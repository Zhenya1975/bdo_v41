import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB
from initial_values.initial_values import be_data_columns_to_master_columns
from datetime import datetime
from app import app
import sqlite3
from sqlalchemy import create_engine
import os
import pytz
import shortuuid

def create_short_uuid():
  result_df = []
  for i in range(72):
    temp_dict = {}
    
    temp_dict['eo_code'] = shortuuid.uuid()
    result_df.append(temp_dict)
  result_df_df = pd.DataFrame(result_df)
  result_df_df.to_excel('temp_data/result_df.xlsx', index = False) 
# create_short_uuid()  
  
# print(shortuuid.uuid())

# print(pytz.all_timezones)
# today_datetime = datetime.now(pytz.timezone('Europe/Moscow'))


db = extensions.db




def delete_alembic_version_table():
  with app.app_context():
    con = sqlite3.connect("database/datab.db")
    # sql = "SELECT * FROM eo_DB JOIN be_DB"
    # sql = "SELECT eo_DB.be_code, models_DB.eo_model_name  FROM eo_DB JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id"
    cursor = con.cursor()
    drop_table_sql = "DROP TABLE alembic_version"
    cursor.execute(drop_table_sql)
    con.commit()
    cursor.close()

# delete_alembic_version_table()

# delete record from eo

def delete_record():
  with app.app_context():
    con = sqlite3.connect("database/datab.db")
    # sql = "SELECT * FROM eo_DB JOIN be_DB"
    # sql = "SELECT eo_DB.be_code, models_DB.eo_model_name  FROM eo_DB JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id"
    cursor = con.cursor()
    delete_records_sql = "DELETE FROM models_DB WHERE eo_model_id=37;"
    cursor.execute(delete_records_sql)
    con.commit()
    cursor.close()
# delete_record()    

def delete_eo_records():
  eo_to_delete_df = pd.read_excel('temp_data/delete_eo.xlsx', index_col = False, dtype=str)
  for row in eo_to_delete_df.itertuples():
    eo_code = getattr(row, "eo_code")
    with app.app_context():
      con = sqlite3.connect("database/datab.db")
      cursor = con.cursor()
      # delete_records_sql = f"DELETE FROM eo_DB WHERE eo_code={eo_code};"
      delete_records_sql = f"DELETE FROM eo_DB WHERE eo_code is null;"
      cursor.execute(delete_records_sql)
      con.commit()
      cursor.close()
    
    
# delete_eo_records()


def insert_record():
  with app.app_context():
    con = sqlite3.connect("database/datab.db")
    cursor = con.cursor()
    
    # insert_record_sql = "INSERT INTO operation_statusDB (operation_status_code, operation_status_description, sap_operation_status) VALUES ('out_of_operation', 'удалено', 'МТКУ');"
    insert_record_sql = "INSERT INTO eo_class_DB (eo_class_code, eo_class_description) VALUES ('E01_04', 'Тягачи-буксировщики');"
    cursor.execute(insert_record_sql)
    con.commit()
    cursor.close()
  
# insert_record()
def update_records():
  with app.app_context():
    con = sqlite3.connect("database/datab.db")
    cursor = con.cursor()
    category_spec_df = pd.read_csv('temp_data/category_spec.csv')
    for row in category_spec_df.itertuples():
      id = getattr(row, 'id')
      eo_category_spec = getattr(row, 'eo_category_spec')

      update_record_sql = f"UPDATE models_DB SET eo_category_spec='{eo_category_spec}' WHERE id = {id};"
      print(update_record_sql)
      cursor.execute(update_record_sql)
      con.commit()
    cursor.close()

# update_records()





def update_records():
  with app.app_context():
    con = sqlite3.connect("database/datab.db")
    cursor = con.cursor()
    update_record_sql = f"UPDATE eo_DB SET eo_model_id=132 WHERE eo_code = '100000099446';"
    cursor.execute(update_record_sql)
    con.commit()
    cursor.close()
# update_records()

def clear_column_records():
  with app.app_context():
    con = sqlite3.connect("database/datab.db")
    cursor = con.cursor()
    update_record_sql = "UPDATE eo_DB SET reported_operation_finish_date=NULL, reported_operation_status_code = NULL, reported_operation_status_date = NULL, reported_operation_status = NULL;"
    
    cursor.execute(update_record_sql)
    con.commit()
    cursor.close()
    
# clear_column_records()

# UPDATE Table1
# SET    Col1 = NULL
# WHERE  Col2 = 'USA'

# update eo_model_id
def update_eo_class_eo():
  eo_master_data_for_update_df = pd.read_csv('temp_data/eo_master_data for update.csv', dtype = str)
  for row in eo_master_data_for_update_df.itertuples():
    eo_code = getattr(row, 'eo_code')
    eo_class_code = getattr(row, 'eo_class_code')
    
    with app.app_context():
      con = sqlite3.connect("database/datab.db")
      cursor = con.cursor()
      update_record_sql = f"UPDATE eo_DB SET eo_class_code ='{eo_class_code}' WHERE eo_code = '{eo_code}';"
      cursor.execute(update_record_sql)
      con.commit()
      cursor.close()
  
# update_eo_class_eo()    

def update_eo_maker():
  eo_master_data_for_update_df = pd.read_csv('temp_data/eo_master_data for update.csv', dtype = str)
  for row in eo_master_data_for_update_df.itertuples():
    eo_code = getattr(row, 'eo_code')
    sap_maker = getattr(row, 'sap_maker')
    with app.app_context():
      con = sqlite3.connect("database/datab.db")
      cursor = con.cursor()
      update_record_sql = f"UPDATE eo_DB SET sap_maker ='{sap_maker}' WHERE eo_code = '{eo_code}';"
      cursor.execute(update_record_sql)
      con.commit()
      cursor.close()
  
# update_eo_maker()      
    

  
def update_record():
  with app.app_context():
    con = sqlite3.connect("database/datab.db")
    cursor = con.cursor()
    update_record_sql = "UPDATE operation_statusDB SET sap_operation_status='' WHERE id= 4;"
    cursor.execute(update_record_sql)
    con.commit()
    cursor.close()




def update_evaluated_finish_date():
  eo_master_data_for_update_df = pd.read_csv('temp_data/eo_master_data for update.csv', dtype = str)
  for row in eo_master_data_for_update_df.itertuples():
    eo_code = getattr(row, 'eo_code')
    evaluated_operation_finish_date = getattr(row, 'evaluated_operation_finish_date')
    try:
      evaluated_operation_finish_date = datetime.strptime(evaluated_operation_finish_date, '%d.%m.%Y')
    except Exception as e:
      print(eo_code, " ", type(evaluated_operation_finish_date), evaluated_operation_finish_date, e)
      
    with app.app_context():
      con = sqlite3.connect("database/datab.db")
      cursor = con.cursor()
      update_record_sql = f"UPDATE eo_DB SET evaluated_operation_finish_date ='{evaluated_operation_finish_date}' WHERE eo_code = '{eo_code}';"
      # print(update_record_sql)
      try:
        cursor.execute(update_record_sql)
        con.commit()
        cursor.close()
      except Exception as e:
        print(eo_code, "exception: ", e)
  
# update_evaluated_finish_date()    