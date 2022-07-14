import pandas as pd
from extensions import extensions
from initial_values.initial_values import be_data_columns_to_master_columns, year_dict
from datetime import datetime
from initial_values.initial_values import sap_user_status_cons_status_list, be_data_cons_status_list, sap_system_status_ban_list, operaton_status_translation, master_data_to_ru_columns, month_dict
import sqlite3
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl

db = extensions.db

date_time_plug = '1.1.2199'
date_time_plug = datetime.strptime(date_time_plug, '%d.%m.%Y')

# наименование итераций
iterations = {
  "Дата вывода 0 - итерация продления": "iteration_0",
  "Дата вывода 1 - итерация продления": "iteration_1",
  "Дата вывода 1 - итерация продления (корр 23.06.22)": "iteration_1_1",
  "Дата вывода 2 - итерация продления (корр 30.06.22)": "iteration_2"
}
iterations_list = ["operation_finish_date_iteration_0",	"operation_finish_date_iteration_1",	"operation_finish_date_iteration_2"]
iterations_dict = {
  "operation_finish_date_iteration_0":"Дата вывода 0 - итерация продления",
  "operation_finish_date_iteration_1":"Дата вывода 1 - итерация продления",
  "operation_finish_date_iteration_2":"Дата вывода 2 - итерация продления"
}


def read_be_2_eo_xlsx():
  # читаем загруженный файл 
  be_eo_raw_data = pd.read_excel('uploads/be_eo_2_data.xlsx', sheet_name='be_eo_data', index_col = False)
  be_eo_data = be_eo_raw_data.rename(columns=be_data_columns_to_master_columns)

  be_eo_data['eo_code'] = be_eo_data['eo_code'].astype(str)
  be_eo_data['operation_finish_date_iteration_0'] = pd.to_datetime(be_eo_data['operation_finish_date_iteration_0'], format='%d.%m.%Y')
  be_eo_data['operation_finish_date_iteration_1'] = pd.to_datetime(be_eo_data['operation_finish_date_iteration_1'], format='%d.%m.%Y')
  be_eo_data['operation_finish_date_iteration_2'] = pd.to_datetime(be_eo_data['operation_finish_date_iteration_2'], format='%d.%m.%Y')
  be_eo_data['conservation_start_date'] = pd.to_datetime(be_eo_data['conservation_start_date'], format='%d.%m.%Y')

  # получаем данные из мастер-файла
  con = sqlite3.connect("database/datab.db")
  cursor = con.cursor()
  # sql = "SELECT * FROM eo_DB JOIN be_DB"
  sql = "SELECT \
  eo_DB.eo_code, \
  eo_DB.temp_eo_code, \
  eo_DB.temp_eo_code_status, \
  eo_DB.be_code, \
  eo_DB.head_type, \
  be_DB.be_description, \
  eo_DB.eo_class_code, \
  eo_class_DB.eo_class_description, \
  models_DB.eo_model_name, \
  models_DB.eo_category_spec, \
  models_DB.type_tehniki, \
  models_DB.marka_oborudovania, \
  eo_DB.eo_model_id, \
  eo_DB.eo_description, \
  eo_DB.gar_no, \
  eo_DB.constr_type, \
  eo_DB.constr_type_descr, \
  eo_DB.operation_start_date, \
  eo_DB.expected_operation_period_years, \
  eo_DB.operation_finish_date_calc, \
  eo_DB.sap_planned_finish_operation_date, \
  eo_DB.operation_finish_date_sap_upd, \
  eo_DB.sap_system_status, \
  eo_DB.sap_user_status, \
  eo_DB.prodlenie_2022 \
  FROM eo_DB \
  LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
  LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
  LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
  LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code"
  
  master_eo_df = pd.read_sql_query(sql, con)
  # подменяем временнные номера
  master_eo_df_temp_eo = master_eo_df.loc[master_eo_df['temp_eo_code_status']=='temp_eo_code']
  indexes_temp_eo = list(master_eo_df_temp_eo.index.values)
  # print(master_eo_df_temp_eo['temp_eo_code'])
  # print(list(master_eo_df_temp_eo.loc[indexes_temp_eo, ['temp_eo_code']]))  
  master_eo_df.loc[indexes_temp_eo, ['eo_code']] = master_eo_df_temp_eo['temp_eo_code']
  # print(master_eo_df.loc[indexes_temp_eo, ['eo_code']])
  # print('должен быть список', master_eo_df.loc[indexes_temp_eo, ['eo_code']])
  
  master_eo_df = master_eo_df.loc[master_eo_df['head_type']=='head']
  master_eo_df['operation_start_date'] = pd.to_datetime(master_eo_df['operation_start_date'])
  master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(master_eo_df['sap_planned_finish_operation_date'])
  master_eo_df['operation_finish_date_sap_upd'] = pd.to_datetime(master_eo_df['operation_finish_date_sap_upd'])
  # джойним данные из файла с мастер-данными
  
  
  be_master_data = pd.merge(master_eo_df, be_eo_data, on='eo_code', how='left')
  be_master_data['operation_finish_date'] = be_master_data['operation_finish_date_sap_upd']
  be_master_data['operation_finish_date'] = be_master_data['operation_finish_date'].dt.date

  be_master_data['operation_finish_date'] = pd.to_datetime(be_master_data['operation_finish_date'])


  result_data_list = []
  i=0
  lenght = len(be_master_data)
  for row in be_master_data.itertuples():
    i=i+1
    # print("Итерация ", iteration,", ", i, " из ", lenght)
    eo_code = getattr(row, 'eo_code')
    be_code = getattr(row, 'be_code')
    be_description = getattr(row, 'be_description')
    eo_class_code = getattr(row, 'eo_class_code')
    eo_class_description = getattr(row, 'eo_class_description')
    eo_model_name = getattr(row, 'eo_model_name')
    # eo_model_id = getattr(row, 'eo_model_id')
    eo_category_spec = getattr(row, 'eo_category_spec')
    type_tehniki = getattr(row, 'type_tehniki')
    marka_oborudovania = getattr(row, 'marka_oborudovania')
    
    eo_description = getattr(row, "eo_description")
    gar_no = getattr(row, "gar_no")
    constr_type = getattr(row, "constr_type")
    constr_type_descr = getattr(row, "constr_type_descr")
    # operation_status_rus = getattr(row, "operation_status")
    sap_user_status = getattr(row, "sap_user_status")
    sap_system_status = getattr(row, "sap_system_status")
    operation_status_from_file = getattr(row, "operation_status") # статус, полученный из файла

    operation_start_date = getattr(row, 'operation_start_date')
    expected_operation_period_years = getattr(row, 'expected_operation_period_years')
    operation_finish_date_calc = getattr(row, 'operation_finish_date_calc')
    sap_planned_finish_operation_date = getattr(row, 'sap_planned_finish_operation_date')
    operation_finish_date_sap_upd = getattr(row, 'operation_finish_date_sap_upd')
    # operation_finish_date_update_iteration = getattr(row, iteration)
    operation_finish_date = getattr(row, 'operation_finish_date')
    if eo_code == '100000061761':
      print(eo_code)