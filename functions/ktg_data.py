import pandas as pd
from extensions import extensions
from initial_values.initial_values import be_data_columns_to_master_columns, year_dict
from datetime import datetime
from dateutil.relativedelta import relativedelta
from initial_values.initial_values import sap_user_status_cons_status_list, be_data_cons_status_list, sap_system_status_ban_list, operaton_status_translation, master_data_to_ru_columns, month_dict, ktg_data_columns_to_master_columns
import sqlite3
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl

db = extensions.db

plug = 'plug'
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
  # "operation_finish_date_iteration_0":"Дата вывода 0 - итерация продления",
  # "operation_finish_date_iteration_1":"Дата вывода 1 - итерация продления",
  "operation_finish_date_iteration_2":"Дата вывода 2 - итерация продления"
}


def read_ktg_data_xlsx():
  # читаем загруженный файл 
  ktg_raw_data = pd.read_excel('uploads/ktg_data.xlsx', index_col = False)
  ktg_data = ktg_raw_data.rename(columns=ktg_data_columns_to_master_columns)
  ktg_data['period'] = pd.to_datetime(ktg_data['period'], format='%d.%m.%Y')
  ktg_data['eo_code'] = ktg_data['eo_code'].astype(str)

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
  models_DB.cost_center, \
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
  eo_DB.prodlenie_2022, \
  eo_DB.custom_eo_status \
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

  master_eo_df['operation_start_date_month'] = ((master_eo_df['operation_start_date'] + pd.offsets.MonthEnd(0) - pd.offsets.MonthBegin(1)).dt.floor('d'))

  
  master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(master_eo_df['sap_planned_finish_operation_date'])
  master_eo_df['operation_finish_date_sap_upd'] = pd.to_datetime(master_eo_df['operation_finish_date_sap_upd'])
  # джойним данные из файла с мастер-данными
  
  
  ktg_master_data = pd.merge(ktg_data, master_eo_df, on='eo_code', how='left')

  wb = openpyxl.Workbook(write_only=True)
  ws = wb.create_sheet("Sheet1")
  i=0
  
  print("Кол-во записей в ktg_data: ", len(ktg_master_data))
  ktg_master_data = ktg_master_data.rename(columns=master_data_to_ru_columns)
  for r in dataframe_to_rows(ktg_master_data, index=False, header=True):
    i=i+1
    # print(i, " из ", lenght)
    ws.append(r)

  wb.save(f"temp_data/ktg_master_data.xlsx")
  
  