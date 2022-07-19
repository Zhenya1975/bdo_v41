import pandas as pd
from extensions import extensions
from initial_values.initial_values import be_data_columns_to_master_columns, year_dict
from datetime import datetime
from dateutil.relativedelta import relativedelta
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
  # "operation_finish_date_iteration_0":"Дата вывода 0 - итерация продления",
  # "operation_finish_date_iteration_1":"Дата вывода 1 - итерация продления",
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

  master_eo_df['operation_start_date_month'] = ((master_eo_df['operation_start_date'] + pd.offsets.MonthEnd(0) - pd.offsets.MonthBegin(1)).dt.floor('d'))

  
  master_eo_df['sap_planned_finish_operation_date'] = pd.to_datetime(master_eo_df['sap_planned_finish_operation_date'])
  master_eo_df['operation_finish_date_sap_upd'] = pd.to_datetime(master_eo_df['operation_finish_date_sap_upd'])
  # джойним данные из файла с мастер-данными
  
  
  be_master_data = pd.merge(master_eo_df, be_eo_data, on='eo_code', how='left')
  
  #### обновление даты завершения из колонки Итерации 
  for iteration, iteration_rus in iterations_dict.items():
    be_master_data[iteration] = pd.to_datetime(be_master_data[iteration])
    be_master_data[iteration].fillna(date_time_plug, inplace = True)

    be_master_data['operation_finish_date'] = be_master_data['operation_finish_date_sap_upd']
    be_master_data['operation_finish_date'] = be_master_data['operation_finish_date'].dt.date  
    
    be_master_data['operation_finish_date'] = be_master_data['operation_finish_date_sap_upd']
    be_master_data['operation_finish_date'] = be_master_data['operation_finish_date'].dt.date
    
    be_master_data[iteration] = be_master_data[iteration].dt.date
    
    be_master_data_temp = be_master_data.loc[be_master_data[iteration]!=date_time_plug.date()]

    indexes = list(be_master_data_temp.index.values)
    
    be_master_data.loc[indexes, ['operation_finish_date']] = be_master_data_temp[iteration]

    be_master_data['operation_finish_date'] = pd.to_datetime(be_master_data['operation_finish_date'])

    be_master_data['operation_finish_date_month'] = ((be_master_data['operation_finish_date'] + pd.offsets.MonthEnd(0) - pd.offsets.MonthBegin(1)).dt.floor('d'))

  result_data_list = []
  i=0
  lenght = len(be_master_data)
  for row in be_master_data.itertuples():
    i=i+1
    # print("eo ", ", ", i, " из ", lenght)
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
    cost_center = getattr(row, "cost_center")
    operation_status_from_file = getattr(row, "operation_status") # статус, полученный из файла

    operation_start_date = getattr(row, 'operation_start_date')
    operation_start_date_month = getattr(row, 'operation_start_date_month')
    expected_operation_period_years = getattr(row, 'expected_operation_period_years')
    operation_finish_date_calc = getattr(row, 'operation_finish_date_calc')
    sap_planned_finish_operation_date = getattr(row, 'sap_planned_finish_operation_date')
    operation_finish_date_sap_upd = getattr(row, 'operation_finish_date_sap_upd')
    # operation_finish_date_update_iteration = getattr(row, iteration)
    operation_finish_date = getattr(row, 'operation_finish_date')
    operation_finish_date_month = getattr(row, 'operation_finish_date_month')
    conservation_start_date = getattr(row, 'conservation_start_date')
    
    # сначала определяем статус ввода в эксплуатацию
    status_condition_dict = {
        "new":"Ввод нового",
        "on_balance":"На балансе",
        "conservation":"Консервация",
        "remake":"Переоборудование",
        "out":"План на вывод",
        "in_operation":"Эксплуатация"
      }
    for status_condition, status_condition_rus in status_condition_dict.items():
      
      if status_condition == "on_balance":
        if sap_system_status not in sap_system_status_ban_list:
          time_operation_point_date = operation_start_date
          # первый день в месяце 
          time_operation_point_date = time_operation_point_date.replace(day=1)
          while time_operation_point_date <= operation_finish_date:
            temp_dict = {}
            temp_dict['eo_code'] = eo_code
            temp_dict['be_code'] = be_code
       
            temp_dict['be_description'] = be_description
            temp_dict['eo_class_code'] = eo_class_code
            temp_dict['eo_class_description'] = eo_class_description
            temp_dict['eo_model_name'] = eo_model_name
            temp_dict['eo_category_spec'] = eo_category_spec
            temp_dict['type_tehniki'] = type_tehniki
            temp_dict['marka_oborudovania'] = marka_oborudovania
            temp_dict['cost_center'] = cost_center
            
            temp_dict['eo_description'] = eo_description
            temp_dict['sap_system_status'] = sap_system_status
            temp_dict['sap_user_status'] = sap_user_status 
            temp_dict['operation_start_date'] = operation_start_date
            temp_dict['operation_finish_date'] = operation_finish_date
            temp_dict['operation_status'] = "На балансе"
            temp_dict['qty'] = 1
            temp_dict['На балансе'] = 1
            temp_dict['month_date'] = time_operation_point_date
            result_data_list.append(temp_dict)
            time_operation_point_date = time_operation_point_date + relativedelta(months=1)
      
      if status_condition == "in_operation":
        
        if sap_user_status not in sap_user_status_cons_status_list and \
        operation_status_from_file != "Консервация" and \
        sap_system_status not in sap_system_status_ban_list:
        # operation_start_date <= datetime.strptime('31.12.2023', '%d.%m.%Y') and \
        # operation_finish_date >= datetime.strptime('1.1.2022', '%d.%m.%Y'):
          time_operation_point_date = operation_start_date
          # первый день в месяце 
          time_operation_point_date = time_operation_point_date.replace(day=1)
          # if eo_code == '100000065592':
          while time_operation_point_date <= operation_finish_date:
            temp_dict = {}
            temp_dict['eo_code'] = eo_code
            temp_dict['be_code'] = be_code
            temp_dict['be_description'] = be_description
            temp_dict['eo_class_code'] = eo_class_code
            temp_dict['eo_class_description'] = eo_class_description
            temp_dict['eo_model_name'] = eo_model_name
            temp_dict['eo_category_spec'] = eo_category_spec
            temp_dict['type_tehniki'] = type_tehniki
            temp_dict['marka_oborudovania'] = marka_oborudovania
            temp_dict['cost_center'] = cost_center

            
            temp_dict['eo_description'] = eo_description
            temp_dict['sap_system_status'] = sap_system_status
            temp_dict['sap_user_status'] = sap_user_status 
            temp_dict['operation_start_date'] = operation_start_date
            temp_dict['operation_finish_date'] = operation_finish_date
            temp_dict['operation_status'] = "Эксплуатация"
            temp_dict['qty'] = 1
            temp_dict['Эксплуатация'] = 1
            # print("time_operation_point_date: ", time_operation_point_date)
            # print("eo_code: ", eo_code, "operation_start_date: ", operation_start_date, "operation_finish_date: ", operation_finish_date)
            # print(time_operation_point)
            # if time_operation_point_date >=datetime.strptime('1.1.2022', '%d.%m.%Y'): #and \
            # time_operation_point_date <= datetime.strptime('31.12.2023', '%d.%m.%Y'):
              # print(time_operation_point)
            temp_dict['month_date'] = time_operation_point_date
              # print("time_operation_point_date: ", time_operation_point_date)
              # print("eo_code: ", eo_code, "operation_start_date: ", operation_start_date, "operation_finish_date: ", operation_finish_date)
              # print(temp_dict)
            result_data_list.append(temp_dict)
            time_operation_point_date = time_operation_point_date + relativedelta(months=1)

      if status_condition == "conservation":
        if (sap_user_status in sap_user_status_cons_status_list or \
        operation_status_from_file == "Консервация") and \
        sap_system_status not in sap_system_status_ban_list:
        # operation_start_date <= datetime.strptime('31.12.2023', '%d.%m.%Y') and \
        # operation_finish_date >= datetime.strptime('1.1.2022', '%d.%m.%Y'):
          time_operation_point_date = conservation_start_date
          conservation_finish_date = operation_finish_date
          # первый день в месяце 
          time_operation_point_date = time_operation_point_date.replace(day=1)
          # if eo_code == '100000065592':
          while time_operation_point_date <= conservation_finish_date:
            temp_dict = {}
            temp_dict['eo_code'] = eo_code
            temp_dict['be_code'] = be_code
            temp_dict['be_description'] = be_description
            temp_dict['eo_class_code'] = eo_class_code
            temp_dict['eo_class_description'] = eo_class_description
            temp_dict['eo_model_name'] = eo_model_name
            temp_dict['eo_category_spec'] = eo_category_spec
            temp_dict['type_tehniki'] = type_tehniki
            temp_dict['marka_oborudovania'] = marka_oborudovania
            temp_dict['cost_center'] = cost_center

            
            temp_dict['eo_description'] = eo_description
            temp_dict['sap_system_status'] = sap_system_status
            temp_dict['sap_user_status'] = sap_user_status 
            temp_dict['operation_start_date'] = operation_start_date
            temp_dict['operation_finish_date'] = operation_finish_date
            temp_dict['conservation_start_date'] = conservation_start_date
            temp_dict['conservation_finish_date'] = conservation_finish_date
            temp_dict['operation_status'] = "Консервация"
            temp_dict['qty'] = 1
            temp_dict['Консервация'] = 1
            temp_dict['month_date'] = time_operation_point_date
            result_data_list.append(temp_dict)
            time_operation_point_date = time_operation_point_date + relativedelta(months=1)
            
      if status_condition == "new":
        # проверяем чтобы ео не была в консервации и в удаленных
        if sap_user_status not in sap_user_status_cons_status_list and \
        sap_system_status not in sap_system_status_ban_list:
          temp_dict = {}
          temp_dict['eo_code'] = eo_code
          temp_dict['be_code'] = be_code
          temp_dict['be_description'] = be_description
          temp_dict['eo_class_code'] = eo_class_code
          temp_dict['eo_class_description'] = eo_class_description
          temp_dict['eo_model_name'] = eo_model_name
          temp_dict['eo_category_spec'] = eo_category_spec
          temp_dict['type_tehniki'] = type_tehniki
          temp_dict['marka_oborudovania'] = marka_oborudovania
          temp_dict['cost_center'] = cost_center

          
          temp_dict['eo_description'] = eo_description
          temp_dict['sap_system_status'] = sap_system_status
          temp_dict['sap_user_status'] = sap_user_status          
          temp_dict['operation_start_date'] = operation_start_date
          temp_dict['operation_finish_date'] = operation_finish_date
          temp_dict['month_date'] = operation_start_date_month
          
          temp_dict['operation_status'] = "Ввод нового"
          temp_dict['qty'] = 1
          temp_dict['Ввод нового'] = 1
          
          result_data_list.append(temp_dict)
      
      elif status_condition == "out": 
        if sap_system_status not in sap_system_status_ban_list:
          temp_dict = {}
          temp_dict['eo_code'] = eo_code
          temp_dict['be_code'] = be_code
          temp_dict['be_description'] = be_description
          temp_dict['eo_class_code'] = eo_class_code
          temp_dict['eo_class_description'] = eo_class_description
          temp_dict['eo_model_name'] = eo_model_name
          temp_dict['eo_category_spec'] = eo_category_spec
          temp_dict['type_tehniki'] = type_tehniki
          temp_dict['marka_oborudovania'] = marka_oborudovania
          temp_dict['cost_center'] = cost_center

          
          temp_dict['eo_description'] = eo_description
          temp_dict['sap_system_status'] = sap_system_status
          temp_dict['sap_user_status'] = sap_user_status 
          temp_dict['operation_start_date'] = operation_start_date
          temp_dict['operation_finish_date'] = operation_finish_date
          temp_dict['month_date'] = operation_finish_date_month
          
          temp_dict['operation_status'] = "План на вывод"
          temp_dict['qty'] = -1
          temp_dict['План на вывод'] = -1
          result_data_list.append(temp_dict)  

        

  iter_df_temp = pd.DataFrame(result_data_list) 
  iter_df_temp['eo_count'] = iter_df_temp['eo_code']
  
  # режем результат по началу 22-го года
  iter_df_temp = iter_df_temp.loc[iter_df_temp['month_date']>=datetime.strptime('1.1.2022', '%d.%m.%Y')]
  # iter_df_temp = iter_df_temp.rename(columns=master_data_to_ru_columns)
  iter_df_temp.reset_index()
  
  list_of_be = list(set(iter_df_temp['be_code']))
  # list_of_be = [1100]
  for be_code in list_of_be:
    df = iter_df_temp.loc[iter_df_temp['be_code']==be_code]
    
    # iter_df_temp["month_date"] = iter_df_temp["month_date"].dt.strftime("%d.%m.%Y")
    
    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet("Sheet1")
    
    i=0
    
    print(be_code, ": ", len(df))
    df = df.rename(columns=master_data_to_ru_columns)
    for r in dataframe_to_rows(df, index=False, header=True):
      i=i+1
      # print(i, " из ", lenght)
      ws.append(r)
  
    wb.save(f"temp_data/df_{be_code}.xlsx")
    
  
      # if eo_code == '100000061761':
      #   print(eo_code)