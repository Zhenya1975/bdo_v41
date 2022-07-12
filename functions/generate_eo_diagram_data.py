import pandas as pd
from extensions import extensions
from initial_values.initial_values import sap_system_status_ban_list, sap_user_status_ban_list, master_data_to_ru_columns
from datetime import datetime
# from app import app
import sqlite3
# from app import app
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl

db = extensions.db

def generate_eo_diagram_data():
  """
  Основная цель - сборка result_diagram_data_df. \n
  Итерируемся по словарю year_dict. \n
  Для каждого года из списка: \n
    - создается выборка из датафрема данных, полученных из мастер-файла. В выборку попадают записи, которые сейчас находятся в   эксплуатации.\n
  Из выборки готовится временный датафрейм с записями с текущим годом и единичкой в поле qty_by_end_of_year и age \n
  - создается выборка с записями, которые вошли в эксплуатацию в текущем году. Эта выборка мерджится справа от предыдущей. \n
  - создается выборка с записями, которые вышли их эксплуатации в текущем году. Эта выборка мерджится справа от предыдущей. \n
  результирующий датафрейм конкатинируется снизу к предыдущему году.\n
  """
  # with app.app_context():
  con = sqlite3.connect("database/datab.db")
  # sql = "SELECT * FROM eo_DB JOIN be_DB"
  sql = "SELECT \
  eo_DB.be_code, \
  be_DB.be_description, \
  eo_DB.eo_class_code, \
  eo_class_DB.eo_class_description, \
  models_DB.eo_model_name, \
  models_DB.eo_category_spec, \
  models_DB.type_tehniki, \
  models_DB.marka_oborudovania, \
  eo_DB.eo_model_id, \
  eo_DB.sap_model_name, \
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
  eo_DB.operation_finish_date_sap_upd, \
  eo_DB.expected_operation_status_code, \
  eo_DB.sap_system_status, \
  eo_DB.sap_user_status, \
  eo_DB.reported_operation_finish_date, \
  eo_DB.reported_operation_status, \
  eo_DB.reported_operation_status_date, \
  eo_DB.evaluated_operation_finish_date \
  FROM eo_DB \
  LEFT JOIN models_DB ON eo_DB.eo_model_id = models_DB.eo_model_id \
  LEFT JOIN be_DB ON eo_DB.be_code = be_DB.be_code \
  LEFT JOIN eo_class_DB ON eo_DB.eo_class_code = eo_class_DB.eo_class_code \
  LEFT JOIN operation_statusDB ON eo_DB.expected_operation_status_code = operation_statusDB.operation_status_code"


    
  master_eo_df = pd.read_sql_query(sql, con)
  master_eo_df.sort_values(['be_code','teh_mesto'], inplace=True)
  date_time_plug = '31/12/2099 23:59:59'
  date_time_plug = datetime.strptime(date_time_plug, '%d/%m/%Y %H:%M:%S')
  # excel_master_eo_df.to_csv('temp_data/excel_master_eo_df.csv')
  
  master_eo_df['evaluated_operation_finish_date'] = pd.to_datetime(master_eo_df['evaluated_operation_finish_date'])
  master_eo_df['operation_finish_date_sap_upd'] = pd.to_datetime(master_eo_df['operation_finish_date_sap_upd'])
  
  master_eo_df['operation_start_date'] = pd.to_datetime(master_eo_df['operation_start_date'])

  # year_dict = {2022:'31.12.2022', 2023:'31.12.2023', 2024:'31.12.2024', 2025:'31.12.2025', 2026:'31.12.2026', 2027:'31.12.2027', 2028:'31.12.2028', 2029:'31.12.2029', 2030:'31.12.2030', 2031:'31.12.2031'}
  year_dict = {2022:{'period_start':'01.01.2022', 'period_end':'31.12.2022'}, 
              2023:{'period_start':'01.01.2023', 'period_end':'31.12.2023'},
               2024:{'period_start':'01.01.2024', 'period_end':'31.12.2024'},
               2025:{'period_start':'01.01.2025', 'period_end':'31.12.2025'},
               2026:{'period_start':'01.01.2026', 'period_end':'31.12.2026'},
               2027:{'period_start':'01.01.2027', 'period_end':'31.12.2027'},
               2028:{'period_start':'01.01.2028', 'period_end':'31.12.2028'},
               2029:{'period_start':'01.01.2029', 'period_end':'31.12.2029'},
               2030:{'period_start':'01.01.2030', 'period_end':'31.12.2030'},
               2031:{'period_start':'01.01.2031', 'period_end':'31.12.2031'},
              }
  eo_diagram_data_df = pd.DataFrame()
  # eo_diagram_data_df['eo_code'] = master_eo_df['eo_code']
  result_diagram_data_df = pd.DataFrame()

  for year, year_data in year_dict.items():
    print(year)
    year_first_date = datetime.strptime(year_data['period_start'], '%d.%m.%Y')
    year_last_date = datetime.strptime(year_data['period_end'], '%d.%m.%Y')

    
    eo_diagram_data_df = master_eo_df.loc[:, ['eo_code', 'eo_description', 'be_code', 'head_type', 'be_description', 'eo_class_code', 'eo_class_description', 'eo_category_spec', 'maker', 'eo_model_name', 'type_tehniki','marka_oborudovania', 'operation_start_date', 'evaluated_operation_finish_date', 'sap_system_status', 'sap_user_status', 'operation_finish_date_sap_upd']]
    
    eo_diagram_data_df['evaluated_operation_finish_date'] = eo_diagram_data_df['operation_finish_date_sap_upd']
    eo_diagram_data_df = master_eo_df.loc[:, ['eo_code', 'eo_description', 'be_code', 'head_type', 'be_description', 'eo_class_code', 'eo_class_description', 'eo_category_spec', 'maker', 'eo_model_name', 'type_tehniki','marka_oborudovania', 'operation_start_date', 'evaluated_operation_finish_date', 'sap_system_status', 'sap_user_status']]
    
    master_eo_df_temp = master_eo_df.loc[:, ['eo_code', 'operation_start_date', 'evaluated_operation_finish_date', 'sap_system_status', 'sap_user_status']]
    master_eo_df_temp= master_eo_df_temp.loc[~master_eo_df_temp['sap_system_status'].isin(sap_system_status_ban_list)]
    master_eo_df_temp = master_eo_df_temp.loc[~master_eo_df_temp['sap_user_status'].isin(sap_user_status_ban_list)]
    

    # выборка в которую попало то что находится в эксплуатации
    # дата начала эксплуатации раньше последнего дня года
    eo_master_current_year_df = master_eo_df_temp.loc[master_eo_df_temp['operation_start_date'] <= year_last_date] 
    # дата завершения больше даты начала года
    eo_master_current_year_df = eo_master_current_year_df.loc[eo_master_current_year_df['evaluated_operation_finish_date'] >= year_first_date]
    eo_master_current_year_df['year'] = year

    
    # считаем количество в эксплуатации
    full_year_period = (year_last_date - year_first_date).total_seconds()

    # если дата начала эксплуатации раньше начала года и дата завершения меньше или равно даты конца года
    eo_master_temp_df_1 =  eo_master_current_year_df.loc[eo_master_current_year_df['operation_start_date'] < year_first_date]
    eo_master_temp_df_1 = eo_master_temp_df_1.loc[eo_master_temp_df_1['evaluated_operation_finish_date']>=year_first_date]
    # выборка строк с позициями, которые завершили эксплуатацию в текущем году
    eo_master_temp_df_1 = eo_master_temp_df_1.loc[eo_master_temp_df_1['evaluated_operation_finish_date'] <= year_last_date]
    # период эксплуатации в текущем году
    eo_master_temp_df_1['operation_period_in_current_year'] = (eo_master_temp_df_1['evaluated_operation_finish_date'] - year_first_date).astype('timedelta64[s]')
    # среднесписочное количество в текущем году это отношение фактического периода к полному периоду
    eo_master_temp_df_1['avg_year_qty'] = eo_master_temp_df_1['operation_period_in_current_year'] / full_year_period
    indexes = list(eo_master_temp_df_1.index.values)
    eo_master_current_year_df.loc[indexes, ['avg_year_qty']] = eo_master_temp_df_1.loc[indexes, ['avg_year_qty']]
    # кол-во на конец года
    eo_master_current_year_df.loc[indexes, ['qty_by_end_of_year']] = 0
  
    # временный статус для фильтрации строк, которые участвуют в текущем году
    eo_master_current_year_df.loc[indexes, ['current_year_operation_status']] = 1
    # возраст
    eo_master_temp_df_1['age'] = (eo_master_temp_df_1['evaluated_operation_finish_date']- eo_master_temp_df_1['operation_start_date']).dt.days / 365.25
    eo_master_current_year_df.loc[indexes, ['age']] = eo_master_temp_df_1.loc[indexes, ['age']]

    # если дата начала эксплуатации позже первого дня года, но дата финиша раньше последнего дня года      
    eo_master_temp_df_1 =  eo_master_current_year_df.loc[eo_master_current_year_df['operation_start_date'] >= year_first_date]
    eo_master_temp_df_1 = eo_master_temp_df_1.loc[eo_master_temp_df_1['evaluated_operation_finish_date']>=year_first_date]
    # выборка строк с позициями, которые завершили эксплуатацию в текущем году
    eo_master_temp_df_1 = eo_master_temp_df_1.loc[eo_master_temp_df_1['evaluated_operation_finish_date'] <= year_last_date]
    # период эксплуатации в текущем году
    eo_master_temp_df_1['operation_period_in_current_year'] = (eo_master_temp_df_1['evaluated_operation_finish_date'] - eo_master_temp_df_1['operation_start_date']).astype('timedelta64[s]')
    # среднесписочное количество в текущем году это отношение фактического периода к полному периоду
    eo_master_temp_df_1['avg_year_qty'] = eo_master_temp_df_1['operation_period_in_current_year'] / full_year_period
    indexes = list(eo_master_temp_df_1.index.values)
    eo_master_current_year_df.loc[indexes, ['avg_year_qty']] = eo_master_temp_df_1.loc[indexes, ['avg_year_qty']]

    
    # временный статус для фильтрации строк, которые участвуют в текущем году
    eo_master_current_year_df.loc[indexes, ['current_year_operation_status']] = 1
    # возраст
    eo_master_temp_df_1['age'] = (eo_master_temp_df_1['evaluated_operation_finish_date']- eo_master_temp_df_1['operation_start_date']).dt.days / 365.25
    eo_master_current_year_df.loc[indexes, ['age']] = eo_master_temp_df_1.loc[indexes, ['age']]
    eo_master_current_year_df.loc[indexes, ['qty_by_end_of_year']] = 0
  
      
    # если дата начала эксплуатации позже первого дня года, но раньше последнего дня года. Дата финиша позже последнего дня года
    eo_master_temp_df_1 =  eo_master_current_year_df.loc[eo_master_current_year_df['operation_start_date'] >= year_first_date]
    eo_master_temp_df_1 = eo_master_temp_df_1.loc[eo_master_temp_df_1['operation_start_date'] <= year_last_date]
    eo_master_temp_df_1 = eo_master_temp_df_1.loc[eo_master_temp_df_1['evaluated_operation_finish_date'] > year_last_date]
    # период эксплуатации в текущем году
    eo_master_temp_df_1['operation_period_in_current_year'] = (year_last_date - eo_master_temp_df_1['operation_start_date']).astype('timedelta64[s]')
    # среднесписочное количество в текущем году это отношение фактического периода к полному периоду
    eo_master_temp_df_1['avg_year_qty'] = eo_master_temp_df_1['operation_period_in_current_year'] / full_year_period
    indexes = list(eo_master_temp_df_1.index.values)
    eo_master_current_year_df.loc[indexes, ['avg_year_qty']] = eo_master_temp_df_1.loc[indexes, ['avg_year_qty']]
    # кол-во на конец года
    eo_master_temp_df_1['qty_by_end_of_year'] = 1
    
    # временный статус для фильтрации строк, которые участвуют в текущем году
    eo_master_current_year_df.loc[indexes, ['current_year_operation_status']] = 1
    # возраст
    eo_master_temp_df_1['age'] = (year_last_date - eo_master_temp_df_1['operation_start_date']).dt.days / 365.25
    eo_master_current_year_df.loc[indexes, ['age']] = eo_master_temp_df_1.loc[indexes, ['age']]
    eo_master_current_year_df.loc[indexes, ['qty_by_end_of_year']] = 1
    
    # если эксплуатируется полный год
    eo_master_temp_df_1 =  eo_master_current_year_df.loc[eo_master_current_year_df['operation_start_date'] < year_first_date]
    eo_master_temp_df_1 = eo_master_temp_df_1.loc[eo_master_temp_df_1['evaluated_operation_finish_date'] > year_last_date]
    eo_master_temp_df_1['avg_year_qty'] =1 
    eo_master_temp_df_1['qty_by_end_of_year'] = 1
    indexes = list(eo_master_temp_df_1.index.values)
    eo_master_current_year_df.loc[indexes, ['qty_by_end_of_year']] = eo_master_temp_df_1.loc[indexes, ['qty_by_end_of_year']] 
    # временный статус для фильтрации строк, которые участвуют в текущем году
    eo_master_current_year_df.loc[indexes, ['current_year_operation_status']] = 1
    # возраст
    eo_master_temp_df_1['age'] = (year_last_date - eo_master_temp_df_1['operation_start_date']).dt.days / 365.25
    eo_master_current_year_df.loc[indexes, ['age']] = eo_master_temp_df_1.loc[indexes, ['age']]
    eo_master_current_year_df.loc[indexes, ['qty_by_end_of_year']] = 1
    eo_master_current_year_df.loc[indexes, ['avg_year_qty']] = 1

    # заполняем пустые ячейки нулями 
    # eo_master_current_year_df['qty_by_end_of_year'].fillna(0, inplace = True)

    eo_master_current_year_df = eo_master_current_year_df.loc[:, ['eo_code', 'age', 'qty_by_end_of_year', 'avg_year_qty', 'current_year_operation_status']]
    
    # eo_master_temp_df.astype({"year": int, "qty_by_end_of_year": int})
    eo_diagram_data_df = pd.merge(eo_diagram_data_df, eo_master_current_year_df, on='eo_code', how='left')

    
    # выборка в которой в указанный период было поступление
    eo_master_temp_in_df = master_eo_df.loc[master_eo_df['operation_start_date'] >= year_first_date]
    eo_master_temp_in_df = eo_master_temp_in_df.loc[eo_master_temp_in_df['operation_start_date'] <= year_last_date]

    # eo_master_temp_in_df['operation_period_in_current_year'] = (year_last_date - eo_master_temp_in_df['operation_start_date']).astype('timedelta64[s]')
    # количество в текущем году это отношение фактического периода к полному периоду
    # eo_master_temp_in_df['qty_in'] = eo_master_temp_in_df['operation_period_in_current_year'] / full_year_period
    eo_master_temp_in_df['qty_in'] = 1
    indexes = list(eo_master_temp_in_df.index.values)
    
    eo_master_temp_in_df = eo_master_temp_in_df.loc[:, ['eo_code', 'qty_in']]
    
    eo_diagram_data_df = pd.merge(eo_diagram_data_df, eo_master_temp_in_df, on='eo_code', how='left')
    
    # выборка в которой в указанный период было выбытие
    eo_master_temp_out_df = master_eo_df.loc[master_eo_df['evaluated_operation_finish_date'] >= year_first_date] 
    eo_master_temp_out_df = eo_master_temp_out_df.loc[eo_master_temp_out_df['evaluated_operation_finish_date']<=year_last_date]
    # eo_master_temp_out_df['operation_period_out_current_year'] = (year_last_date - eo_master_temp_out_df['evaluated_operation_finish_date']).astype('timedelta64[s]')
    # количество в текущем году это отношение фактического периода к полному периоду
    # eo_master_temp_out_df['qty_out'] = (eo_master_temp_out_df['operation_period_out_current_year'] / full_year_period)*-1
    eo_master_temp_out_df['qty_out'] = -1
    indexes = list(eo_master_temp_out_df.index.values)
    eo_master_temp_out_df = eo_master_temp_out_df.loc[:, ['eo_code', 'qty_out']]
    eo_diagram_data_df = pd.merge(eo_diagram_data_df, eo_master_temp_out_df, on='eo_code', how='left')

    
    eo_diagram_data_df_temp_year = eo_diagram_data_df.loc[eo_diagram_data_df['current_year_operation_status']==1]
    indexes = list(eo_diagram_data_df_temp_year.index.values)
    eo_diagram_data_df.loc[indexes, ['year']] = year
    
    eo_diagram_data_df['year'].fillna(0, inplace = True)
    eo_diagram_data_df = eo_diagram_data_df.loc[eo_diagram_data_df['year'] !=0]
    eo_diagram_data_df['qty_by_end_of_year'].fillna(0, inplace = True)
    eo_diagram_data_df['avg_year_qty'].fillna(0, inplace = True)
    eo_diagram_data_df['age'].fillna(0, inplace = True)
    eo_diagram_data_df['qty_in'].fillna(0, inplace = True)
    eo_diagram_data_df['qty_out'].fillna(0, inplace = True)

    result_diagram_data_df = pd.concat([result_diagram_data_df, eo_diagram_data_df], ignore_index=True)

    
    # result_diagram_data_df.to_csv('temp_data/result_diagram_data_df.csv')
  
  result_diagram_data_df["operation_start_date"] = result_diagram_data_df["operation_start_date"].dt.strftime("%d.%m.%Y")  
  result_diagram_data_df["evaluated_operation_finish_date"] = result_diagram_data_df["evaluated_operation_finish_date"].dt.strftime("%d.%m.%Y")  
  result_diagram_data_df.to_csv('downloads/eo_calendar_data_v2.csv')
   
  result_diagram_data_df = result_diagram_data_df.rename(columns=master_data_to_ru_columns)
  # iterations_df.to_csv('temp_data/iterations_df.csv')
  # iterations_df.to_excel('temp_data/iterations_df.xlsx')
  wb = openpyxl.Workbook(write_only=True)
  ws = wb.create_sheet("Sheet1")
  
  i=0
  result_diagram_data_df.reset_index()
  lenght = len(result_diagram_data_df)
  print(len(result_diagram_data_df))
  
  for r in dataframe_to_rows(result_diagram_data_df, index=True):
    i=i+1
    print(i, " из ", lenght)
    ws.append(r)

  wb.save("temp_data/result_diagram_data_df.xlsx")
  # result_diagram_data_df.to_excel('downloads/eo_calendar_data_v2.xlsx')