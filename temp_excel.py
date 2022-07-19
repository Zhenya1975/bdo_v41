import pandas as pd
from extensions import extensions
from initial_values.initial_values import be_data_columns_to_master_columns, year_dict
from datetime import datetime
from dateutil.relativedelta import relativedelta
from initial_values.initial_values import sap_user_status_cons_status_list, be_data_cons_status_list, sap_system_status_ban_list, operaton_status_translation, master_data_to_ru_columns, month_dict
import sqlite3
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl


raw_data = pd.read_excel('temp_data/Заказы + МТР 1100 от 19.07.22.XLSX', index_col = False)
vid_zak_list = list(set(raw_data['ВидЗаказа']))
print(vid_zak_list)
