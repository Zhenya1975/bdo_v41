import pandas as pd
from extensions import extensions
import sqlite3

db = extensions.db

def sql_to_model_eo():
  con = sqlite3.connect("database/datab.db")
  # sql = "SELECT * FROM eo_DB JOIN be_DB"
  sql = "SELECT * FROM models_DB"
  
 
  excel_model_eo_df = pd.read_sql_query(sql, con)
  excel_model_eo_df.to_excel('downloads/model_eo.xlsx', index = False) 