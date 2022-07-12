import pandas as pd
from extensions import extensions
from models.models import Eo_DB, Be_DB, LogsDB, Eo_data_conflicts
from initial_values.initial_values import sap_columns_to_master_columns
# from app import app

db = extensions.db

def generate_excel_conflicts():
  conflicts_data = Eo_data_conflicts.query.all()
  result_data = []
  for conflict in conflicts_data:
    temp_dict = {}
    temp_dict['id'] = conflict.id
    temp_dict['be_eo_data_row_no'] = conflict.be_eo_data_row_no
    temp_dict['eo_code'] = conflict.eo_code
    temp_dict['eo_conflict_field'] = conflict.eo_conflict_field
    temp_dict['eo_conflict_field_current_master_data'] = conflict.eo_conflict_field_current_master_data
    temp_dict['eo_conflict_field_uploaded_data'] = conflict.eo_conflict_field_uploaded_data
    temp_dict['eo_conflict_description'] = conflict.eo_conflict_description
    temp_dict['eo_conflict_date'] = conflict.eo_conflict_date
    temp_dict['filename'] = conflict.filename
    temp_dict['sender_email'] = conflict.sender_email
    temp_dict['email_date'] = conflict.email_date
    temp_dict['eo_conflict_status'] = conflict.eo_conflict_status
    
    
    result_data.append(temp_dict)
  excel_conflicts_df = pd.DataFrame(result_data)
  
  excel_conflicts_df.to_excel('downloads/conflicts.xlsx', index = False)

