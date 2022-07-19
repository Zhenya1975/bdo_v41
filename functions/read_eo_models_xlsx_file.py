import pandas as pd
from extensions import extensions
from models.models import Models_DB
from initial_values.initial_values import be_data_columns_to_master_columns
from datetime import datetime
import sqlite3
db = extensions.db

be_data_columns_to_master_columns = be_data_columns_to_master_columns


def read_eo_models_xlsx():
  """
  Итерируемся по загруженному файлу с моделями. \n
  Обновляем ранее созданные записи или создаем новые, если не находим в мастер-таблице
  """
  # with app.app_context():
  model_eo_raw_data = pd.read_excel('uploads/eo_models.xlsx', index_col = False)
  con = sqlite3.connect("database/datab.db")
  cursor = con.cursor()
  # итерируемся по полученному файлу
  for row in model_eo_raw_data.itertuples():
    id = getattr(row, 'id')
    eo_model_id = getattr(row, 'eo_model_id')
    eo_model_name = getattr(row, 'eo_model_name')
    eo_category_spec = getattr(row, 'eo_category_spec')
    marka_oborudovania = getattr(row, 'marka_oborudovania')
    type_tehniki = getattr(row, 'type_tehniki')
    cost_center = getattr(row, 'cost_center')

    if eo_category_spec !='nan':
      # проверяем есть ли запись
      eo_model_record = Models_DB.query.filter_by(id=id).first()
      if eo_model_record:
        eo_model_record.eo_model_id = eo_model_id
        eo_model_record.eo_model_name = eo_model_name
        eo_model_record.eo_category_spec = eo_category_spec
        eo_model_record.marka_oborudovania = marka_oborudovania
        eo_model_record.type_tehniki = type_tehniki
        eo_model_record.cost_center = cost_center
        db.session.commit()
      else:
        new_model_record = Models_DB(eo_model_id = eo_model_id, eo_model_name = eo_model_name, eo_category_spec = eo_category_spec, marka_oborudovania = marka_oborudovania, type_tehniki = type_tehniki, cost_center=cost_center)
        db.session.add(new_model_record)
        db.session.commit()
      

    

    