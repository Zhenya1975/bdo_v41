import csv
from models.models import Models_DB
from extensions import extensions
from app import app

db = extensions.db


def update_model_data():
  with app.app_context():
    with open('temp_data/model_data.csv', encoding='utf8') as csvfile:
      sap_model_data = csv.reader(csvfile)
      header = next(sap_model_data)
      for row in sap_model_data:
        eo_model_id = row[0]
        eo_model_name = row[1]
        actual_model_data = Models_DB.query.filter_by(eo_model_id=eo_model_id).first()
        if actual_model_data:
          actual_model_data.eo_model_name = eo_model_name
        else:
          model_record = Models_DB(eo_model_id=eo_model_id, eo_model_name = eo_model_name)
          db.session.add(model_record)

        try:
          db.session.commit()
        except Exception as e:
          print("Не получилось добавить или обновить запись в eo_model_id: ", eo_model_id, " Ошибка: ", e)
          db.session.rollback()
          
      models_data = Models_DB.query.all()
      print("кол-во моделей в базе: ", len(models_data))
