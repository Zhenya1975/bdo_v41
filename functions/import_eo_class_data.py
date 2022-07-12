import csv
from models.models import Be_DB, Eo_class_DB
from extensions import extensions
from app import app

db = extensions.db


def update_eo_class_data():
  with app.app_context():
    with open('temp_data/eo_class.csv', encoding='utf8') as csvfile:
      eo_class_data = csv.reader(csvfile)
      header = next(eo_class_data)
      for row in eo_class_data:
        eo_class_code = row[0]
        eo_class_description = row[1]
        actual_eo_class_data = Eo_class_DB.query.filter_by(eo_class_code=eo_class_code).first()
        if actual_eo_class_data:
          actual_eo_class_data.eo_class_code = eo_class_code
          actual_eo_class_data.eo_class_description = eo_class_description
        else:
          eo_class_record = Eo_class_DB(eo_class_code=eo_class_code, eo_class_description = eo_class_description)
          db.session.add(eo_class_record)

        try:
          db.session.commit()
        except Exception as e:
          print("Не получилось добавить или обновить запись в таблице eo_class: ", eo_class_code, " Ошибка: ", e)
          db.session.rollback()
          
      eo_class_data = Eo_class_DB.query.all()
      print("кол-во Eo_class в базе: ", len(eo_class_data))
