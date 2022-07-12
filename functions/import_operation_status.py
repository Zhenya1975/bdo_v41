import csv
from models.models import Operation_statusDB
from extensions import extensions
from app import app

db = extensions.db


def import_operation_status():
  with app.app_context():
    with open('temp_data/operation_status.csv', encoding='utf8') as csvfile:
      operation_status_data = csv.reader(csvfile)
      header = next(operation_status_data)
      for row in operation_status_data:
        operation_status_code = row[0]
        operation_status_description = row[1]
        sap_operation_status = row[2]
        actual_operation_status_data_data = Operation_statusDB.query.filter_by(operation_status_code=operation_status_code).first()
        if actual_operation_status_data_data:
          actual_operation_status_data_data.operation_status_description = operation_status_description
          actual_operation_status_data_data.sap_operation_status = sap_operation_status
        else:
          operation_status_record = Operation_statusDB(operation_status_code=operation_status_code, operation_status_description = operation_status_description, sap_operation_status = sap_operation_status)
          db.session.add(operation_status_record)

        try:
          db.session.commit()
        except Exception as e:
          print("Не получилось добавить или обновить запись в таблице Operation_statusDB. operation_status_code: ", operation_status_code, " Ошибка: ", e)
          db.session.rollback()
          
      operations_status_data = Operation_statusDB.query.all()
      print("кол-во Operation_status в базе: ", len(operations_status_data))
