from extensions import extensions
from models.models import Eo_DB
from app import app

db = extensions.db

eo_to_delete = ['100000073790145899']

def delete_from_eo_master_data():
  with app.app_context():
    for eo in eo_to_delete:
      eo_master_data=Eo_DB.query.filter_by(eo_code=eo).first()
      db.session.delete(eo_master_data)
      db.session.commit()
      print('из мастер файла удалена запись с ео: ', eo)
      