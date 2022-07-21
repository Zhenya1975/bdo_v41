from extensions import extensions
from datetime import datetime
import pytz

db = extensions.db
utc_now = pytz.utc.localize(datetime.utcnow())
pst_now = utc_now.astimezone(pytz.timezone("Europe/Moscow")).strftime("%d.%m.%Y %H:%M:%S")
date_time_plug = '31/12/2199 23:59:59'
date_time_plug = datetime.strptime(date_time_plug, '%d/%m/%Y %H:%M:%S')



class Eo_candidatesDB(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  eo_code = db.Column(db.String)
  temp_eo_code = db.Column(db.String)
  eo_description = db.Column(db.String)
  be_code = db.Column(db.Integer)
  be_description = db.Column(db.String)
  teh_mesto = db.Column(db.String)
  gar_no = db.Column(db.String)
  head_type = db.Column(db.String)
  eo_model_id = db.Column(db.Integer)
  eo_model_name = db.Column(db.String)
  eo_class_code = db.Column(db.String)
  eo_class_description = db.Column(db.String)
  operation_start_date=db.Column(db.DateTime)
  expected_operation_finish_date = db.Column(db.DateTime)

class Eo_calendar_operation_status_DB(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  eo_code = db.Column(db.String, db.ForeignKey('eo_DB.eo_code'))
  july_2022_qty = db.Column(db.Float)
  july_2022_age = db.Column(db.Float)
  july_2022_in = db.Column(db.Integer)
  july_2022_out = db.Column(db.Integer)
  august_2022_qty = db.Column(db.Float)
  august_2022_age = db.Column(db.Float)
  august_2022_in = db.Column(db.Integer)
  august_2022_out = db.Column(db.Integer)
  sep_2022_qty = db.Column(db.Float)
  sep_2022_age = db.Column(db.Float)
  sep_2022_in = db.Column(db.Integer)
  sep_2022_out = db.Column(db.Integer)
  oct_2022_qty = db.Column(db.Float)
  oct_2022_age = db.Column(db.Float)
  oct_2022_in = db.Column(db.Integer)
  oct_2022_out = db.Column(db.Integer)
  nov_2022_qty = db.Column(db.Float)
  nov_2022_age = db.Column(db.Float)
  nov_2022_in = db.Column(db.Integer)
  nov_2022_out = db.Column(db.Integer)
  dec_2022_qty = db.Column(db.Float)
  dec_2022_age = db.Column(db.Float)
  dec_2022_in = db.Column(db.Integer)
  dec_2022_out = db.Column(db.Integer)
  year_2022_qty = db.Column(db.Float)
  year_2022_age = db.Column(db.Float)
  year_2022_in = db.Column(db.Integer)
  year_2022_out = db.Column(db.Integer)
  year_2023_qty = db.Column(db.Float)
  year_2023_age = db.Column(db.Float)
  year_2023_in = db.Column(db.Integer)
  year_2023_out = db.Column(db.Integer)
  year_2024_qty = db.Column(db.Float)
  year_2024_age = db.Column(db.Float)
  year_2024_in = db.Column(db.Integer)
  year_2024_out = db.Column(db.Integer)
  year_2025_qty = db.Column(db.Float)
  year_2025_age = db.Column(db.Float)
  year_2025_in = db.Column(db.Integer)
  year_2025_out = db.Column(db.Integer)
  year_2026_qty = db.Column(db.Float)
  year_2026_age = db.Column(db.Float)
  year_2026_in = db.Column(db.Integer)
  year_2026_out = db.Column(db.Integer)
  year_2027_qty = db.Column(db.Float)
  year_2027_age = db.Column(db.Float)
  year_2027_in = db.Column(db.Integer)
  year_2027_out = db.Column(db.Integer)
  year_2028_qty = db.Column(db.Float)
  year_2028_age = db.Column(db.Float)
  year_2028_in = db.Column(db.Integer)
  year_2028_out = db.Column(db.Integer)
  year_2029_qty = db.Column(db.Float)
  year_2029_age = db.Column(db.Float)
  year_2029_in = db.Column(db.Integer)
  year_2029_out = db.Column(db.Integer)
  year_2030_qty = db.Column(db.Float)
  year_2030_age = db.Column(db.Float)
  year_2030_in = db.Column(db.Integer)
  year_2030_out = db.Column(db.Integer)
  year_2031_qty = db.Column(db.Float)
  year_2031_age = db.Column(db.Float)
  year_2031_in = db.Column(db.Integer)
  year_2031_out = db.Column(db.Integer)


class Eo_DB(db.Model):
  eo_id = db.Column(db.Integer, primary_key=True)
  eo_code = db.Column(db.String, unique=True, nullable=False)
  temp_eo_code = db.Column(db.String)
  temp_eo_code_status = db.Column(db.String)
  eo_description = db.Column(db.String)
  be_code = db.Column(db.Integer, db.ForeignKey('be_DB.be_code'))
  teh_mesto = db.Column(db.String)
  gar_no = db.Column(db.String)
  sap_gar_no = db.Column(db.String)
  head_type = db.Column(db.String)
  eo_model_id = db.Column(db.Integer, db.ForeignKey('models_DB.eo_model_id'))
  eo_class_code = db.Column(db.String, db.ForeignKey('eo_class_DB.eo_class_code'))
  operation_start_date=db.Column(db.DateTime)
  reported_operation_start_date=db.Column(db.DateTime)
  expected_operation_period_years = db.Column(db.Float, default = 10) # ожидаемый период эксплуатации.
  operation_finish_date_calc = db.Column(db.DateTime) #срок завершения эксплуатации, расчитанный из ожидаемого периода эксплуатации
  operation_finish_date_sap_upd = db.Column(db.DateTime) # срок завершения эксплуатации, приведенный из поля в сап и если в поле пусто, то из расчета от ожидаемого срока эксплуатации.
  expected_operation_finish_date = db.Column(db.DateTime, default = date_time_plug) # расчетный срок завершения эксплуатации
  
  sap_planned_finish_operation_date = db.Column(db.DateTime) # дата из поля Плановая дата завершения эксплуатации
  
  expected_operation_status_code = db.Column(db.String, db.ForeignKey('operation_statusDB.operation_status_code')) # статус в котором должно находиться оборудование на текущую дату
  expected_operation_status_code_date = db.Column(db.DateTime) # текущая дата снятия отчета в котором должно находиться оборудование
  reported_operation_status = db.Column(db.String)
  reported_operation_finish_date = db.Column(db.DateTime) # срока завершения эксплуатации из файлов, полученных из бизнес-единиц
  finish_date_delta = db.Column(db.Float)
  reported_operation_status_code = db.Column(db.String, db.ForeignKey('operation_statusDB.operation_status_code'))
  reported_operation_status_date = db.Column(db.DateTime)
  operation_finish_date_conflict = db.Column(db.String) # если даты reported и operation_finish_date_sap_upd отличаются, то пишем сюда текст с конфликтом
  evaluated_operation_finish_date = db.Column(db.DateTime)  # срок завершения эксплуатации с учетом данных из бизнес-единиц
  sap_system_status = db.Column(db.String)
  sap_user_status = db.Column(db.String)
  sap_model_name = db.Column(db.String)
  sap_maker = db.Column(db.String)
  maker = db.Column(db.String)
  age = db.Column(db.Float)
  age_date = db.Column(db.DateTime)
  age_calc_operation_status = db.Column(db.Integer)
  constr_type = db.Column(db.String)
  constr_type_descr = db.Column(db.String)

  conflict_data = db.relationship('Eo_data_conflicts', backref='conflict_data')
  logs_data = db.relationship('LogsDB', backref='logs_data')
  calendar_status = db.relationship('Eo_calendar_operation_status_DB', backref='calendar_status')
  prodlenie_2022 = db.Column(db.Integer)
  custom_eo_status = db.Column(db.String)

class Operation_statusDB(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  operation_status_code = db.Column(db.String)
  operation_status_description = db.Column(db.String)
  sap_operation_status = db.Column(db.String)
  expected_operation_status_data = db.relationship('Eo_DB', backref='operation_status_data', foreign_keys="[Eo_DB.expected_operation_status_code]")
  reported_operation_status_data = db.relationship('Eo_DB', backref='reported_status_data', foreign_keys="[Eo_DB.reported_operation_status_code]")



class Models_DB(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  eo_model_id = db.Column(db.Integer, unique=True)
  eo_model_name = db.Column(db.String)
  model_data = db.relationship('Eo_DB', backref='model_data')
  eo_category_spec = db.Column(db.String)
  type_tehniki = db.Column(db.String)
  marka_oborudovania = db.Column(db.String)
  cost_center = db.Column(db.String)

class Eo_class_DB(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  eo_class_code = db.Column(db.String, unique=True)
  eo_class_description = db.Column(db.String)
  eo_class_data = db.relationship('Eo_DB', backref='eo_class_data')

class Be_DB(db.Model):
  be_id = db.Column(db.Integer, primary_key=True)
  be_code = db.Column(db.Integer, unique=True)
  be_description = db.Column(db.String)
  be_location = db.Column(db.String)
  be_data = db.relationship('Eo_DB', backref='be_data')

class LogsDB(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  log_eo_code = db.Column(db.String, db.ForeignKey('eo_DB.eo_code'))
  log_text = db.Column(db.Text)
  log_date = db.Column(db.String, default=pst_now)
  log_status = db.Column(db.String) # new or old
  be_filename = db.Column(db.String)
  be_sender_email = db.Column(db.String)
  be_email_date = db.Column(db.DateTime)

class Eo_data_conflicts(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  be_eo_data_row_no = db.Column(db.Integer)
  eo_code = db.Column(db.String, db.ForeignKey('eo_DB.eo_code'))
  eo_conflict_field = db.Column(db.String)
  eo_conflict_field_current_master_data = db.Column(db.String)
  eo_conflict_field_uploaded_data = db.Column(db.String)
  eo_conflict_description = db.Column(db.Text)
  eo_conflict_date = db.Column(db.String, default=pst_now)
  eo_conflict_status = db.Column(db.String, default='active')
  filename = db.Column(db.String)
  sender_email = db.Column(db.String)
  email_date = db.Column(db.String)
  		
