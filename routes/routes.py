from flask import Blueprint, render_template, flash, request, redirect, url_for, send_file
from models.models import Eo_DB, LogsDB, Eo_data_conflicts, Eo_candidatesDB
from extensions import extensions
import os
from functions import read_sap_eo_xlsx_file, read_be_eo_xlsx_file_v2, read_be_eo_xlsx_file_v3, generate_excel_master_eo, generate_excel_conflicts, generate_excel_add_candidates, generate_excel_calendar_status_eo, generate_excel_model_eo, read_eo_models_xlsx_file, read_delete_eo_xlsx_file, read_update_eo_data_xlsx_file, eo_data_calculation, generate_eo_diagram_data


UPLOAD_FOLDER = '/uploads'

db = extensions.db

# home = Blueprint('home', __name__, template_folder='templates')
home = Blueprint('home', __name__)

@home.route('/')
def home_view():
  eo_data=Eo_DB.query.order_by(Eo_DB.teh_mesto, Eo_DB.be_code).all()
  log_data = LogsDB.query.filter_by(log_status = "new").all()
  conflicts_data = Eo_data_conflicts.query.filter_by(eo_conflict_status="active").all()
  number_of_active_conflicts = len(list(conflicts_data))
  add_candidates_data = Eo_candidatesDB.query.all()
  number_of_add_candidates = len(list(add_candidates_data))

  # eo_data.sort_values(['teh_mesto', 'be_description', 'eo_class_code', 'head_eo_model_descr'], inplace=True)
  return render_template('home.html', eo_data = eo_data, log_data=log_data, number_of_active_conflicts=number_of_active_conflicts, number_of_add_candidates=number_of_add_candidates)



@home.route('/eo_data_calc', methods=['GET', 'POST'])
def eo_data_calc():
  if request.method == 'POST': 
    
    eo_data_calculation.eo_data_calculation()
    message = "Данные пересчитаны"
    

    flash(message, 'alert-success')
    return redirect(url_for('home.home_view'))
    
  redirect(url_for('home.home_view'))


def allowed_file(filename):
  ALLOWED_EXTENSIONS = {'xlsx','csv'}  
  return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@home.route('/upload_delete_eo_file', methods=['GET', 'POST'])
def upload_delete_eo_file():
  if request.method == 'POST':
    uploaded_file = request.files['file']
    if uploaded_file.filename == '':
      message = f"файл с пустым именем"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))
    
    elif allowed_file(uploaded_file.filename) == False:
      message = f"Неразрешенное расширение файла {uploaded_file.filename}"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))

    elif "delete_eo" not in uploaded_file.filename:
      message = "В имени файла нет текста delete_eo"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))

    elif "xlsx" not in uploaded_file.filename.lower():
      message = "В имени файла нет расширения xlsx"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))

    else:    
      uploaded_file.save(os.path.join('uploads', "delete_eo.xlsx"))
      message = f"файл {uploaded_file.filename} загружен"
      # read_be_eo_xlsx_file.read_be_eo_xlsx()
      read_delete_eo_xlsx_file.read_delete_eo_xlsx()
      flash(message, 'alert-success')
      return redirect(url_for('home.home_view'))

    return 'not uploaded'



@home.route('/update_eo_data_file', methods=['GET', 'POST'])
def update_eo_data_file():
  if request.method == 'POST':
    uploaded_file = request.files['file']
    if uploaded_file.filename == '':
      message = f"файл с пустым именем"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))
    
    elif allowed_file(uploaded_file.filename) == False:
      message = f"Неразрешенное расширение файла {uploaded_file.filename}"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))

    elif "update_eo_data" not in uploaded_file.filename:
      message = "В имени файла нет текста update_eo_data"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))

    elif "xlsx" not in uploaded_file.filename.lower():
      message = "В имени файла нет расширения xlsx"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))

    else:    
      uploaded_file.save(os.path.join('uploads', "update_eo_data.xlsx"))
      message = f"файл {uploaded_file.filename} загружен"
      # read_be_eo_xlsx_file.read_be_eo_xlsx()
      read_update_eo_data_xlsx_file.read_update_eo_data_xlsx()
      flash(message, 'alert-success')
      return redirect(url_for('home.home_view'))

    return 'not uploaded'




@home.route('/upload_models_eo_file', methods=['GET', 'POST'])
def upload_models_eo_file():
  if request.method == 'POST':
    uploaded_file = request.files['file']
    if uploaded_file.filename == '':
      message = f"файл с пустым именем"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))
    
    elif allowed_file(uploaded_file.filename) == False:
      message = f"Неразрешенное расширение файла {uploaded_file.filename}"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))

    elif "model_eo" not in uploaded_file.filename:
      message = "В имени файла нет текста model_eo"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))

    elif "xlsx" not in uploaded_file.filename.lower():
      message = "В имени файла нет расширения xlsx"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))

    else:    
      uploaded_file.save(os.path.join('uploads', "eo_models.xlsx"))
      message = f"файл {uploaded_file.filename} загружен"
      # read_be_eo_xlsx_file.read_be_eo_xlsx()
      read_eo_models_xlsx_file.read_eo_models_xlsx()
      flash(message, 'alert-success')
      return redirect(url_for('home.home_view'))

    return 'not uploaded'


@home.route('/upload_be_eo_file', methods=['GET', 'POST'])
def upload_be_eo_file():
  if request.method == 'POST':
    uploaded_file = request.files['file']
    if uploaded_file.filename == '':
      message = f"файл с пустым именем"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))
    
    elif allowed_file(uploaded_file.filename) == False:
      message = f"Неразрешенное расширение файла {uploaded_file.filename}"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))

    elif "be_eo_data" not in uploaded_file.filename:
      message = "В имени файла нет текста be_eo_data"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))

    elif "xlsx" not in uploaded_file.filename:
      message = "В имени файла нет расширения xlsx"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))

    else:    
      uploaded_file.save(os.path.join('uploads', "be_eo_data.xlsx"))
      message = f"файл {uploaded_file.filename} загружен"
      # print(f"файл {uploaded_file.filename} загружен")
      # read_be_eo_xlsx_file.read_be_eo_xlsx()
      read_be_eo_xlsx_file_v3.read_be_eo_xlsx()
      flash(message, 'alert-success')
      return redirect(url_for('home.home_view'))

      
    return 'not uploaded'

@home.route('/upload_sap_eo_file', methods=['GET', 'POST'])
def upload_sap_eo_file():
  if request.method == 'POST':
    # check if the post request has the file part
  
    uploaded_file = request.files['file']
    if uploaded_file.filename == '':
      message = f"файл с пустым именем"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))
    elif allowed_file(uploaded_file.filename) == False:
      message = f"Неразрешенное расширение файла {uploaded_file.filename}"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view'))
    elif "sap_eo_data" not in uploaded_file.filename:
      message = "В имени файла нет текста sap_eo_data"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))
    elif "xlsx" not in uploaded_file.filename:
      message = "В имени файла нет расширения xlsx"
      flash(message, 'alert-danger')    
      return redirect(url_for('home.home_view'))
    else:    
      # uploaded_file.save(os.path.join('uploads', uploaded_file.filename))
      uploaded_file.save(os.path.join('uploads', "sap_eo_data.xlsx"))
      message = f"файл {uploaded_file.filename} загружен"
      read_sap_eo_xlsx_file.read_sap_eo_xlsx()
      flash(message, 'alert-success')
    return redirect(url_for('home.home_view'))
  
  return 'not uploaded'




@home.route('/download_delete_eo', methods=['GET', 'POST'])
def download_delete_eo():
  if request.method == 'POST': 
    return send_file("downloads/delete_eo.xlsx", as_attachment=True) 
  return 'not downloaded delete_eo'



@home.route('/download_update_eo_data', methods=['GET', 'POST'])
def download_update_eo_data():
  if request.method == 'POST': 
    return send_file("downloads/update_eo_data.xlsx", as_attachment=True) 
  return 'not downloaded update_eo_data'

@home.route('/download_models_eo', methods=['GET', 'POST'])
def download_models_eo():
  if request.method == 'POST': 
    generate_excel_model_eo.sql_to_model_eo()
    return send_file("downloads/model_eo.xlsx", as_attachment=True) 
  return 'not downloaded be_eo_data_template'



@home.route('/download_calendar_eo_v2_file', methods=['GET', 'POST'])
def download_calendar_eo_v2_file():
  if request.method == 'POST': 
    generate_eo_diagram_data.generate_eo_diagram_data()
    return send_file("downloads/eo_calendar_data_v2.csv", as_attachment=True) 
  return 'not downloaded be_eo_data_template'

@home.route('/download_calendar_eo_file', methods=['GET', 'POST'])
def download_calendar_eo_file():
  if request.method == 'POST': 
    generate_excel_calendar_status_eo.sql_to_eo_calendar_master()
    return send_file("downloads/calendar_eo.xlsx", as_attachment=True) 
  return 'not downloaded be_eo_data_template'


@home.route('/download_be_data_template', methods=['GET', 'POST'])
def download_be_data_template():
  if request.method == 'POST':   
    
    return send_file("downloads/be_eo_data_template.xlsx", as_attachment=True) 
  return 'not downloaded be_eo_data_template'


@home.route('/download_master_eo_file', methods=['GET', 'POST'])
def download_master_eo_file():
  if request.method == 'POST':
    generate_excel_master_eo.sql_to_eo_master()
    return send_file("downloads/eo_master_data.xlsx", as_attachment=True) 
    # except Exception as e:
    #   print("не удалось создать excel файл eo_master_data.xlsx. Ошибка: ", e)
    #   message = f"Не удалось выгрузить файл 'eo_master_data.xlsx'"
    #   flash(message, 'alert-danger')
    #   return redirect(url_for('home.home_view')) 
    

@home.route('/conflicts', methods=['GET', 'POST'])
def conflicts():
  if request.method == 'POST':
    try:
        os.remove("conflicts.xlsx")
    except:
      pass
    # выпекаем excel-файл из базы данных
    generate_excel_conflicts.generate_excel_conflicts()

    return send_file("downloads/conflicts.xlsx", as_attachment=True) 


@home.route('/add_candidates', methods=['GET', 'POST'])
def add_candidates():
  if request.method == 'POST':
    try:
      try:
        os.remove("downloads/sap_eo_data.xlsx")
      except:
        pass
      # выпекаем excel-файл из базы данных
      generate_excel_add_candidates.generate_excel_add_candidates()
      return send_file("downloads/sap_eo_data.xlsx", as_attachment=True) 
    except Exception as e:
      print("не удалось создать excel файл sap_eo_data.xlsx с кандидатами на добавление. Ошибка: ", e)
      message = f"Не удалось выгрузить файл sap_eo_data.xlsx с кандидатами на добавление"
      flash(message, 'alert-danger')
      return redirect(url_for('home.home_view')) 


@home.route('/clean_candidates', methods=['GET', 'POST'])
def clean_candidates():
  if request.method == 'POST':
    Eo_candidatesDB.query.delete()
    log_data_new_record = LogsDB(log_text = f"Очищена таблица Кандидаты на добавление", 	log_status = "new")
    db.session.add(log_data_new_record)
    db.session.commit()
    
    return redirect(url_for('home.home_view'))

@home.route('/clean_conflicts', methods=['GET', 'POST'])
def clean_conflicts():
  if request.method == 'POST':
    Eo_data_conflicts.query.delete()
    db.session.commit()
    return redirect(url_for('home.home_view'))    


