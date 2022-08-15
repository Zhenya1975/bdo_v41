sap_columns_to_master_columns = {'Планирующий завод':'be_code', 'Единица оборудования': 'eo_code', 'Название технического объекта':'eo_description', 'Техническое место': 'teh_mesto','Поле сортировки':'gar_no', "ДатВвода в эксплуат.":'operation_start_date', 'Системный статус': 'sap_system_status', 'Пользовательский статус': 'sap_user_status', 'План. срок вывода об.из экспл.': 'sap_planned_finish_operation_date', 'Марка заводская': 'sap_model_name', 'Гаражный номер': 'sap_gar_no','Изготовитель ПроизводУстановки': 'sap_maker', 'Тип конструкции':'constr_type', 'Обозначение типа конструкции':'constr_type_descr', 'Класс ЕО':'eo_class_code'}


be_data_columns_to_master_columns = {"№пп": "be_eo_data_row_no"," ЕО SAP":"eo_code", "ЕО SAP": "eo_code", "Гаражный №": "gar_no", 'Дата ввода в экспл.(полностью включая день) ': 'reported_operation_start_date', 'Дата ввода в экспл': 'reported_operation_start_date', 'Плановая дата вывода из эксплуат': 'reported_operation_finish_date', 'Статус эксплуатации': 'operation_status', 'Дата статуса эксплуатации': 'operation_status_date', 'Удаление записи':'delete_eo_record', "Дата консервации":"conservation_start_date"}

master_data_to_ru_columns = {
  "eo_code":"Единица оборудования",
  'be_code':'Планирующий завод',
  "be_description":"Бизнес единица",
  "gar_no":"Гаражный №", 
  "eo_class_code":"Код класса ЕО",
  "eo_class_description":"Класс ЕО",
  "eo_category_spec":"Категория ЕО",
  "eo_model_name":"Модель",
  "eo_description":"Описание ЕО",
  "sap_user_status":"SAP Пользовательский статус",
  "sap_system_status":"SAP Системный статус",
  "operation_start_date":"Дата начала эксплуатации",
  "expected_operation_period_years":"Нормативный срок службы",
  "operation_finish_date_calc":"Расчетная дата завершения эксплуатации",
  "sap_planned_finish_operation_date":"Дата завершения эксплуатации в САП",
  "operation_finish_date_sap_upd":"Дата завершения эксплуатации САП приведенная",
  "operation_finish_date_update_iteration":"Дата завершения эксплуатации в файле",
  "operation_finish_date":"Дата завершения эксплуатации",
  "operation_status_from_file":"Статус эксплуатации в файле",
  "conservation_start_date":"Дата начала консервации",
  "iteration_name":"Итерация продления",
  "year":"Год",
  "operation_status":"Статус эксплуатации",
  "qty":"Количество",
  "type_tehniki":"Тип техники",	
  "marka_oborudovania":"Марка оборудования",
  "constr_type":"Тип конструкции",
  "constr_type_descr":"Тип конструкции Описание",
  "overhaul_type":'Вид ремонта',
  "overhaul_plan_date":"Дата ремонта 6+6",
  "Cost_rub":"Стоимость капремонта руб",
  "cost_center":"Центр затрат"
  }

sap_system_status_ban_list = ['МТКУ НЕАК УСТН', 'МТКУ ПВЕО', 'МТКУ УСТН', 'МТКУ НЕАК ПВЕО']
sap_user_status_ban_list = ['КОНС СИНХ', 'КОНС', 'ВРНД НПВЭ СИНХ']
sap_user_status_cons_status_list = ['КОНС СИНХ', 'КОНС']
be_data_cons_status_list = ["Консервация", "консервация"]

year_dict = {2022:{'period_start':'01.01.2022', 'period_end':'31.12.2022'}, 
              2023:{'period_start':'01.01.2023', 'period_end':'31.12.2023'},
               2024:{'period_start':'01.01.2024', 'period_end':'31.12.2024'},
               2025:{'period_start':'01.01.2025', 'period_end':'31.12.2025'},
               2026:{'period_start':'01.01.2026', 'period_end':'31.12.2026'},
               2027:{'period_start':'01.01.2027', 'period_end':'31.12.2027'},
               2028:{'period_start':'01.01.2028', 'period_end':'31.12.2028'},
               2029:{'period_start':'01.01.2029', 'period_end':'31.12.2029'},
               2030:{'period_start':'01.01.2030', 'period_end':'31.12.2030'},
               2031:{'period_start':'01.01.2031', 'period_end':'31.12.2031'},
               2032:{'period_start':'01.01.2032', 'period_end':'31.12.2032'},
               2033:{'period_start':'01.01.2033', 'period_end':'31.12.2033'},
               2034:{'period_start':'01.01.2034', 'period_end':'31.12.2034'},
               2035:{'period_start':'01.01.2035', 'period_end':'31.12.2035'}
              }
month_dict = {
  'июль 2022':{'period_start':'01.07.2022', 'period_end':'31.07.2022'},
  'август 2022':{'period_start':'01.08.2022', 'period_end':'31.08.2022'},
  'сентябрь 2022':{'period_start':'01.09.2022', 'period_end':'30.09.2022'},
  'октябрь 2022':{'period_start':'01.10.2022', 'period_end':'31.10.2022'},
  'ноябрь 2022':{'period_start':'01.11.2022', 'period_end':'30.11.2022'},
  'декабрь 2022':{'period_start':'01.12.2022', 'period_end':'31.12.2022'},
  'январь 2023':{'period_start':'01.01.2023', 'period_end':'31.01.2023'},
  'февраль 2023':{'period_start':'01.02.2023', 'period_end':'28.02.2023'},
  'март 2023':{'period_start':'01.03.2023', 'period_end':'31.03.2023'},
  'апрель 2023':{'period_start':'01.04.2023', 'period_end':'30.04.2023'},
  'май 2023':{'period_start':'01.05.2023', 'period_end':'31.05.2023'},
  'июнь 2023':{'period_start':'01.06.2023', 'period_end':'30.06.2023'},
  'июль 2023':{'period_start':'01.07.2023', 'period_end':'31.07.2023'},
  'август 2023':{'period_start':'01.08.2023', 'period_end':'31.08.2023'},
  'сентябрь 2023':{'period_start':'01.09.2023', 'period_end':'30.09.2023'},
  'октябрь 2023':{'period_start':'01.10.2023', 'period_end':'31.10.2023'},
  'ноябрь 2023':{'period_start':'01.11.2023', 'period_end':'30.11.2023'},
  'декабрь 2023':{'period_start':'01.12.2023', 'period_end':'31.12.2023'},
}
operaton_status_translation = {
  "Эксплуатация":"in_operation",
  "эксплуатация":"in_operation",
  "Консервация":"in_conservation",
  "ТУ списано":"scrapped"
}

ktg_data_columns_to_master_columns = {
  'ЕО':'eo_code',
  'ПланКТГ':'plan_ktg',
  'Текущий КТГ для FS-PER':'current_ktg_for_fsper',
  'Утв. КТГ':'accepted_ktg',
  'ФактКТГ':'fact_ktg'
}

