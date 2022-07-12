Таблица Eo_DB - это мастер-файл.

Импорт "Загрузка данных БДО САП."" Имя файла: "sap_eo_data.xlsx" обновляет значения без проверок.

Импорт "Загрузка данных из файлов "Парк оборудования". Имя файла: "be_eo_data.xlsx" Построчно проверяет данные. 
- Если не найден номер ео, то создается кандидат на добавление.
- Если номер найден, но значение в поле не совпадает с мастер-данными то создается конфликт с записью о расхождении.

Конфликт разрешается или повторной загрузкой с исправленным значением или загрузкой файла "Изменение мастер-данных"

Функции



generate_eo_diagram_data.generate_eo_diagram_data():
Основаная цель - сборка result_diagram_data_df.
Итерируемся по словарю year_dict.
Для каждого года из списка:
  - создается выборка из датафрема данных, полученных из мастер-файла. В выборку попадают записи, которые сейчас находятся в эксплуатации.
  Из выборки готовится временный датафрейм с записями с текущим годом и единичкой в поле qty_by_end_of_year и age
  - создается выборка с записями, которые вошли в эксплуатацию в текущем году. Эта выборка мерджится справа от предыдущей.
  - создается выборка с записями, которые вышли их эксплуатации в текущем году. Эта выборка мерджится справа от предыдущей.
  результирующий датафрейм конкатинируется снизу к предыдущему году.


generate_excel_calendar_status_eos.ql_to_eo_calendar_master():
Чтение из базы данных из таблицы ео и eo_calendar_operation_status_DB 
приведение полей дат в дату и сохранение в эксель downloads/calendar_eo.xlsx


generate_excel_conflicts

generate_excel_master_eo
Чтение из базы данных из таблицы ео. \n 
Приведение полей дат в дату и сохранение в эксель downloads/eo_master_data.xlsx

generate_excel_model_eo


read_be_eo_xlsx_file_v3


read_delete_eo_xlsx_file

read_eo_models_xlsx_file

read_sap_eo_xlsx_file.read_sap_eo_xlsx():
Итерируемся по файлу uploads/sap_eo_data.xlsx \n
  Если в мастер-данных нет записи, то создается новая запись в которую добавляется только eo_code. \n
  Обновление данных: \n
   - Проверяем по колонкам есть ли такие колонки. Если есть, то изменяем данные. \n
   - происходит расчет ожидаемого статуса эксплуатации expected_operation_status = expected_operation_status_code(eo_code_excel, operation_start_date, calculated_operation_finish_date, sap_planned_finish_operation_date, today_datetime)



read_update_eo_data_xlsx_file.read_update_eo_data_xlsx(): 
Итерируемся по загруженному файлу uploads/update_eo_data.xlsx
Проверем есть ли искомая колонка в загруженном файле.
Если есть, то обновляем значение в соответствующей колонке в мастер данных

read_eo_models_xlsx_file.read_eo_models_xlsx()
Итерируемся по загруженному файлу с моделями.
Обновляем ранее созданные записи или создаем новые, если не находим в мастер-таблице


eo_data_calculation():
  """
  1. Если expected_operation_finish_date не пустое, то в evaluated_operation_finish_date присваивается expected_operation_finish_date.
  2. Если sap_planned_finish_operation_date не пустое, то в evaluated_operation_finish_date присваивается sap_planned_finish_operation_date
  3. Если reported_operation_finish_date не пустое, то в evaluated_operation_finish_date присваивается reported_operation_finish_date
  """