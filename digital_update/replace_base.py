import os
import ast
import uuid
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

engine = create_engine('postgresql://olv_master:xSxuQ{pC\_a6:S#p@172.17.2.55:5432/olv_master_base')

try:
    conn = engine.connect()
    print("Соединение с базой данных установлено.")
except Exception as e:
    print("Не удалось установить соединение с базой данных:", e)


def lang_columns(lang):
    if lang == 'eng':
        new_columns = ['adv_list_eng',
                       'art3_list_eng',
                       'ad_placement',
                       'use_type',
                       'subbr_list_eng',
                       'day',
                       'ad_network',
                       'br_list_eng',
                       'mod_list_eng',
                       'media_product',
                       'ad_source_type',
                       'art2_list_eng',
                       'id',
                       'ad_player',
                       'media_resource',
                       'ad_server',
                       'media_holding',
                       'first_issue_date',
                       'art4_list_eng',
                       'ad_video_utility',
                       'ots'
                       ]
        old_columns = ["advertiserListEngNames",
                       "productCategoryL3ListEngNames",
                       "adPlacementName",
                       "useTypeName",
                       "productSubbrandListEngNames",
                       "researchDate",
                       "adNetworkName",
                       "productBrandListEngNames",
                       "productModelListEngNames",
                       "crossMediaProductName",
                       "adSourceTypeName",
                       "productCategoryL2ListEngNames",
                       "adMonitoringId",
                       "adPlayerName",
                       "crossMediaResourceName",
                       "adServerName",
                       "crossMediaHoldingName",
                       "firstIssueDate",
                       "productCategoryL4ListEngNames",
                       "adVideoUtilityName",
                       "stat.ots"
                       ]
    else:
        new_columns = ['adv_list_rus',
                       'art3_list_rus',
                       'ad_placement',
                       'use_type',
                       'subbr_list_rus',
                       'day',
                       'ad_network',
                       'br_list_rus',
                       'mod_list_rus',
                       'media_product',
                       'ad_source_type',
                       'art2_list_rus',
                       'id',
                       'ad_player',
                       'media_resource',
                       'ad_server',
                       'media_holding',
                       'first_issue_date',
                       'art4_list_rus',
                       'ad_video_utility',
                       'ots'
                       ]
        old_columns = ["advertiserListNames",
                       "productCategoryL3ListNames",
                       "adPlacementName",
                       "useTypeName",
                       "productSubbrandListNames",
                       "researchDate",
                       "adNetworkName",
                       "productBrandListNames",
                       "productModelListNames",
                       "crossMediaProductName",
                       "adSourceTypeName",
                       "productCategoryL2ListNames",
                       "adMonitoringId",
                       "adPlayerName",
                       "crossMediaResourceName",
                       "adServerName",
                       "crossMediaHoldingName",
                       "firstIssueDate",
                       "productCategoryL4ListNames",
                       "adVideoUtilityName",
                       "stat.ots"
                       ]
    return new_columns, old_columns


def clear_lsts(column):
    '''
    удаление ненужных знаков из столбца таблицы
    '''
    l = ast.literal_eval(column)
    s = '; '.join(l)
    return s


def create_date(date):
    '''
    добавление дня, недели, месяца
    '''
    date_obj = datetime.strptime(date[0], '%Y-%m-%d').date()
    week_obj = date_obj - timedelta(days=date_obj.weekday())  # начало недели
    week_str = week_obj.strftime('%Y-%m-%d')  # неделя в формате 'yyyy-mm-dd'
    month_str = date_obj.replace(day=1).strftime('%Y-%m-%d')  # месяц в формате 'yyyy-mm-dd'
    return week_str, month_str


def columns_rename(data, new_columns, old_columns):
    '''
    переименование столбцов таблицы
    '''
    list_columns = list(filter(lambda x: 'List' in x, data.columns))
    data[list_columns] = data[list_columns].applymap(clear_lsts)
    data = data.rename(columns={old_column: new_column for old_column, new_column in zip(old_columns, new_columns)})
    return data


print('Start time: ', str(datetime.now())[:19])

if __name__ == '__main__':

    # sdate = '2023-03-09' + '.csv'
    sdate = '2023-03-10' + '.csv'
    edate = '2023-03-10' + '.csv'

    # lang = 'rus'
    lang = 'eng'

    folder = '2' if lang == 'rus' else lang
    name_base = 'digital_' + lang
    #name_base = 'mvp_digital_' + lang

    action = 'replace'
    #action = 'append'

    folder_path = "//omdbackup/Share/Dkrylov/viz team/digital_mvp/data_new_" + folder + "/"
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    csv_files_read = csv_files[csv_files.index(sdate):csv_files.index(edate) + 1]

    for file_name in csv_files_read:
        if file_name.endswith('.csv'):
            try:
                file_path = os.path.join(folder_path, file_name)
                day_date = file_name.split(".")[0]
                try:
                    query = text(f"SELECT COUNT(*) FROM {name_base} WHERE day = :day_date")
                    result = engine.execute(query, day_date=day_date).scalar()
                    if result == 0 and action == 'append':
                        print(f"Данных с такой датой {day_date} ещё нет в базе {name_base}")
                        data = pd.read_csv(file_path, sep=';', encoding='cp1251')
                        if data.empty:
                            print(f"Файл {file_name} пустой")
                        else:
                            actual_columns = list(data.columns)
                            new_columns, old_columns = lang_columns(lang)
                            if set(old_columns) != set(actual_columns):
                                missing_columns = set(old_columns) - set(actual_columns)
                                print(f"В таблице {file_name} отсутствуют столбцы: {', '.join(missing_columns)}")
                            else:
                                if data.isna().all().all():
                                    print(f"Все ячейки таблицы {file_name} пустые")
                                else:
                                    data[['week', 'month']] = create_date(data['researchDate'])
                                    data = columns_rename(data, new_columns, old_columns)
                                    if data['id'].isnull().any():
                                        print(f"В столбце id таблицы {file_name} есть нулевые значения")
                                    else:
                                        data['key_id'] = [str(uuid.uuid4()) for _ in range(len(data))]
                                        try:
                                            data.to_sql(
                                                name=name_base,
                                                con=engine,
                                                if_exists='append',
                                                index=False
                                            )
                                            print(
                                                f"В базу {name_base} добавили таблицу {file_name}, которая содержит {data.shape[0]} строк")
                                        except Exception as e:
                                            print(
                                                f"Ошибка при добавлении новой таблицы {file_name} в базу {name_base}: {e}")
                    elif action == 'replace':
                        print(f"Обновляем данные с такой датой {day_date} в базе {name_base}")
                        try:
                            query = text(f"DELETE FROM {name_base} WHERE day = :day_date")
                            engine.execute(query, day_date=day_date)
                            print(f"Удалили данные с такой датой {day_date} из базы {name_base}")

                            data = pd.read_csv(file_path, sep=';', encoding='cp1251')
                            if data.empty:
                                print(f"Файл {file_name} пустой")
                            else:
                                actual_columns = list(data.columns)
                                new_columns, old_columns = lang_columns(lang)
                                if set(old_columns) != set(actual_columns):
                                    missing_columns = set(old_columns) - set(actual_columns)
                                    print(f"В таблице {file_name} отсутствуют столбцы: {', '.join(missing_columns)}")
                                else:
                                    if data.isna().all().all():
                                        print(f"Все ячейки таблицы {file_name} пустые")
                                    else:
                                        data[['week', 'month']] = create_date(data['researchDate'])
                                        data = columns_rename(data, new_columns, old_columns)
                                        if data['id'].isnull().any():
                                            print(f"В столбце id таблицы {file_name} есть нулевые значения")
                                        else:
                                            data['key_id'] = [str(uuid.uuid4()) for _ in range(len(data))]
                                            try:
                                                data.to_sql(
                                                    name=name_base,
                                                    con=engine,
                                                    if_exists='append',
                                                    index=False
                                                )
                                                print(
                                                    f"В базу {name_base} добавили таблицу {file_name}, которая содержит {data.shape[0]} строк")
                                            except Exception as e:
                                                print(
                                                    f"Ошибка при добавлении новой таблицы {file_name} в базу {name_base}: {e}")
                        except Exception as e:
                            print(f"Не получилось удалить данные с такой датой {day_date} из базы {name_base}: {e}")
                    else:
                        print(f"Данные с такой датой {day_date} уже существуют в базе {name_base}")
                        print(f"Таблица {file_name} в базу {name_base} добавлена не будет")
                except Exception as e:
                    print(f"Ошибка при выполнения запроса к базе на проверку данных c датой {day_date}: {e}")
            except Exception as e:
                print(f"Ошибка при чтении файла {file_name}: {e}")

    engine.dispose()

    print('End time: ', str(datetime.now())[:19])
