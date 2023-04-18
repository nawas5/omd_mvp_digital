import os
import ast
import uuid
import time
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import date, timedelta, datetime
from flask import Flask, render_template, request
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

dotenv_path = os.path.join('.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

API_URL = os.getenv('API_URL')
AUTH_URL = os.getenv('AUTH_URL')

PAYLOAD = {
    'limit': 50000,
    'offset': 0
}

app = Flask(__name__)


def get_token():
    # получаем токен
    data = {
        "client_id": os.getenv('CLIENT_ID'),
        "client_secret": os.getenv('CLIENT_SECRET'),
        "username": os.getenv('DV_MVP_USERNAME'),
        "password": os.getenv('PASSWORD'),
        'grant_type': 'password'
    }
    r = requests.post(AUTH_URL, data=data)
    access_token = r.json()['access_token']
    headers = {'Authorization': 'Bearer ' + access_token,
               'Content-Type': 'application/json',
               'Accept': 'application/json'}
    return headers


def send_query(task, headers, url='/task/monitoring'):
    r = requests.post(API_URL + url, headers=headers, json=task, params=PAYLOAD)
    print(r.json())
    task_id = r.json()['taskId']
    return task_id


def check_query_completion(task_id, headers):
    r = requests.get(API_URL + f'/task/state/{task_id}', headers=headers, json={}, params=PAYLOAD)
    print(r.json()['taskStatus'])
    return r.json()['taskStatus'] == 'DONE'


def get_report(task_id, headers):
    r = requests.get(API_URL + f'/task/result/{task_id}', headers=headers, json={}, params=PAYLOAD)
    return r.json()


def result_table(data) -> pd.DataFrame:
    """
    Transform API response to pandas dataframe.

    Args:
        data: API response.

    Returns:
        Dataframe with API response.
    """
    res = {}
    if data is None or type(data) != dict:
        return None
    if 'taskId' not in data or 'resultBody' not in data:
        return None
    if type(data['resultBody']) == list and len(data['resultBody']) == 0:
        msg = data.get('message', None)
        if msg is not None:
            print(msg)
    slices = set()
    statistics = set()
    for item in data['resultBody']:
        stat = item['statistics']
        sls = item['slice']
        for k in sls.keys():
            if k not in slices:
                slices.add(k)
                res[k] = []
        for k in stat.keys():
            if k not in statistics:
                statistics.add(k)
                res['stat.' + k] = []
    for item in data['resultBody']:
        stat = item['statistics']
        sls = item['slice']
        for k in slices:
            if k in sls:
                v = str(sls[k])
            else:
                v = '-'
            res[k].append(v)
        for k in statistics:
            if k in stat:
                v = stat[k]
            else:
                v = None
            res['stat.' + k].append(v)
    df = pd.DataFrame(res)
    df.replace(to_replace=[None], value=np.nan, inplace=True)
    return df


def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


def generate_task(sd, lang):
    slices = []
    if lang == 'eng':
        slices = ['useTypeName', 'adSourceTypeName',
                  'productCategoryL2ListEngNames', 'productCategoryL3ListEngNames', 'productCategoryL4ListEngNames',
                  'advertiserListEngNames', 'productBrandListEngNames', 'productSubbrandListEngNames',
                  'productModelListEngNames',
                  'crossMediaResourceName', 'crossMediaHoldingName', 'crossMediaProductName',
                  'adNetworkName', 'adServerName', 'adPlayerName', 'researchDate',
                  'adPlacementName', 'adMonitoringId', 'firstIssueDate', 'adVideoUtilityName'

                  ]
    else:
        slices = ['useTypeName', 'adSourceTypeName',
                  'productCategoryL2ListNames', 'productCategoryL3ListNames', 'productCategoryL4ListNames',
                  'advertiserListNames', 'productBrandListNames', 'productSubbrandListNames', 'productModelListNames',
                  'crossMediaResourceName', 'crossMediaHoldingName', 'crossMediaProductName',
                  'adNetworkName', 'adServerName', 'adPlayerName', 'researchDate',
                  'adPlacementName', 'adMonitoringId', 'firstIssueDate', 'adVideoUtilityName'

                  ]

    task = {
        'statistics': ['ots'],
        'filter':
            {
                'dateFilter': {
                    'operand': 'OR',
                    'children': [{
                        'operand': 'AND',
                        'elements':
                            [
                                {
                                    'unit': 'researchDate',
                                    'relation': 'GTE',
                                    'value': str(sd)
                                },
                                {
                                    'unit': 'researchDate',
                                    'relation': 'LTE',
                                    'value': str(sd)
                                }
                            ]
                    }]
                },
                'useTypeFilter':
                    {
                        'operand': 'OR',
                        'elements': [
                            {
                                'unit': 'useTypeId',
                                'relation': 'IN',
                                'value': [1, 2, 3]
                            }]
                    },

            },
        'slices': slices
    }
    return (task, sd)


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
    l = ast.literal_eval(column)
    s = '; '.join(l)
    return s


def create_date(day_date):
    date_obj = datetime.strptime(day_date[0], '%Y-%m-%d').date()
    week_obj = date_obj - timedelta(days=date_obj.weekday())  # начало недели
    week_str = week_obj.strftime('%Y-%m-%d')  # неделя в формате 'yyyy-mm-dd'
    month_str = date_obj.replace(day=1).strftime('%Y-%m-%d')  # месяц в формате 'yyyy-mm-dd'
    return week_str, month_str


def columns_rename(data, new_columns, old_columns):
    list_columns = list(filter(lambda x: 'List' in x, data.columns))
    data[list_columns] = data[list_columns].applymap(clear_lsts)
    data = data.rename(columns={old_column: new_column for old_column, new_column in zip(old_columns, new_columns)})
    return data


def get_date_range(start_date=None, end_date=None):
    if start_date is None or end_date is None:
        today = date.today()
        prev_sunday = today - timedelta(days=today.weekday() + 1)
        prev_monday = prev_sunday - timedelta(days=6)
        start_date = prev_monday.strftime('%Y-%m-%d')
        end_date = prev_sunday.strftime('%Y-%m-%d')
    return start_date, end_date


def upload_data(sdate, edate, lang):
    sdate = datetime.strptime(sdate, '%Y-%m-%d').date()
    edate = datetime.strptime(edate, '%Y-%m-%d').date()
    tsk_lst = [generate_task(sd, lang) for sd in perdelta(sdate, edate, timedelta(days=1))]
    folder = '2' if lang == 'rus' else lang
    folder_path = f"//omdbackup/Share/Dkrylov/viz team/digital_mvp/data_new_{folder}/"
    for vtask, sd in tsk_lst:
        headers = get_token()
        t = send_query(vtask, headers)
        f = False
        while f == False:
            time.sleep(60)
            f = check_query_completion(t, headers)
        else:
            report = get_report(t, headers)
        df = result_table(report)
        df.to_csv(folder_path + str(sd) + '.csv',
                  encoding='cp1251', index=False, sep=';')
    # implementation of upload_data function
    message = f"Upload {lang} data {sdate} - {edate}"
    # поставить сообщение об ошибке и в моменте так-же передавать message ?
    return message


def upload_db(sdate, edate, lang, name_base, engine):
    folder = '2' if lang == 'rus' else lang
    name_base = name_base + lang
    folder_path = f"//omdbackup/Share/Dkrylov/viz team/digital_mvp/data_new_{folder}/"
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
                    if result == 0:
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
                    else:
                        print(f"Данные с такой датой {day_date} уже существуют в базе {name_base}")
                        print(f"Таблица {file_name} в базу {name_base} добавлена не будет")
                except Exception as e:
                    print(f"Ошибка при выполнения запроса к базе на проверку данных c датой {day_date}: {e}")
            except Exception as e:
                print(f"Ошибка при чтении файла {file_name}: {e}")

    message = f"Upload {lang} db {sdate} - {edate}"
    return message


def replace_data(sdate, edate, lang, name_base, engine):
    folder = '2' if lang == 'rus' else lang
    name_base = name_base + lang
    folder_path = f"//omdbackup/Share/Dkrylov/viz team/digital_mvp/data_new_{folder}/"
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    csv_files_read = csv_files[csv_files.index(sdate):csv_files.index(edate) + 1]
    for file_name in csv_files_read:
        if file_name.endswith('.csv'):
            try:
                file_path = os.path.join(folder_path, file_name)
                day_date = file_name.split(".")[0]
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
            except Exception as e:
                print(f"Ошибка при чтении файла {file_name}: {e}")

    message = f"Replace {lang} db {sdate} - {edate}"
    return message


# define the route and view function for the index page
@app.route('/', methods=['GET', 'POST'])
def index():
    start_date, end_date = get_date_range()

    if request.method == 'POST':
        button_name = request.form['submit_button']
        if button_name == 'upload_data':
            start_date = request.form['start_date']
            end_date = request.form['end_date']
            language = request.form['language']
            # message error
            message = upload_data(start_date, end_date, language)

            return render_template('index.html', message=message, start_date=start_date, end_date=end_date,
                                   language=language)

        start_date = request.form['start_date']
        end_date = request.form['end_date']
        language = request.form['language']
        message = f"The database is selected in {language}"
        return render_template('index.html', message=message, start_date=start_date, end_date=end_date,
                               language=language)


    else:
        # create engine to connect to the database
        engine = create_engine('postgresql://olv_master:xSxuQ{pC\_a6:S#p@172.17.2.55:5432/olv_master_base')
        try:
            conn = engine.connect()
            message = "Digital data connected successfully"
            message_type = "success"
            return render_template('index.html', message=message, message_type=message_type, start_date=start_date,
                                   end_date=end_date)
        except SQLAlchemyError:
            # if the connection fails, show an error message
            message = "Failed to connected digital data"
            message_type = "error"
            return render_template('index.html', message=message, message_type=message_type, start_date=start_date,
                                   end_date=end_date)


if __name__ == '__main__':
    app.run(debug=True)
