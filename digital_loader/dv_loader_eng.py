import os
import requests
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import date, datetime, timedelta
import time


dotenv_path = os.path.join('.env')
if os.path.exists(dotenv_path):
     load_dotenv(dotenv_path)

API_URL = os.getenv('API_URL')
AUTH_URL = os.getenv('AUTH_URL')

PAYLOAD = {
    'limit': 50000,
    'offset': 0
}


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
    r = requests.get(API_URL+f'/task/state/{task_id}', headers=headers, json={}, params=PAYLOAD)
    print(r.json()['taskStatus'])
    return r.json()['taskStatus'] == 'DONE'


def get_report(task_id, headers):
    r = requests.get(API_URL+f'/task/result/{task_id}', headers=headers, json={}, params=PAYLOAD)
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
                  'advertiserListEngNames', 'productBrandListEngNames', 'productSubbrandListEngNames', 'productModelListEngNames',
                  'crossMediaResourceName', 'crossMediaHoldingName', 'crossMediaProductName',
                  'adNetworkName','adServerName', 'adPlayerName', 'researchDate',
                  'adPlacementName', 'adMonitoringId', 'firstIssueDate', 'adVideoUtilityName'

                  ]
    else:
        slices = ['useTypeName', 'adSourceTypeName',
                  'productCategoryL2ListNames', 'productCategoryL3ListNames', 'productCategoryL4ListNames',
                  'advertiserListNames', 'productBrandListNames', 'productSubbrandListNames', 'productModelListNames',
                  'crossMediaResourceName', 'crossMediaHoldingName', 'crossMediaProductName',
                  'adNetworkName','adServerName', 'adPlayerName', 'researchDate',
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

if __name__ == '__main__':
    sdate = date(2023, 3, 15)
    edate = date(2023, 4, 2)
    lang = 'eng'

    tsk_lst = [generate_task(sd, lang) for sd in perdelta(sdate, edate, timedelta(days=1))]


    # headers = get_token()
    # print(headers)

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
        df.to_csv("//omdbackup/Share/Dkrylov/viz team/digital_mvp/data_new_" + lang + "/" + str(sd) + '.csv', encoding='cp1251', index=False, sep=';')

