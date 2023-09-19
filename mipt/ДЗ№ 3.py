from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json
import orjson
import logging
import pandas as pd
from zipfile import ZipFile
from pathlib import Path

import asyncio
import time
from aiohttp import ClientSession,TCPConnector

from airflow.utils.dates import days_ago
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
# https://airflow.apache.org/docs/apache-airflow/1.10.14/_api/airflow/hooks/dbapi_hook/index.html


default_args = {
    'owner': 'abakhmetev',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


# [Задача №1] Выгрузка телеком компаний из ЕГРЮЛ
def get_okved():
    logger = logging.getLogger(__name__)
    black_list = []
    df_full = pd.DataFrame(columns=['ogrn','inn','kpp','name','full_name','data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД'])
    with ZipFile('/home/rtstudent/egrul.json.zip') as zipobj:
        file_names = zipobj.namelist()
        for name in file_names:
            # Считываем файл из архива в bites, формируем в json, нормализуем json для Pandas
            df = pd.json_normalize(orjson.loads(zipobj.read(name)))
            if 'data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД' in df.columns.values.tolist():
                df = df[df['data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД'].str.contains('^61').fillna(False)][['ogrn','inn','kpp','name','full_name','data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД']]
                df = df.astype({'ogrn':str,'inn': str,'kpp': str,'name': str,'full_name': str,'data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД': str})
                df_full = pd.concat([df_full, df], ignore_index=True, sort=False)
            else:
                black_list.append(name) 

    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_abakhmetev')
    sqlalchemy_engine = sqlite_hook.get_sqlalchemy_engine()
    logger.debug("Тип данных sqlite_hook:", type(sqlalchemy_engine))
    df_full.to_sql('telecom_companies', 
                 sqlalchemy_engine, 
                 if_exists='replace',
                 index=False)
    logger.debug("Данные в БД записаны")             
    with open('/home/rtstudent/students/abakhmetev/black_list.txt', 'w', encoding='utf-8') as f:
        f.write(str(black_list))


# [Задача №2] Выгрузка данных по вакансиям hh.ru
def import_vacancies_main():
    logger = logging.getLogger(__name__)
    # Получения количества pages в запросе
    async def get_count_pages():
        url = "https://api.hh.ru/vacancies"
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        params = {'text': 'middle Python', 'per_page': 20, 'page': 0,'search_field': 'name',}
        async with ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as preresult:
                count_pages_coroutine = await preresult.json()
                pages = count_pages_coroutine.get('pages')
                logger.debug(f"pages: {pages}")
                return pages
            
    pages = asyncio.run(get_count_pages())
    rng = pages - 1
    logger.debug(f"Кол-во листов: {pages}, rng: {rng}")

    # Функция получения ссылок на вакансии
    async def get_vacancies(n, session):
        url = "https://api.hh.ru/vacancies"
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        params = {'text': 'middle Python', 
                'per_page': 20, 
                'page': n,
                'search_field': 'name'
                }
        logger.debug(f"Начата загрузка ссылок на вакансии {url}")
        # Контекст менеджер with в асинхронном режиме
        async with session.get(url, headers=headers, params=params) as response:
            vacancies = await response.json()
            l = []
            if response.status == 200:
                logger.debug(f"Завершена загрузка ссылок на вакансии {url}")
                for vacancy in vacancies['items']:
                    l.append(vacancy['url'])
                return l
            else:
                print(f'Ошибка обращения к серверу. Код ответа:{response.status}, n = {n}' )

    # Функция выборки данных по каждой вакансии с employers industries
    async def get_vacancy(url, session):
        industries = "Отсутствует ссылка на компанию"
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        logger.debug(f"Начата загрузка вакансии {url}")
        # Контекст менеджер with в асинхронном режиме
        async with ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    vacancy = await response.json()       
                    logger.debug(f"Завершена загрузка вакансии {url}")
                    if vacancy['employer'].get('url'):
                        async with session.get(vacancy['employer']['url'], headers=headers) as employer_request:                  
                            if employer_request.status == 200:                     
                                employer_request_coroutine = await employer_request.json()
                                industries = employer_request_coroutine["industries"]
                            else:                                         
                                print(f'Ошибка обращения к серверу. Код ответа: {response.status}. Ссылка: {url}' )
                    else:
                        industries = "Отсутствует ссылка на компанию"        

                    return [vacancy,industries]
                else:
                    print(f'Ошибка обращения к серверу. Код ответа: {response.status}. Ссылка: {url}' )  



    # Функция заполнения eventLoop задачами по получению списка ссылок на вакансии
    async def main_vacancies():
        connector = TCPConnector(limit=2) # Как использовать limit_per_host в aiohttp (https://qna.habr.com/q/1216876)
        async with ClientSession(connector=connector) as session:
            tasks = []
            for n in range(pages):
                tasks.append(asyncio.create_task(get_vacancies(n, session)))

            results = await asyncio.gather(*tasks)
            logger.debug(results)
        href = []
        for result in results:
            for i in result:
                href.append(i)
        print('Количество ссылок: ', len(href))
        return href
    # Запускаем native coroutine main с помощью 
    # библиотеки asyncio
    start = time.time()
    href =asyncio.run(main_vacancies())



    # Функция заполнения eventLoop задачами по получению данных по каждой вакансии с employers industries
    async def main_vacancy():
        connector = TCPConnector(limit_per_host=3) # Как использовать limit_per_host в aiohttp (https://qna.habr.com/q/1216876)
        async with ClientSession(connector=connector) as session:
            tasks = []       
            # for url in href:
            #     tasks.append(asyncio.create_task(get_vacancy(url, session)))
            # results = await asyncio.gather(*tasks)
            # gpt /*
            for i, url in enumerate(href):
                if i != 0 and i % 5 == 0:  # после каждых пяти задач делаем паузу в 1 секунду
                    await asyncio.sleep(3)
                tasks.append(asyncio.create_task(get_vacancy(url, session)))
            results = await asyncio.gather(*tasks)
            # */ gpt
        l_df = []

        for vacancy in results:
            industries = vacancy[1]
            company_name = vacancy[0]['employer']['name']
            position = vacancy[0]['name']
            job_description = vacancy[0]['description']
            skills = []
            for j in vacancy[0]['key_skills']:
                skills.append(j['name'])
            key_skills = ','.join(skills)
            ls = [company_name, position, job_description, key_skills, industries]
            l_df.append(ls)       
        print('Количество элементов в l_df: ', len(l_df))
        return l_df

    l_df = asyncio.run(main_vacancy())
    logger.debug(f"Сформирован лист l_df. Длина: {len(l_df)}")

    # Работа с DataFrame
    df = pd.DataFrame(data=l_df, columns=['company_name', 'position', 'job_description', 'key_skills', 'industries']).astype({
                                'company_name':str,
                                'position': str,
                                'job_description': str,
                                'key_skills': str,
                                'industries': str})

    # Записываем компании в БД
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_abakhmetev')
    sqlalchemy_engine = sqlite_hook.get_sqlalchemy_engine()
    df.to_sql('company_with_industries', 
                sqlalchemy_engine, 
                if_exists='replace',
                index=False)
    logger.debug('Записана БД company_with_industries')


# [Задача №3] Отбор телеком компаний и вывод тоб 10 
def search_telecom():
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_abakhmetev')
    sqlalchemy_engine = sqlite_hook.get_sqlalchemy_engine()
    df = pd.read_sql("SELECT * FROM company_with_industries WHERE 1=1", sqlalchemy_engine)


    df_tel = df[df['industries'].str.contains("('id': '9')|('id': '9.399')|('id': '9.400')|('id': '9.401')|('id': '9.402')", regex=True, na=False)]
    logging.debug("Сформирована df_tel")

    key_skills = []
    for i in df_tel['key_skills']:
        key_skills.extend(i.split(','))
    print('Всего key_skills: ', len(key_skills))
    print('Уникальных key_skills: ', len(set(key_skills))) 

    # Функция подсчета кол-ва каждого скила 
    def analysis(from_list, to_dict):
        for i in from_list:
            if i in to_dict:
                to_dict[i] += 1
            else:
                to_dict[i] = 1

    # Создаем словарь и применяем функцию analysis()
    lst = key_skills
    dct = {}  
    analysis(lst, dct)
    s = pd.Series(dct, name='count')
    s.index.name = 'key'
    s.reset_index()
    df_rang = pd.DataFrame(s.sort_values(ascending=False).head(10)).reset_index()
    print(df_rang)

    df_rang.to_sql('rang_key_skills', 
                sqlalchemy_engine, 
                if_exists='replace',
                index=False)    



with DAG(
    dag_id='abakhmetev',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@weekly',
    default_args=default_args,
    description='DAG for import okved file',
) as dag:
    task1 = PythonOperator(
        task_id='get_okved',
        python_callable=get_okved,
    )

    task2 = PythonOperator(
        task_id='import_vacancies_main',
        python_callable=import_vacancies_main,
    )

    task3 = PythonOperator(
        task_id='search_telecom',
        python_callable=search_telecom,
    )


    task1 >> task3
    task2 >> task3

