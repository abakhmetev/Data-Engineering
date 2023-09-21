import asyncio
import time
from aiohttp import ClientSession,TCPConnector
import pandas as pd
import sqlite3


# Функция получения ссылок на вакансии. Получаем 100 вакансий.
async def get_vacancies(n, session):
    url = "https://api.hh.ru/vacancies"
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    params = {'text': 'middle Python developer', 
            'per_page': 20, 
            'page': n,
            'search_field': 'name'
            }
    # Контекст менеджер with в асинхронном режиме
    async with session.get(url, headers=headers, params=params) as response:
        vacancies = await response.json()
        l = []
        if response.status == 200:
            for vacancy in vacancies['items']:
                l.append(vacancy['url'])
            return l
        else:
            print('Ошибка обращения к серверу. Код ответа: ', response.status_code)


# Функция выборки данных по каждой вакансии вакансии
async def get_vacancy(url, session):
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    # Контекст менеджер with в асинхронном режиме
    async with session.get(url, headers=headers) as response:
        vacancy_json = await response.json()
        if response.status == 200:
            return vacancy_json
        else:
            print('Ошибка обращения к серверу. Код ответа: ', response.status) 


# Функция заполнения eventLoop задачами по получению списка ссылок на вакансии
async def main_vacancies():
    async with ClientSession() as session:
        tasks = []
        for n in range(5):
            tasks.append(asyncio.create_task(get_vacancies(n, session)))

        results = await asyncio.gather(*tasks)
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


# Функция заполнения eventLoop задачами по получению данных по каждой вакансии
async def main_vacancy():
    connector = TCPConnector(limit_per_host=5) # Как использовать limit_per_host в aiohttp (https://qna.habr.com/q/1216876)
    async with ClientSession(connector=connector) as session:
        tasks = []       
        for url in href:
            tasks.append(asyncio.create_task(get_vacancy(url, session)))
        results = await asyncio.gather(*tasks)
    l_df = []

    for vacancy in results:
        # vacancy = result.json()
        company_name = vacancy['employer']['name']
        position = vacancy['name']
        job_description = vacancy['description']
        skills = []
        for j in vacancy['key_skills']:
            skills.append(j['name'])
        key_skills = ','.join(skills)
        ls = [company_name, position, job_description, key_skills]
        l_df.append(ls)       
    print('Количество элементов в l_df: ', len(l_df))
    return l_df
    # print(l_df)
# Запускаем native coroutine main с помощью 
# библиотеки asyncio
l_df = asyncio.run(main_vacancy())
# print('Количество элементов list_vacancy: ', len(list_vacancy))


# Сохраняем список вакансий в DataFrame
df = pd.DataFrame(data=l_df, columns=['company_name', 'position', 'job_description', 'key_skills']).astype({
                        'company_name':str,
                        'position': str,
                        'job_description': str,
                        'key_skills': str})

print(df.info())
print("Время выполнения, с: ", time.time() - start)

connection = sqlite3.connect('hw1.db')
df.to_sql('vacancies_asyncio', 
             connection, 
             if_exists='replace',
             index=False)
