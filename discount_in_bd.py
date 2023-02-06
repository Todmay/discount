import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

# подключение к БД
from sqlalchemy import create_engine

# connect прописывается одной строкой
def connect_db():
    dbt = 'mssql'
    login = ''
    passod = ''
    host = ''
    db = 'public'
    connect = dbt + '+pyodbc://' + login + ':' + pass + '@' + host + '/' + db + '?driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connect)

    return engine

##### транслитерация листа, нужно там где достали русские данные, некоторые буквы требуют кастомной замены
def translite_list(rus_words):
    translited_words = []
    slovar = {'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
              'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'i', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
              'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h',
              'ц': 'c', 'ч': 'cz', 'ш': 'sh', 'щ': 'scz', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e',
              'ю': 'u', 'я': 'ja', 'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'E',
              'Ж': 'ZH', 'З': 'Z', 'И': 'I', 'Й': 'I', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N',
              'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'H',
              'Ц': 'C', 'Ч': 'CZ', 'Ш': 'SH', 'Щ': 'SCH', 'Ъ': '', 'Ы': 'y', 'Ь': '', 'Э': 'E',
              'Ю': 'U', 'Я': 'YA', ',': '', '?': '', ' ': '_', '~': '', '!': '', '@': '', '#': '',
              '$': '', '%': '', '^': '', '&': '', '*': '', '(': '', ')': '', '-': '', '=': '', '+': '',
              ':': '', ';': '', '<': '', '>': '', '\'': '', '"': '', '\\': '', '/': '', '№': '',
              '[': '', ']': '', '{': '', '}': '', 'ґ': '', 'ї': '', 'є': '', 'Ґ': 'g', 'Ї': 'i',
              'Є': 'e', '—': ''}

    for word in rus_words:
        #страхуемся от нестрокового значения
        word = str(word)
        # Циклически заменяем все буквы в строке
        for key in slovar:
            word = word.replace(key, slovar[key])
        translited_word = word
        translited_words.append(translited_word)

    return translited_words


### zero coupon yield curve - кривой бескупонной доходности
def dic_one_kbp():
    url = 'https://www.cbr.ru/hd_base/zcyc_params/'
    start_from = '01.01.2013'
    end_to = '23.01.2023'
    param = f"UniDbQuery.Posted=True&UniDbQuery.From={start_from}&UniDbQuery.To={end_to}"
#забираем страницу с указанного адреса
    page = requests.get(url, params = param)
#создаем экземпляр класса BeautifulSoup
    soup = BeautifulSoup(page.text, 'html.parser')
#находим на странице все таблицы
    tables = soup.find_all('table')
#выбираем нужную таблицу
    table = tables[0]
#берем заголовки таблицы
    headers = []
    for header in table.find_all('th'):
        headers.append(header.text)
#print(headers)
    del headers[1]
    headers[0] = 'date'
    headers = ['dic' + str(i).replace(',', '') for i in headers]
    #print(headers)
#забираем данные из таблицы
    data = []
    for row in table.find_all('tr'):
        data.append([cell.text for cell in row.find_all('td')])
# кладем список в датафрейм
    df = pd.DataFrame(data, columns=headers).dropna()
    #print(df)
    return df

### premiums for illiquidity   -   надбавки за неликвидность
def dic_four_illiq(sheet_name):
    url = 'http://www.cbr.ru/vfs/statistics/pdko/int_rat/loans_nonfin.xlsx'
    # Задаем путь к файлу
    # В файле на сайте ЦБ несколько листов, лучше грузить их в разные таблицы
    # Загружаем данные из файла Excel
    data  = pd.read_excel(url, sheet_name=sheet_name, skiprows=4)

# Проверяем первую ячейку каждого столбца на пустоту
# Готовим заголовки для выравнивания
    headers = ['Дата']
    m_head = ['до 30 дней включая до востребова-ния', 'от 31 до 90 дней', 'от 91 до 180 дней', 'от 181 дня до 1 года', 'до 1 года включая до востребова-ния', 'от 1 года до 3 лет', 'свыше 3 лет', 'свыше 1 года']
    for x in m_head:
        headers.append('Всего ' + x)
    for x in m_head:
        headers.append('Предпринимательство ' + x)
    headers = translite_list(headers)
    # Замена заголовков
    data.columns = headers
    # удаляем первые 4 строки
    #data = data.iloc[4:]
    return data

######  доходности индексов облигаций

def dic_six_bond_index_one_run(url, start_date = '2014-01-01', end_date = '2023-01-20', limit = '100', start = '0'):

    param = 'from=2014-01-01&till=2023-01-23&sort=TRADEDATE&order=desc'
    # Добавляем заголовки для обхода защиты от ДДОС
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36'}

    #указываем период данных
    param = f'iss.only=history&iss.dp=comma&iss.df=%25Y-%25m-%25d&iss.tf=%25H%3A%25M%3A%25S&iss.dtf=%25Y.%25m.%25d%20%25H%3A%25M%3A%25S&from={start_date}&till={end_date}&limit={limit}&start={start}&sort_order=&sort_order_desc='
    # проверяем, принято ли лицензионное соглашение
    cookies = {'disclaimerAgreementCookie': '1'}
    page = requests.get(url, cookies=cookies, headers=headers, params = param, verify = False)
    # парсим ее с помощью библиотеки BeautifulSoup
    soup = BeautifulSoup(page.content, 'lxml')

    # создаем пустой датафрейм
    df = pd.DataFrame()

    # берем из xml названия колонок и добавляем их в датафрейм
    columns_xml = soup.find('columns').find_all('column')
    columns_list = [column['name'] for column in columns_xml]
    df = df.reindex(columns=columns_list)
    #print(df)
    # берем из xml данные и добавляем их в датафрейм
    rows_xml = soup.find('rows').find_all('row')
    for row in rows_xml:
        row_list = []
        for column in columns_list:
            row_list.append(row[column.lower()])
        df = df.append(pd.Series(row_list, index=columns_list), ignore_index=True)


    return df

# достаем xml кусками по 100 штук, больше нельзя, ограничение самого сайта ЦБ
def dic_six_bond_index_one(url):
    df = pd.DataFrame()
    for i in range(23):
        df = df.append(dic_six_bond_index_one_run(url, limit = str((i+1)*100), start = str((i)*100)))

#### без простоя откидывает соединение
    time.sleep(3)
    return df

def dic_six_bond_index_all():

    # Задаем ссылку 1
    url = 'https://www.moex.com/ru/index/RUCBITR1Y/archive'
    df = pd.DataFrame()

    url1 = 'https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RUCBITR1Y.xml'

    df = df.append(dic_six_bond_index_one(url1))

    url2 = 'https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RUCBTR3Y.xml'

    df = df.append(dic_six_bond_index_one(url2))

    url3 = 'https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RUCBITR3+.xml'

    df = df.append(dic_six_bond_index_one(url3))

    return df



############### тут код main() #######
#забираем первый пункт


#kbp_from_cb = dic_one_kbp()
#ill_from_Cb_struct = dic_four_illiq('cтруктура_руб')
# удаляем последние две строки, так как там артефакт загрузки из экселя
#ill_from_Cb_struct = ill_from_Cb_struct.iloc[:-2]
#ill_from_Cb_stavka = dic_four_illiq('ставки_руб')
# удаляем последнюю, так как там артефакт загрузки из экселя
#ill_from_Cb_stavka = ill_from_Cb_stavka.iloc[:-1]
#bond_index = dic_six_bond_index_all()


#print(ill_from_Cb)
#print(ill_from_Cb.iloc[0])
# подключаемся к БД
engine = connect_db()


# создаем таблицу на основе датафрэйма
#kbp_from_cb.to_sql(schema='dbo', name='discount_kbp_curve', con=engine, if_exists='replace', index=False)
#ill_from_Cb_struct.to_sql(schema='dbo', name='discount_premium_illiq_struct', con=engine, if_exists='replace', index=False)
#ill_from_Cb_stavka.to_sql(schema='dbo', name='discount_premium_illiq_stavka', con=engine, if_exists='replace', index=False)
#bond_index.to_sql(schema='dbo', name='discount_bond_index', con=engine, if_exists='replace', index=False)