from contextlib import closing
import json
import pymysql
from pymysql.cursors import DictCursor


class WorkerDB():
    """
        Класс для работы с базой данных.
        необходимые данные:
        dbname - str
        password - str
        user - str, default 'postgres'
        host - str, default 'localhost'
    """

    def __init__(self, dbname='', password='', user='postgres', host='localhost'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def tables_list(self):
        """
            Вывести список всех таблиц в текущей БД
        """
        sql_str = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        with closing(pymysql.connect(db=self.dbname, user=self.user, password=self.password, host=self.host)) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_str)
                res = cursor.fetchall()
        return [table_name[0] for table_name in res]

    def get(self, table_name, columns=None, where=None):
        """
            Получить данные (headers и data) из таблицы table_name
            опционально:
            columns - list[str]: необходимые столбцы
            where - str: SQL-условие, фильтр
        """

        if columns == None:
            columns_str = '*'
        else:
            columns_str = ', '.join(columns)

        if where == None:
            where_str = ''
        else:
            where_str = 'WHERE ' + where

        sql_str = 'SELECT %s FROM %s %s' % (columns_str, table_name, where_str)

        with closing(
                pymysql.connect(db=self.dbname, user=self.user, password=self.password, host=self.host)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_str)
                datas = cursor.fetchall()  # description
                headers = cursor.description

        return self.format_res(raw_res={'headers': headers, 'data': datas})

    def request(self, sql_str):
        """
            Произвести кастомный запрос к БД
        """

        with closing(
                pymysql.connect(db=self.dbname, user=self.user, password=self.password, host=self.host)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_str)
                datas = cursor.fetchall()  # description
                headers = cursor.description

        return self.format_res(raw_res={'headers': headers, 'data': datas})

    def insert(self, table_name, data):
        """
            Вставить данные data в таблицу table_name
        """
        inserted_data = list(data.values())

        sql_str = '''
            INSERT INTO %s (%s)
            VALUES ( %s );
        ''' % (table_name, ', '.join(data), ', '.join(['%s'] * len(data)))

        with closing(
                pymysql.connect(db=self.dbname, user=self.user, password=self.password, host=self.host)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_str, inserted_data)
                id = cursor.lastrowid
            conn.commit()
        return id

    def clear_table(self, table_name):
        """
            полностью очистить таблицу table_name (производится TRUNCATE)
        """
        sql_str = 'TRUNCATE %s CASCADE' % table_name

        with closing(
                pymysql.connect(db=self.dbname, user=self.user, password=self.password, host=self.host)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_str)
            conn.commit()

    def delete_rows(self, table_name, where=None):
        """
            удалить из таблицы table_name записи по условию where
            table_name - str, название таблицы
            where - str, условие
        """

        if where == None:
            where_str = ''
        else:
            where_str = 'WHERE ' + where

        sql_str = 'DELETE FROM %s %s' % (table_name, where_str)

        with closing(
                pymysql.connect(db=self.dbname, user=self.user, password=self.password, host=self.host)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_str)
            conn.commit()

    def update_row(self, table_name, id, datas):
        """
            изменить данные в записи по ее id в таблице table_name
        """
        sql_str = "UPDATE %s SET " % table_name

        new_data_str = ''
        for index, (key, val) in enumerate(datas.items()):
            if index != 0:
                new_data_str += ','
            new_data_str += "%s='%s' " % (key, val)

        sql_str += new_data_str + 'where id=%s' % id
        with closing(
                pymysql.connect(db=self.dbname, user=self.user, password=self.password, host=self.host)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_str)
            conn.commit()

    def format_res(self, raw_res):
        columns = [column_data[0] for column_data in raw_res['headers']]
        data = []
        for raw_data in raw_res['data']:
            data.append({key: val for key, val in zip(columns, raw_data)})

        return data
