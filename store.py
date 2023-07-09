import mysql.connector
import var as v
import getpass
from cleaner import Cleaner
import pandas as pd
import shutil


class Database:
    def __init__(self):
        self.connection = Database.get_user()
        self.cursor = self.connection.cursor()
        self.table = 'tb_scraping'

        self.database = 'investment_analysis'
        Database.check_database(self)
        self.connection.database = self.database

        Database.create_table(self)

        self.cursor.execute(f"SHOW TABLES LIKE '{self.table}'")
        result = self.cursor.fetchone()
        if result:
            for source in v.sources:
                try:
                    df = pd.read_csv(f'./csv/{source["file_name"]}')
                    df['fonte'] = source['name']
                    df['data'] = source['file_name'].split('_')[1].split('.')[0]
                    df = Cleaner(df).clean()
                    Database.insert_data(self, df)
                except pd.errors.ParserError as er:
                    print(f"{v.CROSSMARK}Failed: {er}{v.ENDC}")
            print(f"{v.CHECKMARK}Data inserted in {self.table} table{v.ENDC}")
        else:
            print(f"{v.CROSSMARK}Table '{self.table}' not found{v.ENDC}")

        self.cursor.close()


    def get_user():
        while True:
            try:
                connection = mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password=getpass.getpass(f"Enter password: ")
                )
                return connection
            except mysql.connector.Error as error:
                print(f"{v.CROSSMARK}Failed: {error}{v.ENDC}")


    def check_database(self):
        self.cursor.execute(f"SHOW DATABASES LIKE '{self.database}'")
        result = self.cursor.fetchone()
        if not result:
            try:
                self.cursor.execute(f"CREATE DATABASE {self.database}")
                print(f"{v.CHECKMARK}Database '{self.database}' created{v.ENDC}")
            except mysql.connector.Error as error:
                print(f"{v.CROSSMARK}Failed to create database: {error}{v.ENDC}")
                exit(1)
        else:
            try:
                self.connection.database = self.database
                print(f"Database: {v.BOLD}{self.database}{v.ENDC}\n{v.OKGREEN}Connection {v.OK}{v.ENDC}")
            except mysql.connector.Error as error:
                print(f"{v.CROSSMARK}Failed to connect to database '{self.database}': {error}{v.ENDC}")
                exit(1)



    def create_table(self):
        self.cursor.execute(f"SHOW TABLES LIKE '{self.table}'")
        result = self.cursor.fetchone()
        query = f'''
            CREATE TABLE {self.table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                fonte VARCHAR(12),
                data DATE,
                ativo VARCHAR(6),
                cotacao FLOAT(10,2),
                p_l FLOAT(10,2),
                p_vp FLOAT(10,2),
                psr FLOAT(10,2),
                div_yield FLOAT(10,2),
                p_ativo FLOAT(10,2),
                p_cap_giro FLOAT(10,2),
                p_ebit FLOAT(10,2),
                p_ativo_circ FLOAT(10,2),
                ev_ebit FLOAT(10,2),
                ev_ebitda FLOAT(10,2),
                mrg_ebit FLOAT(10,2),
                mrg_liq FLOAT(10,2),
                liq_corr FLOAT(10,2),
                roic FLOAT(10,2),
                roe FLOAT(10,2),
                liq_2meses FLOAT(20,2),
                patrim_liq FLOAT(20,2),
                div_bruta_patrim FLOAT(10,2),
                cresc_rec_5anos FLOAT(10,2)
            )
        '''

        if not result:
            try:
                self.cursor.execute(query)
                print(f"{v.CHECKMARK}Table '{self.table}' created")
            except mysql.connector.Error as error:
                print(f"{v.CROSSMARK}Failed to create table: {error}{v.ENDC}")


    def insert_data(self, df):
        self.cursor.execute(f"DESCRIBE {self.table}")
        columns = [column[0] for column in self.cursor.fetchall() if column[0] != 'id']
        df_filtered = df[columns]
        query = f'''
            INSERT INTO {self.table} (
                {', '.join(columns)}
            )
            VALUES (
                {', '.join(['%s' for _ in columns])}
            )
        '''
        try:
            for _, row in df_filtered.iterrows():
                converted_row = []
                for value in row:
                    try:
                        numeric_value = float(value)
                        converted_row.append((f'{numeric_value:.2f}'))
                    except ValueError:
                        converted_row.append(value)
                self.cursor.execute(query, tuple(converted_row))
            self.connection.commit()
        except mysql.connector.Error as error:
            print(f"{v.CROSSMARK}Failed to insert data from {df['fonte'][0]}: {error}{v.ENDC}")


if __name__ == '__main__':
    Database()
    shutil.rmtree('__pycache__')