import common as c
import config


class Database:
    def __init__(self):
        self.database = 'investment_analysis'
        self.table = 'tb_scraping'
        self.connection = Database.get_user(self)
        self.cursor = self.connection.cursor()
        self.connection.database = self.database

        self.cursor.execute(f"SHOW TABLES LIKE '{self.table}'")
        result = self.cursor.fetchone()
        if result:
            for source in c.sources:
                try:
                    df = c.pd.read_csv(f'./csv/{source["file_name"]}')
                    df['fonte'] = source['name']
                    df['data'] = source['file_name'].split('_')[1].split('.')[0]
                    df = c.Cleaner(df).clean()
                    Database.insert_data(self, df)
                except c.pd.errors.ParserError as er:
                    print(f"{c.CROSSMARK}Failed: {er}{c.ENDC}")
            print(f"{c.CHECKMARK}Data inserted in {self.table} table{c.ENDC}")
        else:
            print(f"{c.CROSSMARK}Table '{self.table}' not found{c.ENDC}")
            if not config.is_initialized():
                Database.create_guest_user(self)
                config.set_initialized()

        self.cursor.close()


    def get_user(self):
        if config.is_first_run():
            try:
                connection = c.mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password=c.getpass.getpass(f"Enter root password: ")
                )
                cursor = connection.cursor()
                cursor.execute(f"SHOW DATABASES LIKE '{self.database}'")
                result = cursor.fetchone()
                if not result:
                    cursor.execute(f"CREATE DATABASE {self.database}")
                    print(f"{c.CHECKMARK}Database '{self.database}' created{c.ENDC}")
                    cursor.execute(f"USE {self.database}")
                    cursor.execute(f'''
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
                    ''')
                    print(f"{c.CHECKMARK}Table '{self.table}' created")
                else:
                    cursor.execute(f"USE {self.database}")
                    print(f"{c.OKGREEN}Connection {c.OK}{c.ENDC}")

                Database.create_guest_user(cursor)
                config.set_initialized()

                return connection
            except c.mysql.connector.Error as error:
                print(f"{c.CROSSMARK}Failed: {error}{c.ENDC}")
                exit(1)
        else:
            try:
                connection = c.mysql.connector.connect(
                    host='localhost',
                    user='guest',
                    password='paS$word123',
                    database=self.database
                )
                cursor = connection.cursor()
                print(f"{c.OKGREEN}Connection {c.OK}{c.ENDC}")
                return connection
            except c.mysql.connector.Error as error:
                print(f"{c.CROSSMARK}Failed: {error}{c.ENDC}")
                exit(1)

    def create_guest_user(cursor):
        try:
            cursor.execute("CREATE USER 'guest'@'localhost' IDENTIFIED BY 'paS$word123'")
            cursor.execute("GRANT ALL PRIVILEGES ON investment_analysis.* TO 'guest'@'localhost'")
            cursor.execute("FLUSH PRIVILEGES")
        except c.mysql.connector.Error as error:
            print(f"{c.CROSSMARK}Failed: {error}{c.ENDC}")
            exit(1)


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
        except c.mysql.connector.Error as error:
            print(f"{c.CROSSMARK}Failed to insert data from {df['fonte'][0]}: {error}{c.ENDC}")


if __name__ == '__main__':
    database = Database()
