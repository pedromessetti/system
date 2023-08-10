# -*- coding: iso-8859-1 -*-

import mysql.connector
import pandas as pd
import common as c


class Database:
    def __init__(self):
        self.connection = mysql.connector.connect(
                host='localhost',
                user='investment_admin', # <-- put your user here
                password='-Gs@IVxBY4jLZ$wM', # <-- put your password here
                auth_plugin='mysql_native_password'
            )
        self.cursor = self.connection.cursor()
        self.table = 'tb_scraping'
        self.database = 'investment_analysis'

        self.cursor.execute(f"SHOW DATABASES LIKE '{self.database}'")
        result = self.cursor.fetchone()
        if not result:
            Database.create(self)
        else:
            self.cursor.execute(f"USE {self.database}")


    def create(self):
        try:
            self.cursor.execute(f"CREATE DATABASE {self.database}")
            print(f"OK - Database {self.database} created")
            self.cursor.execute(f"USE {self.database}")
            self.cursor.execute(f'''
                CREATE TABLE {self.table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    fonte VARCHAR(12),
                    data DATE,
                    ativo VARCHAR(6),
                    cotacao FLOAT(20,5),
                    p_l FLOAT(20,5),
                    p_vp FLOAT(20,5),
                    psr FLOAT(20,5),
                    div_yield FLOAT(20,5),
                    p_ativo FLOAT(20,5),
                    p_cap_giro FLOAT(20,5),
                    p_ebit FLOAT(20,5),
                    p_ativo_circ FLOAT(20,5),
                    ev_ebit FLOAT(20,5),
                    ev_ebitda FLOAT(20,5),
                    mrg_ebit FLOAT(20,5),
                    mrg_liq FLOAT(20,5),
                    liq_corr FLOAT(20,5),
                    roic FLOAT(20,5),
                    roe FLOAT(20,5),
                    liq_2meses FLOAT(20,5),
                    patrim_liq FLOAT(20,5),
                    div_bruta_patrim FLOAT(20,5),
                    cresc_rec_5anos FLOAT(20,5)
                )
            ''')
            print(f"OK - Table {self.table} created")
        except mysql.connector.Error as error:
            print(f"KO - Creation: {error}")
            exit(1)


    def insert_data(self):
        for source in c.sources:
            try:
                # Windows
                #df = pd.read_csv(f'./csv/{source["file_name"]}', encoding='iso-8859-1')
                # Linux and MacOS
                df = pd.read_csv(f'./csv/{source["file_name"]}', encoding='iso-8859-1')
                df['fonte'] = source['name']
                df['data'] = source['file_name'].split('_')[1].split('.')[0]
                df = Cleaner(df).clean()
                self.cursor.execute(f"DESCRIBE {self.table}")
                columns = [column[0] for column in self.cursor.fetchall() if column[0] != 'id']
                query = f'''
                    INSERT INTO {self.table} (
                        {', '.join(columns)}
                    )
                    VALUES (
                        {', '.join(['%s' for _ in columns])}
                    )
                '''
                values_list = [tuple(row[columns]) for _, row in df.iterrows()]
                try:
                    for _, row in df.iterrows():
                        date_val = row['data']
                        ativo_val = row['ativo']
                        fonte_val = row['fonte']
                        self.cursor.execute(f"SELECT COUNT(*) FROM {self.table} WHERE `data` = %s AND `ativo` = %s AND `fonte` = %s", (date_val, ativo_val, fonte_val))
                        count = self.cursor.fetchone()[0]
                        counter = 0
                        if count > 0:
                            if counter == 0:
                                values_list = []
                            query = f'''
                                UPDATE {self.table}
                                SET {', '.join([f"`{col}` = %s" for col in columns])}
                                WHERE `data` = %s AND `ativo`= %s AND `fonte` = %s
                            '''
                            ([row[col] for col in columns] + [date_val, ativo_val, fonte_val]).append(values_list)
                            counter+=1
                    self.cursor.executemany(query, values_list)
                    self.connection.commit()
                except mysql.connector.Error as error:
                    print(f"KO - Insert data from {df['fonte'][0]}: {error}")
            except pd.errors.ParserError as er:
                print(f"KO - {source['name']}: {er}")
        print(f"OK - Data insert in {self.table}")
        self.cursor.close()
        self.connection.close()


class Cleaner:
    def __init__(self, df):
        self.df = df

    def clean(self):
        if self.df['fonte'][0] == 'StatusInvest':
            self.status_invest()
        elif self.df['fonte'][0] == 'Fundamentus':
            self.fundamentus()
        elif self.df['fonte'][0] == 'InvestSite':
            self.invest_site()
        self.div()
        return self.df

    def fundamentus(self):
        self.df = self.df.rename(columns={'papel': 'ativo'})
        self.df = self.df.rename(columns={'cotação': 'cotacao'})
        self.df = self.df.rename(columns={'p_ativ_circ_liq': 'p_ativo_circ'})
        self.df = self.df.rename(columns={'mrg__líq_': 'mrg_liq'})
        self.df = self.df.rename(columns={'liq__corr_': 'liq_corr'})
        self.df = self.df.rename(columns={'patrim__líq': 'patrim_liq'})
        self.df = self.df.rename(columns={'dív_brut__patrim_': 'div_bruta_patrim'})
        self.df = self.df.rename(columns={'cresc__rec_5a': 'cresc_rec_5anos'})
        return self.df
    
    def status_invest(self):
        self.df = self.df.rename(columns={'ticker': 'ativo'})
        self.df = self.df.rename(columns={'preco': 'cotacao'})
        self.df = self.df.rename(columns={'dy': 'div_yield'})
        self.df = self.df.rename(columns={'p_ativos': 'p_ativo'})
        self.df = self.df.rename(columns={'p_at_cir_liq': 'p_ativo_circ'})
        self.df = self.df.rename(columns={'margem_ebit': 'mrg_ebit'})
        self.df = self.df.rename(columns={'marg_liquida': 'mrg_liq'})
        self.df = self.df.rename(columns={'liq_corrente': 'liq_corr'})
        self.df = self.df.rename(columns={'liquidez_media_diaria': 'liq_2meses'})
        self.df = self.df.rename(columns={'div_liq___patri': 'div_bruta_patrim'})
        self.df = self.df.rename(columns={'cagr_receitas_5_anos': 'cresc_rec_5anos'})
        self.df['ev_ebitda'] = "0.00"
        self.df['patrim_liq'] = "0.00"
        return self.df
        
    def invest_site(self):
        self.df = self.df.rename(columns={'ação': 'ativo'})
        self.df = self.df.rename(columns={'preço': 'cotacao'})
        self.df = self.df.rename(columns={'preço_lucro': 'p_l'})
        self.df = self.df.rename(columns={'preço_vpa': 'p_vp'})
        self.df = self.df.rename(columns={'preço_rec_líq_': 'psr'})
        self.df = self.df.rename(columns={'preço_ativo_total': 'p_ativo'})
        self.df = self.df.rename(columns={'preço_cap_giro': 'p_cap_giro'})
        self.df = self.df.rename(columns={'preço_ebit': 'p_ebit'})
        self.df = self.df.rename(columns={'margem_ebit': 'mrg_ebit'})
        self.df = self.df.rename(columns={'margem_líquida': 'mrg_liq'})
        self.df = self.df.rename(columns={'roinvc': 'roic'})
        self.df = self.df.rename(columns={'rpl': 'roe'})
        self.df['p_ativo_circ'] = "0.00"
        self.df['liq_corr'] = "0.00"
        self.df['liq_2meses'] = "0.00"
        self.df['patrim_liq'] = "0.00"
        self.df['div_bruta_patrim'] = "0.00"
        self.df['cresc_rec_5anos'] = "0.00"
        return self.df

    def div(self):
        self.df['div_yield'] = pd.to_numeric(self.df['div_yield'], errors='coerce') / 100
        self.df['mrg_ebit'] = pd.to_numeric(self.df['mrg_ebit'], errors='coerce') / 100
        self.df['mrg_liq'] = pd.to_numeric(self.df['mrg_liq'], errors='coerce') / 100
        self.df['roic'] = pd.to_numeric(self.df['roic'], errors='coerce') / 100
        self.df['roe'] = pd.to_numeric(self.df['roe'], errors='coerce') / 100
        self.df['cresc_rec_5anos'] = pd.to_numeric(self.df['cresc_rec_5anos'], errors='coerce') / 100
        return self.df


if __name__ == '__main__':
    db = Database()
    db.insert_data()
    c.shutil.rmtree('__pycache__')
