# -*- coding: iso-8859-1 -*-

import mysql.connector
import pandas as pd
import common as c


class Database:
    def __init__(self):
        self.connection = mysql.connector.connect(
                    host='localhost',
                    user='ias_admin', # <-- put your user here
                    password='r&hgEV4CC!8&76Nk' # <-- put your password here
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

        for source in c.sources:
            try:
                # Windows
                #df = pd.read_csv(f'./csv/{source["file_name"]}', encoding='iso-8859-1')
                # Linux and MacOS
                df = pd.read_csv(f'./csv/{source["file_name"]}')
                df['fonte'] = source['name']
                df['data'] = source['file_name'].split('_')[1].split('.')[0]
                df = Cleaner(df).clean()
                Database.insert_data(self, df)
            except pd.errors.ParserError as er:
                print(f"KO - Insert data from {source['name']}: {er}")
        print(f"OK - Data insert in {self.table}")

        self.cursor.close()

    def create(self):
        try:
            self.cursor.execute(f"CREATE DATABASE {self.database}")
            self.cursor.execute(f"USE {self.database}")
            self.cursor.execute(f'''
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
        except mysql.connector.Error as error:
            print(f"KO - Creation: {error}")
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
        except mysql.connector.Error as error:
            print(f"KO - Insert data from {df['fonte'][0]}: {error}")


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
    Database()
    c.shutil.rmtree('__pycache__')
