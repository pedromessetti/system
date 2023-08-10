# -*- coding: iso-8859-1 -*-

import pandas as pd
import common as c
from store import Database


class Analyzer:
    def __init__(self):
        self.conn = Database().connection
        self.cursor = self.conn.cursor()

        df_1 = self.modify_df(self.get_data(), 'p_l', ['roe', 'ev_ebit', 'roic'], True)
        df_2 = self.modify_df(self.get_data(), 'roe', ['p_l', 'ev_ebit', 'roic'])
        df_3 = self.modify_df(self.get_data(), 'ev_ebit', ['roe', 'p_l', 'roic'], True)
        df_4 = self.modify_df(self.get_data(), 'roic', ['p_l', 'ev_ebit', 'roe'])

        ## In case of enconding error, use the following lines:
        # df_1 = self.modify_df(self.get_data(), 'p_l', ['roe', 'ev_ebit', 'roic'], True).encode('iso-8859-1')
        # df_2 = self.modify_df(self.get_data(), 'roe', ['p_l', 'ev_ebit', 'roic']).encode('iso-8859-1')
        # df_3 = self.modify_df(self.get_data(), 'ev_ebit', ['roe', 'p_l', 'roic'], True).encode('iso-8859-1')
        # df_4 = self.modify_df(self.get_data(), 'roic', ['p_l', 'ev_ebit', 'roe']).encode('iso-8859-1')

        df_pl = pd.merge(df_1, df_2, on=['ativo', 'dt_coleta', 'fonte'], how='outer')
        df_pl['versao'] = 'joe_pl'
        df_roic = pd.merge(df_3, df_4, on=['ativo', 'dt_coleta', 'fonte'], how='outer')
        df_roic['versao'] = 'joe_roic'
        final_df = pd.merge(df_pl, df_roic, on=['ativo', 'dt_coleta', 'fonte', 'versao'], how='outer')
        final_df = final_df.fillna(value=0)
        
        self.insert_data(final_df)
        self.cursor.close()
        self.conn.close()

    def modify_df(self, df, sort_by, drop_col, ascending=False):
        df = df.sort_values(by=[sort_by], ascending=[False])
        ## Discard unused rows
        df = df.drop(columns=drop_col)
        ## Rename column 'data' to 'dt_coleta'
        df = df.rename(columns={'data': 'dt_coleta'})
        ## If the value of the column is 0, discard the row
        df = df[df[sort_by] != 0]
        ## Add a new column named 'pontos' to the dataframe, for each row, the value is incremented by 1
        ## if the value of the column is the same as the previous row, the value of the column 'pontos' is also the same
        pontos = f'pontos_{sort_by}'
        df[pontos] = 1
        for i in range(1, len(df)):
            if df.iloc[i][sort_by] == df.iloc[i-1][sort_by]:
                df.at[df.index[i], pontos] = df.at[df.index[i-1], pontos]
            else:
                df.at[df.index[i], pontos] = df.at[df.index[i-1], pontos] + 1
        if ascending:
            df = df.sort_values(by=[sort_by], ascending=ascending)
        else:
            df[pontos] = df.iloc[:, -1].iloc[::-1].values
        return df

    def get_data(self):
        self.cursor.execute(f"SELECT fonte, data, ativo, p_l, roe, ev_ebit, roic FROM tb_scraping")
        columns = [column[0] for column in self.cursor.description]
        df = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        return df

    def insert_data(self, df):
        # Create table tb_joe_qual_preco
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_joe_qual_preco (
                id INT AUTO_INCREMENT PRIMARY KEY,
                dt_coleta DATE,
                fonte VARCHAR(12),
                versao VARCHAR(8),
                ativo VARCHAR(6),
                p_l FLOAT(10,5),
                pontos_p_l FLOAT(10,5),
                roe FLOAT(10,5),
                pontos_roe FLOAT(10,5),
                ev_ebit FLOAT(10,5),
                pontos_ev_ebit FLOAT(10,5),
                roic FLOAT(10,5),
                pontos_roic FLOAT(10,5)
            )
        ''')

        # Insert data in tb_joe_qual_preco
        self.cursor.execute("DESCRIBE tb_joe_qual_preco")
        columns = [column[0] for column in self.cursor.fetchall() if column[0] != 'id']
        df = df[columns]
        query = f'''
            INSERT INTO tb_joe_qual_preco ({', '.join(columns)})
            VALUES ({', '.join(['%s' for _ in columns])})
        '''
        self.cursor.executemany(query, df.values.tolist())
        print('OK - Data insert in tb_joe_qual_preco')

        # Create table tb_joe_final
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_joe_final (
                id INT AUTO_INCREMENT PRIMARY KEY,
                dt_coleta DATE,
                fonte VARCHAR(12),
                versao VARCHAR(8),
                ativo VARCHAR(6),
                soma_pontos_pl FLOAT(10,5),
                soma_pontos_roic FLOAT(10,5)
            )
        ''')

        # Insert data in tb_joe_final
        self.cursor.execute('''
                SELECT
                    dt_coleta,
                    fonte,
                    versao,
                    ativo,
                    (pontos_p_l + pontos_roe) AS soma_pontos_pl,
                    (pontos_ev_ebit + pontos_roic) AS soma_pontos_roic
                    FROM tb_joe_qual_preco
                    ORDER BY dt_coleta, fonte, versao, ativo
            ''')
        results = self.cursor.fetchall()
        self.cursor.execute("DESCRIBE tb_joe_final")
        columns = [column[0] for column in self.cursor.fetchall() if column[0] != 'id']
        for row in results:
            query = f'''
                INSERT INTO tb_joe_final ({', '.join(columns)})
                VALUES ({', '.join(['%s' for _ in row])})
            '''
            self.cursor.execute(query, tuple(row))
        self.conn.commit()
        print('OK - Data insert in tb_joe_final')


if __name__ == '__main__':
    Analyzer()
    c.shutil.rmtree('__pycache__')
