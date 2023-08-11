# -*- coding: iso-8859-1 -*-

import pandas as pd
import common as c
from store import Database


class Analyzer:
    def __init__(self):
        self.conn = Database().connection
        self.cursor = self.conn.cursor()
        self.cursor.execute("SHOW TABLES LIKE 'tb_joe_qual_preco'")
        if not self.cursor.fetchone():
            self.create("tb_joe_qual_preco")
        self.cursor.execute("SHOW TABLES LIKE 'tb_joe_final'")
        if not self.cursor.fetchone():
            self.create("tb_joe_final")

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
        ## Add new column named 'pontos' to the dataframe, for each row, the value is incremented by 1
        ## If value of the column is the same as the previous row, the value of the column 'pontos' is also be the same
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
    
    def create(self, tb):
        if tb == "tb_joe_qual_preco":
            self.cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {tb} (
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

        if tb =="tb_joe_final":
            self.cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {tb} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    dt_coleta DATE,
                    fonte VARCHAR(12),
                    versao VARCHAR(8),
                    ativo VARCHAR(6),
                    soma_pontos_pl FLOAT(10,5),
                    soma_pontos_roic FLOAT(10,5)
                )
            ''')

        print(f"OK - Table {tb} created")

    def insert_data(self, df):
        # Insert data in tb_joe_qual_preco
        self.cursor.execute("DESCRIBE tb_joe_qual_preco")
        columns = [column[0] for column in self.cursor.fetchall() if column[0] != 'id']
        df = df[columns]
        query = f'''
            INSERT INTO tb_joe_qual_preco ({', '.join(columns)})
            VALUES ({', '.join(['%s' for _ in columns])})
        '''
        values_list = df.values.tolist()
        for _, row in df.iterrows():
            data_val = row['dt_coleta']
            ativo_val = row['ativo']
            fonte_val = row['fonte']
            versao_val = row['versao']
            self.cursor.execute(f"SELECT COUNT(*) FROM tb_joe_qual_preco WHERE `dt_coleta` = %s AND `ativo` = %s AND `fonte` = %s AND `versao` = %s", (data_val, ativo_val, fonte_val, versao_val))
            count = self.cursor.fetchone()[0]
            counter = 0
            if count > 0:
                if counter == 0:
                    values_list = []
                query = f'''
                    UPDATE tb_joe_qual_preco
                    SET {', '.join([f"`{col}` = %s" for col in columns])}
                    WHERE `dt_coleta` = %s AND `ativo`= %s AND `fonte` = %s AND `versao` = %s
                '''
                ([row[col] for col in columns] + [data_val, ativo_val, fonte_val, versao_val]).append(values_list)
                counter+=1
        self.cursor.executemany(query, values_list)
        print('OK - Data insert in tb_joe_qual_preco')

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
            values = tuple(row)
            data_val = row[0]
            fonte_val = row[1]
            versao_val = row[2]
            ativo_val = row[3]
            self.cursor.execute(f"SELECT COUNT(*) FROM tb_joe_final WHERE `dt_coleta` = %s AND `ativo` = %s AND `fonte` = %s AND `versao` = %s", (data_val, ativo_val, fonte_val, versao_val))
            count = self.cursor.fetchone()[0]
            if count > 0:
                query = f'''
                    UPDATE tb_joe_final
                    SET {', '.join([f"`{col}` = %s" for col in columns])}
                    WHERE `dt_coleta` = %s AND `ativo`= %s AND `fonte` = %s AND `versao` = %s
                '''
                values = tuple(row) + (data_val, ativo_val, fonte_val, versao_val)
                counter+=1
            self.cursor.execute(query, values)
        print('OK - Data insert in tb_joe_final')
        self.conn.commit()


if __name__ == '__main__':
    Analyzer()
    c.shutil.rmtree('__pycache__')
