# -*- coding: iso-8859-1 -*-

import pandas as pd
import common as c
from store import Database
import mysql.connector


class Analyzer:
    def __init__(self):
        self.connection = mysql.connector.connect(
                    host='localhost',
                    user='investment_admin', # <-- put your user here
                    password='-Gs@IVxBY4jLZ$wM', # <-- put your password here
                    auth_plugin='mysql_native_password',
                    database='investment_analysis'
                )
        self.cursor = self.connection.cursor()

        self.df_1 = self.get_data().sort_values(by=['p_l'], ascending=[False])
        ## Discard unused rows
        self.df_1 = self.df_1.drop(columns=['roe', 'ev_ebit', 'roic'])
        ## If the value of the column 'p_l' is 0, discard the row
        self.df_1 = self.df_1[self.df_1['p_l'] != 0]
        ## Add a new column named 'pontos' to the dataframe, for each row, the value is incremented by 1
        self.df_1['pontos_pl'] = 1
        for i in range(1, len(self.df_1)):
            if self.df_1.iloc[i]['p_l'] == self.df_1.iloc[i-1]['p_l']:
                self.df_1.at[self.df_1.index[i], 'pontos_pl'] = self.df_1.at[self.df_1.index[i-1], 'pontos_pl']
            else:
                self.df_1.at[self.df_1.index[i], 'pontos_pl'] = self.df_1.at[self.df_1.index[i-1], 'pontos_pl'] + 1
        self.df_1 = self.df_1.sort_values(by=['p_l'], ascending=[True])        
        #print(f'DataFrame 1\n {self.df_1}')


        self.df_2 = self.get_data().sort_values(by=['roe'], ascending=[False])
        ## Discard unused rows
        self.df_2 = self.df_2.drop(columns=['p_l', 'ev_ebit', 'roic'])
        ## If the value of the column 'roe' is 0, discard the row
        self.df_2 = self.df_2[self.df_2['roe'] != 0]
        ## Add a new column named 'pontos' to the dataframe, for each row, the value is incremented by 1
        ## if the value of the column 'roe' is the same as the previous row, the value of the column 'pontos' is the same as the previous row
        self.df_2['pontos_roe'] = 1
        for i in range(1, len(self.df_2)):
            if self.df_2.iloc[i]['roe'] == self.df_2.iloc[i-1]['roe']:
                self.df_2.at[self.df_2.index[i], 'pontos_roe'] = self.df_2.at[self.df_2.index[i-1], 'pontos_roe']
            else:
                self.df_2.at[self.df_2.index[i], 'pontos_roe'] = self.df_2.at[self.df_2.index[i-1], 'pontos_roe'] + 1
        self.df_2['pontos_roe'] = self.df_2.iloc[:, -1].iloc[::-1].values
        #print(f'DataFrame 2\n {self.df_2}')

        self.df_p_l = pd.merge(self.df_1, self.df_2, on=['ativo', 'data', 'fonte'], how='outer')
        print(f'DataFrame P/L\n {self.df_p_l}')


        self.df_3 = self.get_data().sort_values(by=['ev_ebit'], ascending=[False])
        ## Discard unused rows
        self.df_3 = self.df_3.drop(columns=['roe', 'p_l', 'roic'])
        ## If the value of the column 'ev_ebit' is 0, discard the row
        self.df_3 = self.df_3[self.df_3['ev_ebit'] != 0]
        ## Add a new column named 'pontos' to the dataframe, for each row, the value is incremented by 1
        ## if the value of the column 'ev_ebit' is the same as the previous row, the value of the column 'pontos' is the same as the previous row
        self.df_3['pontos_ev_ebit'] = 1
        for i in range(1, len(self.df_3)):
            if self.df_3.iloc[i]['ev_ebit'] == self.df_3.iloc[i-1]['ev_ebit']:
                self.df_3.at[self.df_3.index[i], 'pontos_ev_ebit'] = self.df_3.at[self.df_3.index[i-1], 'pontos_ev_ebit']
            else:
                self.df_3.at[self.df_3.index[i], 'pontos_ev_ebit'] = self.df_3.at[self.df_3.index[i-1], 'pontos_ev_ebit'] + 1
        self.df_3 = self.df_3.sort_values(by=['ev_ebit'], ascending=[True])
        #print(f'DataFrame 3\n {self.df_3}')


        self.df_4 = self.get_data().sort_values(by=['roic'], ascending=[False])
        ## Discard unused rows
        self.df_4 = self.df_4.drop(columns=['p_l', 'ev_ebit', 'roe'])
        ## If the value of the column 'roe' is 0, discard the row
        self.df_4 = self.df_4[self.df_4['roic'] != 0]
        ## Add a new column named 'pontos' to the dataframe, for each row, the value is incremented by 1
        ## if the value of the column 'roic' is the same as the previous row, the value of the column 'pontos' is the same as the previous row
        self.df_4['pontos_roic'] = 1
        for i in range(1, len(self.df_4)):
            if self.df_4.iloc[i]['roic'] == self.df_4.iloc[i-1]['roic']:
                self.df_4.at[self.df_4.index[i], 'pontos_roic'] = self.df_4.at[self.df_4.index[i-1], 'pontos_roic']
            else:
                self.df_4.at[self.df_4.index[i], 'pontos_roic'] = self.df_4.at[self.df_4.index[i-1], 'pontos_roic'] + 1
        self.df_4['pontos_roic'] = self.df_4.iloc[:, -1].iloc[::-1].values
        #print(f'DataFrame 4\n {self.df_4}')

        self.df_roic = pd.merge(self.df_3, self.df_4, on=['ativo', 'data', 'fonte'], how='outer')        
        print(f'DataFrame ROIC\n {self.df_roic}')


        self.final_df = pd.merge(self.df_p_l, self.df_roic, on=['ativo', 'data', 'fonte'], how='outer')
        print(f'DataFrame Final\n {self.final_df}')


    def get_data(self):
        self.cursor.execute(f"SELECT fonte, data, ativo, p_l, roe, ev_ebit, roic FROM teste")
        columns = [column[0] for column in self.cursor.description]
        df = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        return df


if __name__ == '__main__':
    Analyzer()
    c.shutil.rmtree('__pycache__')
