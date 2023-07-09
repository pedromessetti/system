from datetime import date
from bs4 import BeautifulSoup
import mysql.connector
import pandas as pd
import requests
import getpass
import shutil
import csv
import os


# MACRO VARIABLES
BOLD = '\033[1m'
UNDERLINE = '\033[4m'
ENDC = '\033[0m' # End of color
OKBLUE = '\033[94m'
OKGREEN = '\033[92m' # Success
FAIL = '\033[91m' # Error
WARNING = f'{ENDC}\033[93m' # Warning
WHITE = '\033[37m'
CHECKMARK = f"{ENDC}{OKGREEN}✔ "
CROSSMARK = f"{ENDC}{FAIL}✘ "
OK = f"{ENDC}{OKGREEN}[{WHITE}OK{OKGREEN}]"


# SCRAPING VARIABLES
sources = [
    {
        'name': 'Fundamentus',
        'url': 'https://www.fundamentus.com.br/resultado.php',
        'file_name': f'Fundamentus_{date.today()}.csv',
        'type': 'generate'
    }, 
    {
        'name': 'StatusInvest',
        'url': 'https://statusinvest.com.br/category/AdvancedSearchResultExport?search=%7B%22Sector%22%3A%22%22%2C%22SubSector%22%3A%22%22%2C%22Segment%22%3A%22%22%2C%22my_range%22%3A%22-20%3B100%22%2C%22forecast%22%3A%7B%22upsidedownside%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22estimatesnumber%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22revisedup%22%3Atrue%2C%22reviseddown%22%3Atrue%2C%22consensus%22%3A%5B%5D%7D%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_l%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22peg_ratio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margembruta%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemliquida%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22ev_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidaebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidapatrimonioliquido%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_sr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_capitalgiro%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativocirculante%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roe%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roic%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezcorrente%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22pl_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22passivo_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22giroativos%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22receitas_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lucros_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22vpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valormercado%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D&CategoryType=1',
        'file_name': f'StatusInvest_{date.today()}.csv',
        'type': 'download'
    },
    {
        'name': 'InvestSite',
        'url': 'https://www.investsite.com.br/selecao_acoes.php?dt_arr=%255B%252220230623%2522%252C%2522atual%2522%255D&todos=todos&ROTanC_min=&ROTanC_max=&chk_lst%5B%5D=itm7&ROInvC_min=&ROInvC_max=&chk_lst%5B%5D=itm8&ROE_min=&ROE_max=&chk_lst%5B%5D=itm9&ROA_min=&ROA_max=&chk_lst%5B%5D=itm10&margem_liquida_min=&margem_liquida_max=&chk_lst%5B%5D=itm11&margem_bruta_min=&margem_bruta_max=&chk_lst%5B%5D=itm12&margem_EBIT_min=&margem_EBIT_max=&chk_lst%5B%5D=itm13&giro_ativo_min=&giro_ativo_max=&chk_lst%5B%5D=itm14&fin_leverage_min=&fin_leverage_max=&chk_lst%5B%5D=itm15&debt_equity_min=&debt_equity_max=&chk_lst%5B%5D=itm16&p_e_min=&p_e_max=&chk_lst%5B%5D=itm17&p_bv_min=&p_bv_max=&chk_lst%5B%5D=itm18&p_receita_liquida_min=&p_receita_liquida_max=&chk_lst%5B%5D=itm19&p_FCO_min=&p_FCO_max=&chk_lst%5B%5D=itm20&p_FCF1_min=&p_FCF1_max=&chk_lst%5B%5D=itm21&p_EBIT_min=&p_EBIT_max=&chk_lst%5B%5D=itm22&p_ncav_min=&p_ncav_max=&chk_lst%5B%5D=itm23&p_ativo_total_min=&p_ativo_total_max=&chk_lst%5B%5D=itm24&p_capital_giro_min=&p_capital_giro_max=&chk_lst%5B%5D=itm25&EV_EBIT_min=&EV_EBIT_max=&chk_lst%5B%5D=itm26&EV_EBITDA_min=&EV_EBITDA_max=&chk_lst%5B%5D=itm27&EV_receita_liquida_min=&EV_receita_liquida_max=&chk_lst%5B%5D=itm28&EV_FCO_min=&EV_FCO_max=&chk_lst%5B%5D=itm29&EV_FCF1_min=&EV_FCF1_max=&chk_lst%5B%5D=itm30&EV_ativo_total_min=&EV_ativo_total_max=&chk_lst%5B%5D=itm31&div_yield_min=&div_yield_max=&chk_lst%5B%5D=itm32&vol_financ_min=&vol_financ_max=&chk_lst%5B%5D=itm33&market_cap_min=&market_cap_max=&chk_lst%5B%5D=itm34&setor=',
        'file_name': f'InvestSite_{date.today()}.csv',
        'type': 'generate'
    }
]


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
        self.df = self.df.rename(columns={'p': 'cotacao'})
        self.df = self.df.rename(columns={'p_lucro': 'p_l'})
        self.df = self.df.rename(columns={'p_vpa': 'p_vp'})
        self.df = self.df.rename(columns={'p_rec_líq_': 'psr'})
        self.df = self.df.rename(columns={'p_ativo_total': 'p_ativo'})
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