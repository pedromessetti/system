# -*- coding: iso-8859-1 -*-

from bs4 import BeautifulSoup
import common as c
import requests
import csv
import os


class Scraper:
    def __init__(self, url, file_name):
        self.url = url
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        self.file_name = file_name
        try:
            self.response = requests.get(self.url, headers=self.headers)
        except requests.exceptions.ConnectionError as error:
            print(f'{c.CROSSMARK}{self.file_name}\nError: {c.ENDC}{error}')

    
    def run(self, type):
        if type == 'download':
            self.download_csv()
        if type == 'generate':
            self.generate_csv()


    def generate_csv(self):
        try:
            if self.response.status_code == 200:
                soup = BeautifulSoup(self.response.content, 'html.parser')

                with open(self.file_name, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    for tr in soup.find_all('tr'):
                        data = []
                        for th in tr.find_all('th'):
                            data.append(th.text.strip().lower().replace('.',' ').replace(' ', '_').replace('/', '_').replace('preço', 'p'))
                        if data:
                            writer.writerow(data)
                            continue
                        data = [td.text.strip().replace('%', '').replace('.', '').replace(',', '.').replace('NA', '0') for td in tr.find_all('td')]
                        try:
                            writer.writerow([float(val) if val else None for val in data])
                        except ValueError:
                            writer.writerow(data)

                if self.file_name == f'InvestSite_{c.date.today()}.csv':
                    with open(self.file_name, 'r') as csv_file:
                        reader = csv.reader(csv_file)
                        rows = list(reader)[6:]

                    with open(self.file_name, 'w', newline='') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerows(rows)

                c.shutil.move(self.file_name, 'csv/' + self.file_name)
                print(f'{c.CHECKMARK}{self.file_name}{c.ENDC}')
            else:
                print(f'{c.CROSSMARK}{self.file_name}\nError: Response Status Code {c.BOLD}{self.response.status_code}{c.ENDC}')
        except Exception as error:
            print(f'{c.CROSSMARK}{self.file_name}\nError: {c.ENDC}{error}')


    def download_csv(self):
        try:
            if self.response.status_code == 200:
                with open(self.file_name, 'wb') as csv_file:
                    csv_file.write(self.response.content)

                with open(self.file_name, 'r') as csv_file:
                    reader = csv.reader(csv_file, delimiter=';')
                    modified_rows = []
                    for i, row in enumerate(reader):
                        if i == 0:
                            modified_rows.append([cell.strip().lower().replace('.', '').replace(' ', '_').replace('/', '_') for cell in row])
                        else:
                            modified_rows.append([cell.replace('.', '').replace(',', '.') if cell else '0' for cell in row])

                with open(self.file_name, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerows(modified_rows)

                c.shutil.move(self.file_name, 'csv/' + self.file_name)
                print(f'{c.CHECKMARK}{self.file_name}{c.ENDC}')
            else:
                print(f'{c.CROSSMARK}{self.file_name}\nError: Response Status Code {c.BOLD}{self.response.status_code}{c.ENDC}')
        except Exception as error:
            print(f'{c.CROSSMARK}{self.file_name}\nError: {c.ENDC}{error}')


if __name__ == '__main__':
    if not os.path.exists('csv'):
        os.mkdir('csv')
        print(f'{c.CHECKMARK}CSV directory created{c.ENDC}\n')
    else:
        for file in os.listdir('./csv'):
            if not file.endswith(f'_{c.date.today()}.csv'):
                os.remove(f'./csv/{file}')
                print(f'{c.CROSSMARK}Removed {file}{c.ENDC}\n')
    for source in c.sources:
        Scraper(source['url'], source['file_name']).run(source['type'])
    c.shutil.rmtree('__pycache__')
