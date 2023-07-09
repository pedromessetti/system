<h1 align="center">
    Investiment Analysis System
</h1>

<p>
The Investment Analysis System is a Python application that collects stock data from various financial websites and generates CSV files for analysis. It uses web scraping techniques to gather data from the following sources:

- [Fundamentus](https://www.Fundamentus.com.br/resultado.php): Scrapes fundamental stock data from Fundamentus and generates a `Fundamentus.csv` file.
- [Status Invest](https://statusinvest.com.br/): Downloads a CSV file from Status Invest containing advanced search results and saves it as `status_invest.csv`.
- [Invest Site](https://www.investsite.com.br/): Scrapes stock selection data from Invest Site and generates a `invest_site.csv` file.
</p>

## Index
- [Index](#index)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [License](#license)
- [Learnings](#learnings)
- [Contributing](#contributing)
- [Author](#author)

## Prerequisites

Before running the application, ensure that you have the following prerequisites installed:

- Python 3.10
- BeautifulSoup
- Requests
- Shutil
- MySQL

You can install the dependencies by running the following command:

    pip3 install bs4 requests shutils mysql mysql-connector pandas

## Usage

To use the script, follow these steps:

1. Clone the repository:

        git clone https://github.com/pedromessetti/investment-analysis-system.git

2. `cd` to the project directory

3. Run the scraper:

        python3.10 scraper.py

4. Store the data:

        python3.10 store.py

This will execute the scraping functions and store the necessary CSV files in the `csv` directory.

## License

This project is licensed under the [MIT License](LICENSE).

## Learnings

The project was developed using Python 3.10 and various libraries. Here are some learnings that I have with the following tools:

- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/): A library for parsing HTML and XML.
- [Requests](https://docs.python-requests.org/): A library for making HTTP requests.

## Contributing

If you encounter any issues or have suggestions for improvement, please open an issue.

## Author
| [<img src="https://avatars.githubusercontent.com/u/105685220?v=4" width=115><br><sub>Pedro Vinicius Messetti</sub>](https://github.com/pedromessetti) |
|:---------------------------------------------------------------------------------------------------------------------------------------------------: |