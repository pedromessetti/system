<h1 align="center">
    Investiment Analysis System
</h1>

## Prerequisites

Before running the application, ensure that you have the following prerequisites installed:

- Python 3.10
- BeautifulSoup
- Requests
- Shutil
- MySQL
- Pandas

You can install the dependencies by running the following command:

    pip3 install bs4 requests shutils mysql mysql-connector pandas

## Usage

To use the script, follow these steps:

1. Clone the repository:

        git clone https://github.com/pedromessetti/system.git

2. `cd` to the project directory

3. Open the `store.py` file and modify the following line with your MySQL user and password
        
        class Database:
                def __init__(self):
                self.user='your_user' # put your user here
                self.password='your_password' # put your password here

4. Run the scraper:

        python3.10 scraper.py

5. Store data in the database:

        python3.10 store.py

OBS: At first running you be asked to put your root password for create database, table and grant all privileges to your user

## License

This project is licensed under the [MIT License](LICENSE).

## Contributing

If you encounter any issues or have suggestions for improvement, please open an issue.

## Author
| [<img src="https://avatars.githubusercontent.com/u/105685220?v=4" width=115><br><sub>Pedro Vinicius Messetti</sub>](https://github.com/pedromessetti) |
|:---------------------------------------------------------------------------------------------------------------------------------------------------: |