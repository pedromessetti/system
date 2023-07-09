import json

CONFIG_FILE = 'config.json'


def save_config(data):
    with open('config.json', 'w') as f:
        json.dump(data, f)


def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def is_first_run():
    config = load_config()
    return not config.get('initialized', False)


def is_initialized():
    try:
        with open(CONFIG_FILE, 'r') as file:
            config_data = json.load(file)
            return config_data.get('initialized', False)
    except FileNotFoundError:
        return False


def set_initialized():
    config_data = {'initialized': True}
    with open(CONFIG_FILE, 'w') as file:
        json.dump(config_data, file)
