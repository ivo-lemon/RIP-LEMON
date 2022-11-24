import requests
import os
from tqdm import tqdm
import random
import pandas as pd
from dotenv import load_dotenv
from time import sleep, time
import csv

load_dotenv()  # take environment variables from .env.
SERVER_URL = os.getenv('SERVER_URL')
USER_VALIDATOR_ROLE_TOKEN = os.getenv('USER_VALIDATOR_ROLE_TOKEN')
TIMEOUT_IN_SECONDS = os.getenv('TIMEOUT_IN_SECONDS')

print(SERVER_URL)
print(USER_VALIDATOR_ROLE_TOKEN)
print(TIMEOUT_IN_SECONDS)

accountIdsDf = pd.read_csv('data/account_ids.csv')
accountIds = accountIdsDf['account_id'].to_list()

with open('output/failed_account_ids.csv', 'a') as file:
    writer = csv.writer(file)
    for accountId in tqdm(accountIds):
        try:
            sleep(int(TIMEOUT_IN_SECONDS)) # Time in seconds
            response = requests.post(
                url = f'{SERVER_URL}/priv/crypto-vault-accounts/{accountId}', 
                headers = {
                    'Authorization': f'Bearer {USER_VALIDATOR_ROLE_TOKEN}'
                }
            )
            response.raise_for_status()
            print(f'Crypto account for account id: {accountId} succesfully created')

        except requests.exceptions.RequestException as err:
            print(f'An error ocurred while creating crypto vault account for account with id: {accountId}\nError: {err}')
            writer.writerow([accountId])
        else:
            print(f'Successfully created crypto vault account for account with id: {accountId}')

quotes = [
	'No hay nada más permanente, que una solución temporal',
	"Mucho cacique y poca flecha",
	"Músculo interno y cerebro externo",
	"Me siento scammeado",
	"Qué lindo día para vapear",
	"Me dan asco las vacaciones",
	"Con ese balance falopa no pueden haber pasado un due dilligence",
	"Te la agitó el backfront",
	"Los proyectos no se terminan, se abandonan",
]

print(random.choice(quotes))