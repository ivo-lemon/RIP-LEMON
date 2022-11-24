import requests
import os
import random
import pandas as pd
from dotenv import load_dotenv
from time import sleep
import csv

load_dotenv()  # take environment variables from .env.
SERVER_URL = os.getenv('SERVER_URL')
USER_CRYPTO_ROLE_TOKEN = os.getenv('USER_CRYPTO_ROLE_TOKEN')
TIMEOUT_IN_SECONDS = os.getenv('TIMEOUT_IN_SECONDS')

print(SERVER_URL)
print(USER_CRYPTO_ROLE_TOKEN)
print(TIMEOUT_IN_SECONDS)

transactionsDf = pd.read_csv('data/transactions.csv')

with open('output/failed_transactions.csv', 'a') as file:
    writer = csv.writer(file)
    for index, row in transactionsDf.iterrows():
        sleep(int(TIMEOUT_IN_SECONDS)) # Time in seconds
        try:
            requestBody = {
                "amount": row['amount'],
                "currency": row['currency'],
                "network": row['network'],
                "crypto_account_id": row['crypto_account_id'],
                "external_id": row['external_id'],
                "origin_address": row['origin_address'],
                "destination_address": row['destination_address'],
                "destination_address_tag": row['destination_address_tag']
            }

            response = requests.post(
                url = f'{SERVER_URL}/priv/cash-in-crypto', 
                headers = {
                    'Authorization': f'Bearer {USER_CRYPTO_ROLE_TOKEN}'
                },
                body = requestBody
            )

            response.raise_for_status()
            print(f'Cash in crypto transaction with crypto account id {row["crypto_account_id"]} and external id {row["external_id"]} succesfully created')
        except requests.exceptions.RequestException as err:
            print(f'An error ocurred while creating cash in crypto transaction for crypto account with id: {row["crypto_account_id"]}\nError: {err}')
            writer.writerow([row['crypto_account_id'], row['external_id']])
       
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