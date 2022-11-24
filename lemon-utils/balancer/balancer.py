import requests
import os
import random
import pandas as pd
from dotenv import load_dotenv
from time import sleep
import csv

load_dotenv()  # take environment variables from .env.
SERVER_URL = os.getenv('SERVER_URL')
TIMEOUT_IN_SECONDS = os.getenv('TIMEOUT_IN_SECONDS')

print(SERVER_URL)
print(TIMEOUT_IN_SECONDS)

transactionsDf = pd.read_csv('data/cash_in_not_balanced.csv')
path = '/sapi/v1/broker/universalTransfer';

with open('output/failed_transactions.csv', 'a') as file:
    writer = csv.writer(file)
    for index, row in transactionsDf.iterrows():
        sleep(int(TIMEOUT_IN_SECONDS)) # Time in seconds

        url = f'{SERVER_URL}{path}'
        accountType = 'SPOT'

        params = {
            "fromId": row['subaccount_id'],
            "fromAccountType": accountType,
            "toAccountType": accountType,
            # Assumes that Binance coin should be ok to request Binance again for withdrawal
            "asset": row['coin'],
            # Assumes that Binance amount deposited should be ok to request Binance again for withdrawal
            "amount": row['amount'],
        }

        print(params)
        try:
            response = requests.post(
                url = url, 
                data = params
            )

            print(response)
            response.raise_for_status()
            print(f'Cash in crypto transaction with crypto account id {row["subaccount_id"]} and tx id {row["transaction_id"]} succesfully sent')
        except requests.exceptions.RequestException as err:
            print(f'An error ocurred while balancing cash in crypto transaction for crypto account with id: {row["subaccount_id"]}\nError: {err}')
            writer.writerow([row['subaccount_id'], row['transaction_id']])
        
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