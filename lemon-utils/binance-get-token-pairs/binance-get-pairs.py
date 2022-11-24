import requests
import json

# Gets all trading pairs listed on Binance for a list of currencies
binance_url = 'https://api.binance.com/api/v3/exchangeInfo'

lemon_currencies = [
'BTC', 'ETH', 'USDT', 'ADA', 
'DAI', 'UNI', 'AXS', 'SAND', 
'MANA', 'SLP', 'SOL', 'ALGO', 
'DOT', 'MATIC', 'USDC', 'UST', 
'LUNA', 'BNB', 'CAKE', 'AVAX', 'ATOM', 'FTM'] # Add new currencies here

def filterDict(dictionary):
    retDict = {}
    retDict['symbol'] = dictionary['symbol']
    retDict['status'] = dictionary['status']
    return retDict

lemon_currencies = [
'BTC', 'ETH', 'USDT', 'ADA', 
'DAI', 'UNI', 'AXS', 'SAND', 
'MANA', 'SLP', 'SOL', 'ALGO', 
'DOT', 'MATIC', 'USDC', 'UST', 
'LUNA', 'BNB', 'CAKE', 'AVAX', 'ATOM', 'FTM'] # Add new currencies here

binance_response = requests.get(binance_url)
if binance_response.status_code == 200: 
    json_str = binance_response.text
    json_binance_filters = json.loads(json_str)
    binance_symbols = json_binance_filters["symbols"]
    binance_lemon_symbols = list(filter(lambda x: x['baseAsset'] in lemon_currencies and x['quoteAsset'] in lemon_currencies , binance_symbols))
    binance_lemon_symbols = list(filter(lambda x: x['status'] == 'TRADING', binance_lemon_symbols))
    binance_lemon_symbol_simplified = list(map(filterDict, binance_lemon_symbols))
    binance_lemon_pairs = list(map(lambda x : x['symbol'], binance_lemon_symbol_simplified))
    print(json.dumps(binance_lemon_pairs, sort_keys=True))
else:
    print("Call to " + binance_url + " failed")