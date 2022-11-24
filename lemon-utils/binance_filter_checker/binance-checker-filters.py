import json

# Save data collected from /exchangeInfo in Binance in the binance_filters.json file
# Save data collected from the Filter classes in Lemeons server in the lemon_filters.json file
# Execute the file to find out if there are differences between Lemon and Binance in regards to filter errors
# If there are then adjust Lemon's by updating the corresponding classes
# bleh to merge
# binance_filters.json last updated @19-11-2021 11:43 UTC+1
# lemon_filters.json last updated @19-11-2021 11:43 UTC+1
def mapDictToListOfDicts(dictionary):
    retDict = {}
    retDict['symbol'] = dictionary['symbol']
    retDict['filters'] = dictionary['filters']
    return retDict

lemon_symbols = ['BTCUSDT', 'ETHUSDT', 'UNIUSDT', 'DAIUSDT', 'ADAUSDT',
'BTCDAI', 'ETHDAI', 'ETHBTC', 'USDTDAI', 'UNIBTC', 'ADABTC', 'ADAETH',
'SOLETH', 'SOLUSDT', 'SOLBTC', 'SLPUSDT', 'SLPETH', 'AXSETH', 'AXSUSDT',
'AXSBTC', 'SANDUSDT', 'SANDBTC', 'MANAUSDT', 'MANAETH', 'MANABTC',
'ALGOBTC', 'ALGOUSDT', 'MATICBTC', 'MATICUSDT', 'MATICETH', 'DOTBTC',
'DOTUSDT', 'DOTETH', 'USDCUSDT', 'BTCUSDC', 'ETHUSDC', 'ADAUSDC',
'SOLUSDC'] # Add new pairs here

binance_filters = open('binance_filters.json')
json_str = binance_filters.read()
json_binance_filters = json.loads(json_str)
binance_symbols = json_binance_filters["symbols"]
#print(binance_symbols)
binance_lemon_symbols = list(filter(lambda x: lemon_symbols.__contains__(x['symbol']), binance_symbols))

binance_lemon_symbol_filters = list(map(mapDictToListOfDicts, binance_lemon_symbols))

lemon_filter_file = open('lemon_filters.json')
lemon_filters = lemon_filter_file.read()
json_lemon_filters = json.loads(lemon_filters)

for symbol_filters in json_lemon_filters:
    symbol = symbol_filters["symbol"]
    filters = symbol_filters["filters"]
    binance_filter_list = list(filter(lambda x: symbol == x["symbol"], binance_lemon_symbol_filters))
    if(len(binance_filter_list) == 0):
        print(f'Update binance_filter.json or remove the symbol: {symbol} that does not exist on Binance!')
        continue

    print(binance_filter_list[0]['symbol'])
    binance_filters = list(map(lambda x: x["filters"], binance_filter_list))[0]
    for lemon_filter in filters:
        binance_filter = list(filter(lambda x: x["filterType"] == lemon_filter["filterType"], binance_filters))[0]
        #print(lemon_filter["filterType"])
        #print(binance_filter["filterType"])
        #print(binance_filter == lemon_filter)
        if (binance_filter != lemon_filter):
            print(f"Values for symbol: {symbol} and filter: {lemon_filter['filterType']} are outdated")
        else:
            print(f"No differences for symbol {symbol} and filter {lemon_filter['filterType']}")