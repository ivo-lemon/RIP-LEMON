import requests
import json

# Creates filters to be added to LotSizeFilter.kt MarketLotSizeFilter.kt MinNotionalFilter.kt
# lemon_symbols.json last updated @11-03-2022

filters = [ 'LOT_SIZE', 'MARKET_LOT_SIZE', 'MIN_NOTIONAL' ]
def mapDictToListOfDicts(dictionary):
    retDict = {}
    retDict['symbol'] = dictionary['symbol']
    retDict['baseAsset'] = dictionary['baseAsset']
    retDict['quoteAsset'] = dictionary['quoteAsset']
    retDict['filters'] = list(filter(lambda x: x['filterType'] in filters , dictionary['filters']))
    return retDict

lemon_symbols = [
'ETHBTC', 'BNBBTC', 'BNBETH', 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'MANABTC', 
'MANAETH', 'ADABTC', 'ADAETH', 'ADAUSDT', 'ADABNB', 'BNBUSDC', 'BTCUSDC', 
'ETHUSDC', 'USDCUSDT', 'ADAUSDC', 'MATICBNB', 'MATICBTC', 'MATICUSDT', 'ATOMBNB', 
'ATOMBTC', 'ATOMUSDT', 'ATOMUSDC', 'FTMBNB', 'FTMBTC', 'FTMUSDT', 'ALGOBNB', 'ALGOBTC', 
'ALGOUSDT', 'SOLBNB', 'SOLBTC', 'SOLUSDT', 'MANAUSDT', 'BTCDAI', 'ETHDAI', 'BNBDAI', 'USDTDAI', 
'SANDBNB', 'SANDBTC', 'SANDUSDT', 'DOTBNB', 'DOTBTC', 'DOTUSDT', 'LUNABNB', 'LUNABTC', 'LUNAUSDT', 
'UNIBNB', 'UNIBTC', 'UNIUSDT', 'AVAXBNB', 'AVAXBTC', 'AVAXUSDT', 'CAKEBNB', 'AXSBNB', 'AXSBTC', 
'AXSUSDT', 'SLPETH', 'CAKEBTC', 'CAKEUSDT', 'SLPUSDT', 'SOLUSDC', 'AXSETH', 'FTMETH', 
'SOLETH', 'SANDETH', 'DOTETH', 'MATICETH', 'AVAXETH', 'MANABNB', 'LUNAETH', 'USTBTC', 
'USTUSDT', 'ATOMETH', 'LUNAUST', 'ETHUST', 'UNIETH', 'BNBUST', 'SLPBNB'] # Add new pairs here

binance_url = 'https://api.binance.com/api/v3/exchangeInfo'
binance_response = requests.get(binance_url)

if binance_response.status_code == 200:
    json_str = binance_response.text
    json_binance_filters = json.loads(json_str)
    binance_symbols = json_binance_filters["symbols"]
    binance_lemon_symbols = list(filter(lambda x: x['symbol'] in lemon_symbols, binance_symbols))
    binance_lemon_symbol_filters = list(map(mapDictToListOfDicts, binance_lemon_symbols))

    # Open Files
    lotSizeFilterFile = open('LotSizeFilterMap.kt', 'w')
    marketlotSizeFilterFile = open('MarketLotSizeFilter.kt', 'w')
    minNotionalFilterFile = open('MinNotionalFilter.kt', 'w')

    #Print Headers
    print('private val filterMap = mapOf(', file = lotSizeFilterFile)
    print('private val filterMap = mapOf(', file = marketlotSizeFilterFile)
    print('private val filterMap = mapOf(', file = minNotionalFilterFile)
    
    #Set First
    first = True

    # Loop over the filters
    for lemon_filter in binance_lemon_symbol_filters:

        # Get Filters
        lotSizeFilter = list(filter(lambda x: x['filterType'] == 'LOT_SIZE' , lemon_filter['filters']))[0]
        marketLotSizeFilter = list(filter(lambda x: x['filterType'] == 'MARKET_LOT_SIZE' , lemon_filter['filters']))[0]
        minNotionalFilter = list(filter(lambda x: x['filterType'] == 'MIN_NOTIONAL' , lemon_filter['filters']))[0]

        #Get Instrument
        instrument = lemon_filter['baseAsset'] + "_" + lemon_filter['quoteAsset']

        #If first line then do nothing, else print close of object
        if first:
            first = False
        else:
            print("    ),", file = lotSizeFilterFile)
            print("    ),", file = marketlotSizeFilterFile)
            print("    ),", file = minNotionalFilterFile)

        # LotSizeFilter
        print("    Instrument." + instrument + " to LotSizeFilterData(", file = lotSizeFilterFile) 
        print("        minQty = BigDecimal(\"" + str(lotSizeFilter['minQty']) + "\"),", file = lotSizeFilterFile) 
        print("        maxQty = BigDecimal(\"" + str(lotSizeFilter['maxQty']) + "\"),", file = lotSizeFilterFile) 
        print("        stepSize = BigDecimal(\"" + str(lotSizeFilter['stepSize']) + "\")", file = lotSizeFilterFile) 

        # MarketLotSizeFilter
        print("    Instrument." + instrument + " to MarketLotSizeFilterData(", file = marketlotSizeFilterFile) 
        print("        minQty = BigDecimal(\"" + str(marketLotSizeFilter['minQty']) + "\"),", file = marketlotSizeFilterFile) 
        print("        maxQty = BigDecimal(\"" + str(marketLotSizeFilter['maxQty']) + "\"),", file = marketlotSizeFilterFile) 
        print("        stepSize = BigDecimal(\"" + str(marketLotSizeFilter['stepSize']) + "\")", file = marketlotSizeFilterFile) 

        # MinNotionalFilter
        print("    Instrument." + instrument + " to MinNotionalFilterData(", file = minNotionalFilterFile) 
        print("        minNotional = BigDecimal(\"" + str(minNotionalFilter['minNotional']) + "\"),", file = minNotionalFilterFile) 
        print("        applyToMarket = " + str(minNotionalFilter['applyToMarket']).lower() + ",", file = minNotionalFilterFile) 
        print("        avgPriceMins = " + str(minNotionalFilter['avgPriceMins']), file = minNotionalFilterFile) 

    #Print last closing object
    print("    )", file = lotSizeFilterFile) 
    print("    )", file = marketlotSizeFilterFile) 
    print("    )", file = minNotionalFilterFile) 
    
    #Print closing map
    print(")", file = lotSizeFilterFile) 
    print(")", file = marketlotSizeFilterFile) 
    print(")", file = minNotionalFilterFile) 

    #Close Files
    lotSizeFilterFile.close()
    marketlotSizeFilterFile.close()
    minNotionalFilterFile.close()
else:
    print("Call to " + binance_url + " failed")

