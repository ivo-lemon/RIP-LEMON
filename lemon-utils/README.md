In this repository we will have all projects related to utilities used in Lemon
In general these are short scripts related to specific tasks.
These will be listed in this README for further information please scroll to the project of your interest

## Crypto Account Creator

This is a Python 3.8 based project that consists of a script that given a csv of account ids, creates crypto accounts for each account id by calling an endpoint exposed by Lemon's server

### Introduction

This project runs a virtual environment on your local machine. To run it follow this instructions

1. Clone the repository
2. Run ```init.sh```
   This bash file will create a virtual environment, upgrades all the needed libraries to the latest stable version and then installs them in the newly created virtual environment. The list of used libraries can be found in requirements.txt
3. Before running the script _always make sure to activate the virtual environment_, you can do this simply by doing
   Run ```source activate.sh```
4. Load your csv file containing the account ids to the data folder and create a .env file with the corresponding environment variables
5. Always ensure you are running with Python3.8, to run the project simply call ```python3 crypto-account-creator.py```

If you need to add new libraries on the fly, remember to _activate_ your virtual environtment and add them to the requirements.txt file. To install simply run:
```pip3 install -r requirements.txt```

## Binance Filter Checker

### Introduction

This project executes two json files that hold the information regarding to Binance filters and Lemon filters.
The script must be run using python 3 and the output will consist of printed lines containing the differences

### Context

Binance has several filters that are associated with orders made, these for the moment are only 3 for market orders and therefore
are the only ones that need to be maintained. For more information, please visit: https://binance-docs.github.io/apidocs/spot/en/#filters

Lemon copies these filters and applies them before doing the request, therefore the request to trade cryptocurrencies is not done if the filter logic fails. 
These filters should be adjusted to match Binance filters, because if not, trades that could be correctly made on Binance could not be made because Lemon expects them to fail

### Procedure

The idea behind this is then to first load the json files for ```binance_filters.json``` with the data obtained when using ```GET /api/v3/exchangeInfo```. This json will contain more data than needed, but what interests us is the filter data embedded in it.
(For further references see: https://binance-docs.github.io/apidocs/spot/en/#exchange-information)

After that we should load the ```lemon_filters.json``` file to have the latest data that is available on Lemon's server

Then running the script will give us the differences, if there are any

## Binance Get Token Pairs

### Introduction

This project executes a json file that holds the information regarding Binance symbols (pairs of currencies that are available to trade)
The script must be run using python 3 and the output will consist of printed lines containing a JSON with the pairs that can be traded

### Procedure
Load the ```binance_filters.json``` with the data obtained from ```GET /api/v3/exchangeInfo``` on Binance API. 
If you need new coins then add them in the currency array in the code. After running the script all possible trading symbols will be printed as a json
(For further references see: https://binance-docs.github.io/apidocs/spot/en/#exchange-information)
