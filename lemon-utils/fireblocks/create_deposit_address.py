from fireblocks_sdk import FireblocksSDK, TransferPeerPath, DestinationTransferPeerPath, TRANSACTION_STATUS_CONFIRMED, TRANSACTION_STATUS_CANCELLED, TRANSACTION_STATUS_REJECTED, TRANSACTION_STATUS_FAILED, VAULT_ACCOUNT, TRANSACTION_MINT, TRANSACTION_BURN

apiSecret = open('star_lemon_me.key', 'r').read()
apiKey = apikey
fireblocks = FireblocksSDK(apiSecret, apiKey)

try:
    address = fireblocks.generate_new_address("7", "BTC_TEST")
    print(address)

except Exception as e:
    print(e)
