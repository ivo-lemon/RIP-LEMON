from fireblocks_sdk import FireblocksSDK, TransferPeerPath, DestinationTransferPeerPath, TRANSACTION_STATUS_CONFIRMED, TRANSACTION_STATUS_CANCELLED, TRANSACTION_STATUS_REJECTED, TRANSACTION_STATUS_FAILED, VAULT_ACCOUNT, TRANSACTION_MINT, TRANSACTION_BURN

apiSecret = open('star_lemon_me.key', 'r').read()
apiKey = '42558917-bf48-ed5a-633c-8e714ff0e6b7'
fireblocks = FireblocksSDK(apiSecret, apiKey)

# -- SUPPORTED ASSETS --
supported_assets = fireblocks.get_supported_assets()

# print(supported_assets)

# Filter specific asset:

a = [s_a for s_a in supported_assets if "ETH" in s_a["id"]]
for x in a:
    print(x)


