from fireblocks_sdk import FireblocksSDK, TransferPeerPath, DestinationTransferPeerPath, TRANSACTION_STATUS_CONFIRMED, TRANSACTION_STATUS_CANCELLED, TRANSACTION_STATUS_REJECTED, TRANSACTION_STATUS_FAILED, VAULT_ACCOUNT, TRANSACTION_MINT, TRANSACTION_BURN

apiSecret = open('star_lemon_me.key', 'r').read()
apiKey = apikey
fireblocks = FireblocksSDK(apiSecret, apiKey)

vaultAsset = fireblocks.create_vault_asset("7", "BTC_TEST")

print(vaultAsset)