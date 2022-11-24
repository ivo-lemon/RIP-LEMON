from fireblocks_sdk import FireblocksSDK, TransferPeerPath, DestinationTransferPeerPath, TRANSACTION_STATUS_CONFIRMED, TRANSACTION_STATUS_CANCELLED, TRANSACTION_STATUS_REJECTED, TRANSACTION_STATUS_FAILED, VAULT_ACCOUNT, TRANSACTION_MINT, TRANSACTION_BURN

apiSecret = open('star_lemon_me.key', 'r').read()
apiKey = apikey
fireblocks = FireblocksSDK(apiSecret, apiKey)

vault_account = fireblocks.get_vault_account("7")

print(vault_account)

depositAddresses = fireblocks.get_deposit_addresses("7", "BNB_TEST")

print("Address to deposit: ", depositAddresses)
