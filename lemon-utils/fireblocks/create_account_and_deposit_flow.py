from fireblocks_sdk import FireblocksSDK, TransferPeerPath, DestinationTransferPeerPath, TRANSACTION_STATUS_CONFIRMED, TRANSACTION_STATUS_CANCELLED, TRANSACTION_STATUS_REJECTED, TRANSACTION_STATUS_FAILED, VAULT_ACCOUNT, TRANSACTION_MINT, TRANSACTION_BURN

apiSecret = open('star_lemon_me.key', 'r').read()
apiKey = apikey
fireblocks = FireblocksSDK(apiSecret, apiKey)

# vault_accounts = fireblocks.get_vault_accounts()

# --- ACCOUNT CREATION ---
# customerRefId parameter not working (not recognized).
new_vault_account = fireblocks.create_vault_account(name = "uri", hiddenOnUI = False, autoFuel=False)
print(new_vault_account)

#vault_account_id = new_vault_account["id"]


# print(vault_accounts)