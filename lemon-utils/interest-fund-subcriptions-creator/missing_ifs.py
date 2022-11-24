# ---------------------------------------
# | Interest fund subscriptions creator |
# ---------------------------------------
# 
# This script creates all the missing interest fund subscriptions.
# As input needs a CSV with all the missing rows to insert.
# The SQL queries to get that CSV are described below.
#
# 
# SQL queries:
# ------------
#
# To check how many subscriptions are missing per currency: 
#   select
#   	w.assettypeid,
#   	count(*)
#   from lemoncash_ar.Wallets w
#   join lemoncash_ar.InterestFunds if2 on w.AssetTypeId = if2.currency
#   left join lemoncash_ar.InterestFundSubscriptions ifs on w.id = ifs.wallet_id
#   where w.balance > 0
#   and ifs.wallet_id is null
#   and cast(if2.apy as float) > 0
#   group by w.assettypeid
#
# To get the results for the CSV:
#   with users_with_earn_disabled as (
#     select w.userid
#     from lemoncash_ar.wallets w
#     join lemoncash_ar.interestfundsubscriptions ifs on w.id = ifs.wallet_id 
#     group by userid 
#     having sum(case when not ifs.enabled then 1 else 0 end) = count(*)
#   )
#   select
#     'replace_me' as id,
#     if2.id as interest_fund_id,
#     w.id as wallet_id,
#     case when w.userid in (select * from users_with_earn_disabled) then 0 else 1 end as enabled,
#     getdate() as createdAt,
#     getdate() as updatedAt
#   from lemoncash_ar.Wallets w
#   join lemoncash_ar.InterestFunds if2 on w.AssetTypeId = if2.currency
#   left join lemoncash_ar.InterestFundSubscriptions ifs on w.id = ifs.wallet_id
#   where cast(w.balance as float) > 0
#   and cast(if2.apy as float) > 0
#   and ifs.id is null
# 
# 
# Dependencies:
# -------------
# pip install mysql-connector-python
#

import mysql.connector
import csv
import uuid

mydb = mysql.connector.connect(
  host="XXX",
  user="XXX",
  password="XXX",
  database="lemoncash_ar"
)

filename = 'subscriptions.csv'
with open(filename) as f:
    lines = sum(1 for _ in f) - 1

csvreader = csv.reader(open(filename))
header_row = next(csvreader)
headers = {}
for i, header in enumerate(header_row):
  headers[header] = i

mycursor = mydb.cursor()
sql = """
  INSERT INTO InterestFundSubscriptions (id, interest_fund_id, wallet_id, enabled, created_at, updated_at)
  VALUES (%s, %s, %s, %s, %s, %s)
"""
for row in csvreader:
  try:
    values = (
      f"{uuid.uuid4()}",
      row[headers["interest_fund_id"]],
      row[headers["wallet_id"]],
      row[headers["enabled"]],
      row[headers["createdat"]],
      row[headers["updatedat"]]
    )
    mycursor.execute(sql, values)
    print(f"{csvreader.line_num - 1}/{lines}")
    mydb.commit()
  except:
    print("Error on wallet_id", headers["wallet_id"])

print("Done!")