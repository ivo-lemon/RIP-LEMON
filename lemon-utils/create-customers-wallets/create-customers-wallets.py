lines_per_file = 50000
lemon_currencies = ['NEAR', 'SHIB', 'ENS', 'DOGE', 'TRX']


def obtain_customer_values(line):
    separated_line = line.split(",")
    return separated_line[0].strip().replace('"', ''), separated_line[1].strip()


def print_headers(file):
    print("INSERT IGNORE INTO Wallets (balance, createdAt, updatedAt, UserId, AssetTypeId, account_id)", file=file)
    print("VALUES", file=file)


def print_value_line(value_line, line_end, file):
    print("{}{}".format(value_line, line_end), file=file)


def generate_sql_wallets_file2():
    accounts_file = open('accounts.csv', 'r')
    line_number = 0
    file_number = 0
    sql_file = open('wallets-{}.sql'.format(file_number), 'w')
    print_headers(sql_file)
    value_line = None
    for line in accounts_file:
        account_id, customer_id = obtain_customer_values(line)
        for currency in lemon_currencies:
            if value_line:
                if line_number % lines_per_file == 0:
                    print_value_line(value_line, ';', sql_file)
                    sql_file.close()
                    file_number += 1
                    sql_file = open('wallets-{}.sql'.format(file_number), 'w')
                    print_headers(sql_file)
                else:
                    print_value_line(value_line, ',', sql_file)
            value_line = "(0, now(), now(), {}, '{}', '{}')".format(customer_id, currency, account_id)
            line_number += 1
    print_value_line(value_line, ';', sql_file)
    sql_file.close()
    accounts_file.close()


if __name__ == '__main__':
    generate_sql_wallets_file2()
