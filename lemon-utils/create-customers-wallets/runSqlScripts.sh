#!/bin/bash

cd `dirname $0`

script_usage () {
    echo "Usage:	./runSqlScripts.sh"
    echo ""
    echo "Arguments:"
    echo "	  --help	display script usage"
    echo "	  -h		display script usage"
    echo ""
    echo "  accounts.csv file should exist."
    echo "  To retrieve it export Accounts Table from Kore Database or redshift"
    echo "  accountId and userId should be in first and second columns"
    echo ""
    echo "  Set Wallet currencies and sql files size in create-customers-wallets.py"
    echo ""
    echo "  database_config.sh file should exist defining DATABASE environment variables"
    echo "  Example:"
    echo "    DATABASE_ENDPOINT=localhost"
    echo "    DATABASE_PORT=3306"
    echo "    DATABASE_USER=root"
    echo "    DATABASE_PASSWORD=1234"
    echo "    DATABASE_NAME=lemoncash_ar"
}

if [ "${1}" == "--help" ] || [ "${1}" == "-h" ]
then
	script_usage
	exit 0
fi

if [ ! -f accounts.csv ]; then
    echo "ERROR: accounts.csv does not exist on your filesystem."
    echo "Use --help or -h to see script usage"
    exit 1
fi

if [ ! -f database_config.sh ]; then
    echo "ERROR: database_config.sh does not exist on your filesystem."
    echo "Use --help or -h to see script usage"
    exit 1
fi

echo "Remove old sql files"
rm -f *.sql
python3 create-customers-wallets.py

. ./database_config.sh

FULL_START=$(date +%s)
for file in $(ls -1tr wallets-*.sql); do
  START=$(date +%s)
  echo ""
  echo "Start Processing File $file"
  mysql -h ${DATABASE_ENDPOINT} -P ${DATABASE_PORT} -u ${DATABASE_USER} -p${DATABASE_PASSWORD} ${DATABASE_NAME} < ${file}
  sleep 1
  END=$(date +%s)
  DIFF=$(echo "$END - $START" | bc)
  echo "End File $file - Duration ${DIFF}"
done
FULL_END=$(date +%s)
FULL_DIFF=$(echo "$FULL_END - $FULL_START" | bc)
echo ""
echo "Process Finished - Duration ${FULL_DIFF}"

echo "Remove sql files"
rm *.sql

exit 0
