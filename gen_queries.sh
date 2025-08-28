#!/bin/bash

OUTPUT_FILE="queries.txt"
NUM_QUERIES=1000000

if ! command -v uuidgen &> /dev/null; then
    echo "Error: Command 'uuidgen' not found."
    echo "Please install 'uuid-runtime' (apt-get install uuid-runtime) or similar package."
    exit 1
fi

> "$OUTPUT_FILE"

echo "Creating ${NUM_QUERIES} unique queries into file ${OUTPUT_FILE}..."

CATEGORIES=("electronics" "books" "clothing" "home" "toys" "sports" "automotive")
STATUSES=("shipped" "pending" "delivered" "cancelled" "returned")
LOG_LEVELS=("INFO" "WARN" "ERROR" "DEBUG" "CRITICAL")

for i in $(seq 1 ${NUM_QUERIES}); do

  TIMESTAMP=$(date -u +"%b %d, %Y %H:%M:%S")
  NANOSECONDS=$(printf "%09d" $(( (i * 100000 + (RANDOM % 99999)) % 1000000000 )) )
  FULL_TIMESTAMP="${TIMESTAMP}.${NANOSECONDS} UTC"

  QUERY_TYPE=$(( i % 5 ))

  case ${QUERY_TYPE} in
    0)
      QUERY="SELECT user_name, email, last_login FROM users WHERE id = ${i};"
      ;;
    1)
      CATEGORY=${CATEGORIES[$(( i % ${#CATEGORIES[@]} ))]}
      QUERY="SELECT product_name, price FROM products WHERE category = '${CATEGORY}' AND product_sku = 'SKU-${i}-A';"
      ;;
    2)
      STATUS=${STATUSES[$(( i % ${#STATUSES[@]} ))]}
      QUERY="UPDATE orders SET status = '${STATUS}', updated_at = NOW() WHERE id = ${i};"
      ;;
    3)
      LEVEL=${LOG_LEVELS[$(( i % ${#LOG_LEVELS[@]} ))]}
      QUERY="SELECT COUNT(*) FROM logs WHERE level = '${LEVEL}' AND event_id = 'evt-${i}-${NANOSECONDS}';"
      ;;
    4)
      TOKEN=$(uuidgen)
      QUERY="INSERT INTO sessions (user_id, session_token, ip_address) VALUES (${i}, '${TOKEN}', '192.168.1.1');"
      ;;
  esac

  printf "%s\t%s\n" "${FULL_TIMESTAMP}" "${QUERY}" >> "${OUTPUT_FILE}"

done

echo "Done! File ${OUTPUT_FILE} with ${NUM_QUERIES} unique query lines has been successfully created."
