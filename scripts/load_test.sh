#!/bin/bash

# Target the API Gateway port, not the internal Producer port
TARGET_URL="http://localhost:8000/api/logs"
TOTAL_REQUESTS=100
BATCH_SIZE=20

echo "Initiating Load Test to Gateway ($TARGET_URL)..."
echo "Sending $TOTAL_REQUESTS requests in batches of $BATCH_SIZE."
echo "---------------------------------------------------"

for ((i=1; i<=TOTAL_REQUESTS; i++)); do
  # Run curl commands in the background to simulate concurrent load
  curl -s -X POST $TARGET_URL \
    -H "Content-Type: application/json" \
    -d "{
      \"organizationId\": \"org-$((i % 5))\",
      \"level\": \"INFO\",
      \"message\": \"High throughput test message $i\",
      \"source\": \"load-tester-script\"
    }" > /dev/null &
    
  # Wait for every batch to finish before sending the next
  if (( i % BATCH_SIZE == 0 )); then
    wait
    echo "Successfully dispatched $i requests..."
  fi
done

# Wait for any remaining background jobs to finish
wait
echo "---------------------------------------------------"
echo "Load test complete! Check your consumer logs to verify ingestion."