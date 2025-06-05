set -e

ITERATIONS=${1:-10}

for ((i = 1; i <= ITERATIONS; i++)); do
  echo "🔁 Test run $i/$ITERATIONS..."
  go test -v
done

echo "✅ All test runs passed."
