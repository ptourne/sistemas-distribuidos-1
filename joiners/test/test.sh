set -e

for i in {1..10}; do
  echo "🔁 Test run $i/10..."
  go test -v
done

echo "✅ All test runs passed."
