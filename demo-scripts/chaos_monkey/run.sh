#!/bin/bash

# Chaos Monkey Runner Script
# This script runs chaos monkey

# Root repository directory
REPO_ROOT=$(git rev-parse --show-toplevel)
if [ $? -ne 0 ]; then
  echo "Error: Not a git repository. Please run this script from the root of the repository."
  exit 1
fi
SCRIPT_PATH=$REPO_ROOT/demo-scripts/chaos_monkey
pushd $SCRIPT_PATH > /dev/null || exit 1

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
  echo "❌ Docker daemon is not running or not accessible."
  echo "Please ensure:"
  echo "  1. Docker is installed and running: sudo systemctl start docker"
  echo "  2. You have permission to access Docker:"
  echo "     - Run with sudo: sudo $0"
  echo "     - Or add your user to docker group: sudo usermod -aG docker $USER"
  echo ""
  popd > /dev/null || exit 1
  exit 1
fi

echo "🐒 Starting Chaos Monkey..."
echo "⚠️  Press Ctrl+C to stop"
echo ""

# Run the chaos monkey
go run ./main.go 

popd > /dev/null || exit 1
echo "🐒 Chaos Monkey has stopped."