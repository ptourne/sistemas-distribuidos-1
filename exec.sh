./stop.sh
clear
./clean.sh
set -e
python3 generate_compose.py
./start.sh
lazydocker