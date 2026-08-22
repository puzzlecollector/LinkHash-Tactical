#!/usr/bin/env bash
# One-shot setup on a fresh Ubuntu bot server.
#   git clone https://<PAT>@github.com/<you>/linkhash-strategies.git
#   cd linkhash-strategies && cp .env.example .env && nano .env   # fill creds
#   bash setup.sh
set -euo pipefail
cd "$(dirname "$0")"

python3 -m venv venv
./venv/bin/pip install -q --upgrade pip
./venv/bin/pip install -q -r requirements.txt
echo "venv ready."

if [ ! -f .env ]; then
  echo "!! no .env — copy .env.example to .env and fill it before starting." >&2
  exit 1
fi

# quick self-test (dry unless .env enables live)
./venv/bin/python strategy_bot.py --status

# install + start the systemd service (stays dry-run until .env flips it)
sudo cp linkhash-strategy-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now linkhash-strategy-bot.service
echo "service started. logs: tail -f bot.log   |  status: ./venv/bin/python strategy_bot.py --status"
