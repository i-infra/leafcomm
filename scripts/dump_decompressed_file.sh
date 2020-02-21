set -exu
redis-cli -s ~/.sproutwave/sproutwave.sock get $1 | pipenv run python scripts/SerializedReading_decoder.py - > $1
