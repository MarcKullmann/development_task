#!/bin/sh
# python_commands.sh

python -m app.db &
python -m app.main &
python -m tests.test_main &

wait
