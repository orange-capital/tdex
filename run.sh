#!/bin/bash

app=tdex
uuid=$(cat /dev/urandom | tr -dc 'a-z' | fold -w 5 | head -n 1)
export ERL_TOP=/opt/OTP
export VALGRIND_LOG_DIR=`pwd`
export VALGRIND_MISC_FLAGS="--leak-check=full --show-reachable=yes"

case $1 in
remote)
    iex --name ${uuid}_${app}@robot.sh --remsh ${app}@robot.sh --erl "-setcookie nopass"
    ;;
test)
    CONFIG_FILE=priv/${app}.config mix test --no-start
    ;;
valgrind)
    echo "Starting iex with valgrind"
    python3 scripts/patch.py
    CONFIG_FILE=priv/${app}.config iex --valgrind --name ${app}@robot.sh --erl "-setcookie nopass" -S mix
    ;;
*)
    CONFIG_FILE=priv/${app}.config iex --name ${app}@robot.sh --erl "-setcookie nopass" -S mix
    ;;
esac