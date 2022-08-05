#!/usr/bin/env bash

# The cleanup hook ensures these containers are removed when the script exits.
POSTGRES_CONTAINER=test-container
export INDEXER_DATA=/tmp/e2e_test/

NET=localhost:8981
CURL_TEMPFILE=curl_out.txt
PIDFILE=testindexerpidfile
CONNECTION_STRING="host=localhost user=algorand password=algorand dbname=DB_NAME_HERE port=5434 sslmode=disable"
MAX_TIME=20
# Set to to prevent cleanup so you can look at the DB or run queries.
HALT_ON_FAILURE=


###################
## Print Helpers ##
###################
function print_alert() {
  printf "\n=====\n===== $1\n=====\n"
}

function print_health() {
    curl -q -s "$NET/health?pretty"
}

##################
## Test Helpers ##
##################
function sleep_forever {
  # sleep infinity doesn't work on mac...
  sleep 1000000000000000
}

function fail_and_exit {
  print_alert "Failed test - $1 ($2): $3"
  echo ""
  print_health
  if [ ! -z $HALT_ON_FAILURE ]; then
    sleep_forever
  fi
  exit 1
}

# $1 - database
# $2 - query
function base_query() {
  #export PGPASSWORD=algorand
  #psql -XA -h localhost -p 5434 -h localhost -U algorand $1 -c "$2"
  docker exec $POSTGRES_CONTAINER psql -XA -Ualgorand $1 -c "$2"
}

# SQL Test - query and veryify results
# $1 - max runtime in seconds, default value = 20
# $2 - test description.
# $3 - database
# $4 - query
# $5 - substring that should be in the response
function sql_test_timeout {
  local MAX_TIME_BEFORE=MAX_TIME
  MAX_TIME=$1
  shift
  sql_test "$@"
  MAX_TIME=$MAX_TIME_BEFORE
}

# SQL Test - query and veryify results
# $1 - test description.
# $2 - database
# $3 - query
# $4... - substring(s) that should be in the response
function sql_test {
  local DESCRIPTION=$1
  shift
  local DATABASE=$1
  shift
  local QUERY=$1
  shift
  local SUBSTRING

  local START=$SECONDS

  set +e
  RES=$(base_query $DATABASE "$QUERY")
  if [[ $? != 0 ]]; then
    echo "ERROR from psql: $RESULT"
    fail_and_exit "$DESCRIPTION" "$QUERY" "psql had a non-zero exit code."
  fi
  set -e

  # Check results
  for SUBSTRING in "$@"; do
    if [[ "$RES" != *"$SUBSTRING"* ]]; then
      fail_and_exit "$DESCRIPTION" "$QUERY" "unexpected response. should contain '$SUBSTRING', actual: '$RES'"
    fi
  done

  local ELAPSED=$(($SECONDS - $START))
  if [[ $ELAPSED -gt $MAX_TIME ]]; then
    fail_and_exit "$DESCRIPTION" "$QUERY" "query duration too long, $ELAPSED > $MAX_TIME"
  fi

  print_alert "Passed test: $DESCRIPTION"
}

# rest_test helper
function base_curl() {
  curl -o "$CURL_TEMPFILE" -w "%{http_code}" -q -s "$NET$1"
}

# CURL Test - query and veryify results
# $1 - max runtime in seconds, default value = 20
# $2 - test description.
# $3 - query
# $4 - match result
# $5 - expected status code
# $6... - substring that should be in the response
function rest_test_timeout {
  local MAX_TIME_BEFORE=MAX_TIME
  MAX_TIME=$1
  shift
  rest_test "$@"
  MAX_TIME=$MAX_TIME_BEFORE
}

# CURL Test - query and veryify results
# $1 - test description.
# $2 - query
# $3 - expected status code
# $4 - match result
# $5... - substring(s) that should be in the response
function rest_test {
  local DESCRIPTION=$1
  shift
  local QUERY=$1
  shift
  local EXPECTED_CODE=$1
  shift
  local MATCH_RESULT=$1
  shift
  local SUBSTRING

  local START=$SECONDS

  set +e
  local CODE=$(base_curl "$QUERY")
  if [[ $? != 0 ]]; then
    cat $CURL_TEMPFILE
    fail_and_exit "$DESCRIPTION" "$QUERY" "curl had a non-zero exit code."
  fi
  set -e

  local RES=$(cat "$CURL_TEMPFILE")
  if [[ "$CODE" != "$EXPECTED_CODE" ]]; then
    fail_and_exit "$DESCRIPTION" "$QUERY" "unexpected HTTP status code expected $EXPECTED_CODE (actual $CODE): $RES"
  fi

  local ELAPSED=$(($SECONDS - $START))
  if [[ $ELAPSED -gt $MAX_TIME ]]; then
    fail_and_exit "$DESCRIPTION" "$QUERY" "query duration too long, $ELAPSED > $MAX_TIME"
  fi

  # Check result substrings
  for SUBSTRING in "$@"; do
    if [[ $MATCH_RESULT = true ]]; then
      if [[ "$RES" != *"$SUBSTRING"* ]]; then
        fail_and_exit "$DESCRIPTION" "$QUERY" "unexpected response. should contain '$SUBSTRING', actual: $RES"
      fi
    else
      if [[ "$RES" == *"$SUBSTRING"* ]]; then
        fail_and_exit "$DESCRIPTION" "$QUERY" "unexpected response. should NOT contain '$SUBSTRING', actual: $RES"
      fi
    fi
  done

  print_alert "Passed test: $DESCRIPTION"
}

#####################
## Indexer Helpers ##
#####################

# Suppresses output if the command succeeds
# $1 command to run
function suppress() {
  /bin/rm --force /tmp/suppress.out 2> /dev/null
  ${1+"$@"} > /tmp/suppress.out 2>&1 || cat /tmp/suppress.out
  /bin/rm /tmp/suppress.out
}

# $1 - connection string
# $2 - if set, puts in read-only mode
function start_indexer_with_connection_string() {
  if [ ! -z $2 ]; then
    # strictly read-only
    RO="--no-algod"
  else
    # we may start up from canned data, but need to update for the current running binary.
    RO="--allow-migration"
  fi
  mkdir -p $INDEXER_DATA
  ALGORAND_DATA= ../cmd/algorand-indexer/algorand-indexer daemon \
    -S $NET "$RO" \
    -P "$1" \
    -i /tmp \
    --enable-all-parameters \
    "$RO" \
    --pidfile $PIDFILE 2>&1 > /dev/null &
}

# $1 - postgres dbname
# $2 - if set, halts execution
function start_indexer() {
  if [ ! -z $2 ]; then
    echo "daemon -i /tmp -S $NET -P \"${CONNECTION_STRING/DB_NAME_HERE/$1}\""
    sleep_forever
  fi

  start_indexer_with_connection_string "${CONNECTION_STRING/DB_NAME_HERE/$1}"
}


# $1 - postgres dbname
# $2 - e2edata tar.bz2 archive
# $3 - if set, halts execution
function start_indexer_with_blocks() {
  if [ ! -f $2 ]; then
    echo "Cannot find $2"
    exit
  fi

  create_db $1

  local TEMPDIR=$(mktemp -d -t ci-XXXXXXX)
  tar -xf "$2" -C $TEMPDIR

  if [ ! -z $3 ]; then
    echo "Start args 'import -P \"${CONNECTION_STRING/DB_NAME_HERE/$1}\" --genesis \"$TEMPDIR/algod/genesis.json\" $TEMPDIR/blocktars/*'"
    sleep_forever
  fi
  ALGORAND_DATA= ../cmd/algorand-indexer/algorand-indexer import \
    -P "${CONNECTION_STRING/DB_NAME_HERE/$1}" \
    --genesis "$TEMPDIR/algod/genesis.json" \
    $TEMPDIR/blocktars/*

  rm -rf $TEMPDIR

  start_indexer $1 $3
}

# $1 - number of attempts
function wait_for_started() {
  wait_for '"round":' "$1"
}

# $1 - number of attempts
function wait_for_migrated() {
  wait_for '"migration-required":false' "$1"
}

# $1 - number of attempts
function wait_for_available() {
  wait_for '"db-available":true' "$1"
}

# Query indexer for 20 seconds waiting for migration to complete.
# Exit with error if still not ready.
# $1 - string to look for
# $2 - number of attempts (optional, default = 20)
function wait_for() {
  local n=0

  set +e
  local READY
  until [ "$n" -ge ${2:-20} ] || [ ! -z $READY ]; do
    curl -q -s "$NET/health" | grep "$1" > /dev/null 2>&1 && READY=1
    n=$((n+1))
    sleep 1
  done
  set -e

  if [ -z $READY ]; then
    echo "Error: timed out waiting for $1."
    print_health
    exit 1
  fi
}

# Kill indexer using the PIDFILE
function kill_indexer() {
  if test -f "$PIDFILE"; then
    kill -9 $(cat "$PIDFILE") > /dev/null 2>&1 || true
    rm $PIDFILE
    rm -rf $INDEXER_DATA
    pwd
    ls -l
    rm ledger*sqlite* || true
  fi
}

####################
## Docker helpers ##
####################

# $1 - name of docker container to kill.
function kill_container() {
  print_alert "Killing container - $1"
  docker rm -f $1 > /dev/null 2>&1 || true
}

function start_postgres() {
  if [ $# -ne 0 ]; then
    print_alert "Unexpected number of arguments to start_postgres."
    exit 1
  fi

  # Cleanup from last time
  kill_container $POSTGRES_CONTAINER

  print_alert "Starting - $POSTGRES_CONTAINER"
  # Start postgres container...
  docker run \
    -d \
    --name $POSTGRES_CONTAINER \
    -e POSTGRES_USER=algorand \
    -e POSTGRES_PASSWORD=algorand \
    -e PGPASSWORD=algorand \
    -p 5434:5432 \
    postgres

  sleep 5

  print_alert "Started - $POSTGRES_CONTAINER"
}

# $1 - postgres database name.
function create_db() {
  local DATABASE=$1

  # Create DB
  docker exec -it $POSTGRES_CONTAINER psql -Ualgorand -c "create database $DATABASE"
}

# $1 - postgres database name.
# $2 - pg_dump file to import into the database.
function initialize_db() {
  local DATABASE=$1
  local DUMPFILE=$2
  print_alert "Initializing database ($DATABASE) with $DUMPFILE"

  # load some data into it.
  create_db $DATABASE
  #docker exec -i $POSTGRES_CONTAINER psql -Ualgorand -c "\\l"
  docker exec -i $POSTGRES_CONTAINER psql -Ualgorand -d $DATABASE < $DUMPFILE > /dev/null 2>&1
}

function cleanup() {
  kill_container $POSTGRES_CONTAINER
  rm $CURL_TEMPFILE > /dev/null 2>&1 || true
  kill_indexer
}

#####################
## User Interaction #
#####################

# Interactive yes/no prompt
function ask () {
    # https://djm.me/ask
    local prompt default reply

    if [ "${2:-}" = "Y" ]; then
        prompt="Y/n"
        default=Y
    elif [ "${2:-}" = "N" ]; then
        prompt="y/N"
        default=N
    else
        prompt="y/n"
        default=
    fi

    while true; do

        # Ask the question (not using "read -p" as it uses stderr not stdout)
        echo -n "$1 [$prompt] "

        # Read the answer (use /dev/tty in case stdin is redirected from somewhere else)
        read reply </dev/tty

        # Default?
        if [ -z "$reply" ]; then
            reply=$default
        fi

        # Check if the reply is valid
        case "$reply" in
            Y*|y*) return 0 ;;
            N*|n*) return 1 ;;
        esac

    done
}

############################################################################
## Integration tests are sometimes useful to run after a migration as well #
############################################################################
function cumulative_rewards_tests() {
    rest_test 'Ensure migration updated specific account rewards.' '/v2/accounts/FZPGVIFCMHCE2HC2LEDD7IZQLKZVHRV5PENSD26Y2AOS3OWCYMKTY33UXI' 200 true '"rewards":80000539878'
    # Rewards / Rewind is now disabled
    #rest_test 'Ensure migration updated specific account rewards @ round = 810.' '/v2/accounts/FZPGVIFCMHCE2HC2LEDD7IZQLKZVHRV5PENSD26Y2AOS3OWCYMKTY33UXI?round=810' 200 '"rewards":80000539878'
    #rest_test 'Ensure migration updated specific account rewards @ round = 800.' '/v2/accounts/FZPGVIFCMHCE2HC2LEDD7IZQLKZVHRV5PENSD26Y2AOS3OWCYMKTY33UXI?round=800' 200 '"rewards":68000335902'
    #rest_test 'Ensure migration updated specific account rewards @ round = 500.' '/v2/accounts/FZPGVIFCMHCE2HC2LEDD7IZQLKZVHRV5PENSD26Y2AOS3OWCYMKTY33UXI?round=500' 200 '"rewards":28000055972'
    #rest_test 'Ensure migration updated specific account rewards @ round = 100.' '/v2/accounts/FZPGVIFCMHCE2HC2LEDD7IZQLKZVHRV5PENSD26Y2AOS3OWCYMKTY33UXI?round=100' 200 '"rewards":7999999996'

    # One disabled test...
    rest_test 'Ensure migration updated specific account rewards @ round = 810.' '/v2/accounts/FZPGVIFCMHCE2HC2LEDD7IZQLKZVHRV5PENSD26Y2AOS3OWCYMKTY33UXI?round=810' 200 true '"rewards":0'
}

