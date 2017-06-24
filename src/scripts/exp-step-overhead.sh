#!/bin/bash

# Strict mode, see http://redsymbol.net/articles/unofficial-bash-strict-mode/
# But note that the Flink jobs will return 1, because of the cancel,
# so we surround them with set +e, set -e
set -euo pipefail

# Create result file, and check that it had not already existed.
resFile=result.csv
if [ -f $resFile ]; then
  echo "!!! $resFile file exists. Exiting."
  exit 73
fi
printf "" >$resFile

# Stop Flink cluster on script exit in any case.
function finish {
  set +e
  printf "\nScript exiting, stopping cluster:\n"
  ./flink-cfl/bin/stop-cluster.sh
}
trap finish EXIT

# Copy a clean flink here.
rm -r ./flink-cfl
./copy-flink.sh


mkdir -p /tmp/ggevay


# Let's do a stop-cluster to be sure that we don't have any Flink running.
# Note: For this reason, the config copied by copy-flink.sh should have all the machines that will be used.
echo -n "Stopping any remaining Flink cluster. "
./flink-cfl/bin/stop-cluster.sh >$(mktemp /tmp/ggevay/initial-stop-cluster.XXXXXXXXXXXXXXXXXXXXXX)
echo "Done."


for numMachines in {1..25}
do
  echo -n "$numMachines machines. "

  echo -n "$numMachines ">>$resFile

  ./create-conf-para.sh $numMachines

  ./flink-cfl/bin/start-cluster.sh >$(mktemp /tmp/ggevay/start-cluster.XXXXXXXXXXXXXXXXXXXXXX)
  echo -n "Cluster started. "
  
  # give the TMs time to really start up
  sleep 15

  # warmup jobs
  echo -n "Warmup jobs"
  for i in {1..10}
  do
    tmpOut=$(mktemp /tmp/ggevay/out.XXXXXXXXXXXXXXXXXXXXXX)
    set +e
    ./flink-cfl/bin/flink run -c gg.jobs.SimpleCF /home/ggevay/cfl-1.0-SNAPSHOT.jar 100 &>$tmpOut
    set -e
    ./check-cfl-success.sh $tmpOut
    echo -n .
    sleep 3
  done
  echo -n " "

  # Real jobs
  echo -n "Real jobs" 
  for i in {1..7}
  do
    tmpOut=$(mktemp /tmp/ggevay/out.XXXXXXXXXXXXXXXXXXXXXX)
    export rtmarker=ezarealt
    set +e
    /usr/bin/time -f ${rtmarker}%e ./flink-cfl/bin/flink run -c gg.jobs.SimpleCF /home/ggevay/cfl-1.0-SNAPSHOT.jar 10000 &>$tmpOut
    set -e
    ./check-cfl-success.sh $tmpOut

    # Get the execution time from the output
    ./parse-time.sh $tmpOut >>$resFile
    echo -n " " >>$resFile

    echo -n .

    sleep 3
  done

  echo >>$resFile  #newline

  echo

  ./flink-cfl/bin/stop-cluster.sh >$(mktemp /tmp/ggevay/stop-cluster.XXXXXXXXXXXXXXXXXXXXXX)
  sleep 1
done
