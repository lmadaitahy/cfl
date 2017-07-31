#!/bin/bash

# This is for SimpleCFDataSize.
# gg.jobs.SimpleCFDataSize

warmupIts=100
warmupS=1000

realIts=100


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
./flink-cfl/bin/stop-cluster.sh >$(mktemp /tmp/ggevay/initial-stop-cluster.XXXXXXX)
echo "Done."


numMachines=25
ok=false
while [ $ok = false ]; do
  echo -n "$numMachines machines. "

  ./create-conf-para.sh $numMachines
  sed -i.bak "s/taskmanager.network.numberOfBuffers: 65536/taskmanager.network.numberOfBuffers: 262144/" flink-cfl/conf/flink-conf.yaml
  echo "akka.ask.timeout: 30s" >>flink-cfl/conf/flink-conf.yaml


  ./flink-cfl/bin/start-cluster.sh >$(mktemp /tmp/ggevay/start-cluster.XXXXXX)
  echo -n "Cluster started. "
  
  # give the TMs time to really start up
  sleep 30

  # warmup jobs
  echo -n "Warmup jobs"
  for i in {1..10}; do
    tmpOut=$(mktemp /tmp/ggevay/out.XXXXXXXX)

    set +e
    ./flink-cfl/bin/flink run -c gg.jobs.SimpleCFDataSize /home/ggevay/cfl-1.0-SNAPSHOT.jar $warmupIts $warmupS &>$tmpOut
    ./check-cfl-success.sh $tmpOut
    if [ $? != 0 ]; then
        # The Flink job failed. Stop the Flink cluster and try with this numMachines again.
        echo "Flink job failed. Restarting Flink cluster and trying again."
        echo -n "restart" >>$resFile
        ./flink-cfl/bin/stop-cluster.sh >$(mktemp /tmp/ggevay/stop-cluster.XXXXXXXX)
        continue 2 # Continue 2nd enclosing loop
    fi
    set -e

    echo -n .
    sleep 25
  done
  echo -n " "

  # Real jobs
  echo "Real jobs"
  for S in {7500..30000..500}; do #{150..1000..50}; do
      echo -n $S
      echo -n "$S ">>$resFile
      for i in {1..5}; do
        tmpOut=$(mktemp /tmp/ggevay/out.XXXXXXXXX)
        export rtmarker=ezarealt

        set +e
        /usr/bin/time -f ${rtmarker}%e ./flink-cfl/bin/flink run -c gg.jobs.SimpleCFDataSize /home/ggevay/cfl-1.0-SNAPSHOT.jar $realIts $S &>$tmpOut
        ./check-cfl-success.sh $tmpOut
        if [ $? != 0 ]; then
            echo "!!!!!!!!!!!!! Flink job failed"
            ./flink-cfl/bin/stop-cluster.sh >$(mktemp /tmp/ggevay/stop-cluster.XXXXXXX)
            continue
            exit 5
        fi
        set -e

        # Get the execution time from the output
        ./parse-time.sh $tmpOut >>$resFile
        echo -n " " >>$resFile

        echo -n .
        sleep 25
      done
      echo >>$resFile  #newline
      echo
  done


  ./flink-cfl/bin/stop-cluster.sh >$(mktemp /tmp/ggevay/stop-cluster.XXXXXXX)
  sleep 1

  ok=true
done  

