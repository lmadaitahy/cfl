#!/bin/bash

# gg.jobs.ClickCountDiffs

# $1 specifies whether we reuse inputs (true or false)


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

# Copy configuration.
cp conf_ClickCountLaby/* flink-cfl/conf


mkdir -p /tmp/ggevay


# Let's do a stop-cluster to be sure that we don't have any Flink running.
# Note: For this reason, the config copied by copy-flink.sh should have all the machines that will be used.
echo -n "Stopping any remaining Flink cluster. "
./flink-cfl/bin/stop-cluster.sh >$(mktemp /tmp/ggevay/initial-stop-cluster.XXXXXXX)
echo "Done."


numMachines=25
echo "$numMachines machines."

for size in 25000000 50000000 100000000 200000000 400000000 800000000; do
#for size in 800000000; do

  echo -n "Size: $size. "
  echo -n "$size ">>$resFile

  for j in {1..3}; do
      ./flink-cfl/bin/start-cluster.sh >$(mktemp /tmp/ggevay/start-cluster.XXXXXX)
      echo -n "Cluster started for repeat $j. "
      
      # give the TMs time to really start up
      sleep 20

      # warmup jobs
      echo -n "Warmup jobs"
      for i in {1..2}; do
        tmpOut=$(mktemp /tmp/ggevay/out.XXXXXXXX)

        set +e
        ./flink-cfl/bin/flink run -c gg.jobs.ClickCountDiffs /home/ggevay/cfl-1.0-SNAPSHOT.jar hdfs://cloud-11:44000/user/ggevay/ClickCountGenerated/0.05/25000000 365 true &>$tmpOut
        ./check-cfl-success.sh $tmpOut
        if [ $? != 0 ]; then
            echo "!!!!!!!!!!!!! Flink job failed"
            exit 6
        fi
        set -e

        echo -n .
        sleep 25
      done
      echo -n " "

      # Real job
      echo -n "Real job"

      tmpOut=$(mktemp /tmp/ggevay/out.XXXXXXXXX)
      export rtmarker=ezarealt

      set +e
      /usr/bin/time -f ${rtmarker}%e ./flink-cfl/bin/flink run -c gg.jobs.ClickCountDiffs /home/ggevay/cfl-1.0-SNAPSHOT.jar hdfs://cloud-11:44000/user/ggevay/ClickCountGenerated/0.05/$size 365 $1 &>$tmpOut
      ./check-cfl-success.sh $tmpOut
      if [ $? != 0 ]; then
        echo "!!!!!!!!!!!!! Flink job failed"
        exit 5
      fi
      set -e

      # Get the execution time from the output
      ./parse-time.sh $tmpOut >>$resFile
      echo -n " " >>$resFile

      echo -n ". "


      ./flink-cfl/bin/stop-cluster.sh >$(mktemp /tmp/ggevay/stop-cluster.XXXXXXX)
      sleep 15

  done

  echo >>$resFile  #newline
  echo

done
