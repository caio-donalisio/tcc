COURT="$1"

for y in {2022..2023}; do
  for m in {1..12}; do
    start_date=$(date -d "$y-$m-01" +"%Y-%m-%d")
    end_date=$(date -d "$y-$m-$(cal $m $y | awk 'NF {DAYS = $NF}; END {print DAYS}')" +"%Y-%m-%d")
    echo $COURT $start_date $end_date
    python -m app.commands $COURT --start-date $start_date --end-date $end_date --output-uri gs://inspira-production-buckets-$COURT
  done
done
