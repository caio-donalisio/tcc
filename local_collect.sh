#Script used for crawler to iterate through months locally

#example:
# . local_collect.sh --court tjrn --start-year 2020 --end-year 2023 --extra-args "--skip-cache"

unset COURT
unset START_YEAR
unset END_YEAR
unset EXTRA_ARGS

VALID=true
DEFAULT_END_YEAR=$(date +'%Y')

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --court)
    COURT="$2"
    shift # past argument
    shift # past value
    ;;
    --start-year)
    START_YEAR="$2"
    shift # past argument
    shift # past value
    ;;
    --end-year)
    END_YEAR="$2"
    shift # past argument
    shift # past value
    ;;
    --extra-args)
    EXTRA_ARGS="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    printf "Unknown option: $key \nValid options are: \n--court \n--start-year \n--end-year \n--extra-args\n\n"
    return 
    ;;
esac
done

#Sets default end-year if it were not given
if [ -z "$END_YEAR" ]; then
  END_YEAR=$DEFAULT_END_YEAR
  echo "Setting default end year to $DEFAULT_END_YEAR" 
fi

#Checks if start-year and court were given
if [ -z "$START_YEAR" ] || [ -z "$COURT" ]; then
  echo "Error: COURT or START_YEAR is null"
  return
fi

#Checks if python exists
if ! command -v python &> /dev/null
then
    echo "Python could not be found"
    return
fi

for (( y=$((START_YEAR)); y<=$((END_YEAR)); y++)); do
  for m in {1..12}; do
    start_date=$(date -d "$y-$m-01" +"%Y-%m-%d")
    end_date=$(date -d "$y-$m-$(cal $m $y | awk 'NF {DAYS = $NF}; END {print DAYS}')" +"%Y-%m-%d")
    echo "python -m app.commands $COURT --start-date $start_date --end-date $end_date --output-uri gs://inspira-production-buckets-$COURT $EXTRA_ARGS"
    python -m app.commands $COURT --start-date $start_date --end-date $end_date --output-uri gs://inspira-production-buckets-$COURT $EXTRA_ARGS
  done
done

unset COURT
unset START_YEAR
unset END_YEAR
unset EXTRA_ARGS