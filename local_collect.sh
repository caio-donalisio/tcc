#Script used for crawler to iterate through months locally

#example:
# . local_collect.sh --court tjrn --start-date 2020-01-01 --end-date 2023-05-31 --extra-args "--skip-cache"

unset COURT
unset START_DATE
unset END_DATE
unset EXTRA_ARGS
unset REPEAT

DEFAULT_END_DATE=$(date +'%Y')

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --court)
    COURT="$2"
    shift # past argument
    shift # past value
    ;;
    --start-date)
    START_DATE="$2"
    shift # past argument
    shift # past value
    ;;
    --end-date)
    END_DATE="$2"
    shift # past argument
    shift # past value
    ;;
    --extra-args)
    EXTRA_ARGS="$2"
    shift # past argument
    shift # past value
    ;;
    --repeat)
    REPEAT="$2"
    REPEAT=$(($REPEAT+0))
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    printf "Unknown option: $key \nValid options are: \n--court \n--start-date \n--end-date \n--extra-args \n--repeat\n\n"
    return 
    ;;
esac
done



#Sets default end-year if it were not given
if [ -z "$END_DATE" ]; then
  END_DATE=$DEFAULT_END_DATE
  echo "Setting default end year to $DEFAULT_END_DATE" 
fi

if [ -z "$REPEAT" ]; then
  REPEAT=1
fi

#Checks if start-year and court were given
if [ -z "$START_DATE" ] || [ -z "$COURT" ]; then
  echo "Error: COURT or START_DATE is null"
  return
fi

#Checks if python exists
if ! command -v python &> /dev/null
then
    echo "Python could not be found"
    return
fi

while [[ $REPEAT>0 ]]; do
  current_date="$START_DATE"
  while [[ "$current_date" < "$END_DATE" ]]; do
    # Extract the year and month from the current date
    start_date=$(date -d "$current_date" +%Y-%m-%d)
    end_date=$(date -d "$current_date +1 month -1 day" +%Y-%m-%d)
    echo "Scrapes remaining: $REPEAT"
    echo "python -m app.commands $COURT --start-date $start_date --end-date $end_date --output-uri gs://inspira-production-buckets-$COURT $EXTRA_ARGS"
    python -m app.commands $COURT --start-date $start_date --end-date $end_date --output-uri gs://inspira-production-buckets-$COURT $EXTRA_ARGS
    # Move to the next month
    current_date=$(date -d "$current_date +1 month" +%Y-%m-%d)
  done
  REPEAT=$(($REPEAT-1))
  echo $REPEAT
done

unset COURT
unset START_DATE
unset END_DATE
unset EXTRA_ARGS
unset REPEAT