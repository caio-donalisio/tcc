#Script used to count files in a given bucket

#example:
# . count_bucket.sh --court tjrn --start-date 2020 --end-date 2023 --filename-pattern "*.html"

unset COURT
unset START_DATE
unset END_DATE
unset FILENAME_PATTERN

VALID=true
DEFAULT_END_DATE=$(date +'%Y-%m-%d')
echo $DEFAULT_END_DATE

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
    --filename-pattern)
    FILENAME_PATTERN="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    printf "Unknown option: $key \nValid options are: \n--court \n--start-date \n--end-date \n--filename-pattern\n\n"
    return 
    ;;
esac
done

#Sets default end-year if it were not given
if [ -z "$END_DATE" ]; then
  END_DATE=$DEFAULT_END_DATE
  echo "Setting default end year to $DEFAULT_END_DATE" 
fi

if [ -z "$FILENAME_PATTERN" ]; then
  FILENAME_PATTERN="*"
fi

#Checks if start-year and court were given
if [ -z "$START_DATE" ] || [ -z "$COURT" ]; then
  echo "Error: COURT or START_DATE is null"
  return
fi

COUNTS=()
printf "Counting files...\n\n"
printf "court: $COURT\nstart_date: $START_DATE\nend_date: $END_DATE\nfilename_pattern: $FILENAME_PATTERN\n"

current_date="$START_DATE"
while [[ "$current_date" < "$END_DATE" ]]; do
  # Extract the year and month from the current date
  year=$(date -d "$current_date" +%Y)
  month=$(date -d "$current_date" +%m)
  echo
	echo "$COURT $year $month '$FILENAME_PATTERN'"
  NUM_FILES=$(gsutil -m ls "gs://inspira-production-buckets-$COURT/$year/$month/$FILENAME_PATTERN" | wc -l)
  COUNTS+=($NUM_FILES)
	echo $NUM_FILES 
  # Move to the next month
  current_date=$(date -d "$current_date +1 month" +%Y-%m-%d)
done

echo
echo "Printing all counts at once:"
echo  "$COURT from $START_DATE to $END_DATE"
echo
printf '%s\n' "${COUNTS[@]}"

unset COURT
unset START_DATE
unset END_DATE
unset FILENAME_PATTERN

