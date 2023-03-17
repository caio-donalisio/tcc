#Script used to count files in a given bucket

#example:
# . count_bucket.sh --court tjrn --start-year 2020 --end-year 2023 --filename-pattern "*.html"

unset COURT
unset START_YEAR
unset END_YEAR
unset FILENAME_PATTERN

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
    --filename-pattern)
    FILENAME_PATTERN="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    printf "Unknown option: $key \nValid options are: \n--court \n--start-year \n--end-year \n--filename-pattern\n\n"
    return 
    ;;
esac
done

#Sets default end-year if it were not given
if [ -z "$END_YEAR" ]; then
  END_YEAR=$DEFAULT_END_YEAR
  echo "Setting default end year to $DEFAULT_END_YEAR" 
fi

if [ -z "$FILENAME_PATTERN" ]; then
  FILENAME_PATTERN="*"
fi

#Checks if start-year and court were given
if [ -z "$START_YEAR" ] || [ -z "$COURT" ]; then
  echo "Error: COURT or START_YEAR is null"
  return
fi

COUNTS=()
printf "Counting files...\n\n"
printf "court: $COURT\nstart_year: $START_YEAR\nend_year: $END_YEAR\nfilename_pattern: $FILENAME_PATTERN\n"
for (( y=$((START_YEAR)); y<=$((END_YEAR)); y++)); do
  for m in {01..12}; do
	echo
	echo "$COURT $y $m '$FILENAME_PATTERN'"
    start_date=$(date -d "$y-$m-01" +"%Y-%m-%d")
    end_date=$(date -d "$y-$m-$(cal $m $y | awk 'NF {DAYS = $NF}; END {print DAYS}')" +"%Y-%m-%d")
    NUM_FILES=$(gsutil -m ls "gs://inspira-production-buckets-$COURT/$y/$m/$FILENAME_PATTERN" | wc -l)
	COUNTS+=($NUM_FILES)
	echo $NUM_FILES 
  done
done
echo
echo "Printing all counts at once:"
echo  "$COURT from $START_YEAR to $END_YEAR"
echo
printf '%s\n' "${COUNTS[@]}"

unset COURT
unset START_YEAR
unset END_YEAR
unset FILENAME_PATTERN

