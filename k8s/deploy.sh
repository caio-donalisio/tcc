#!/usr/bin/env bash

loadEnv() {
    local envFile="${1?Missing environment file}"
    # export $( grep -v '#' "${envFile}" | sed 's/\r$//' | awk '/=/ {print $1}' )
    local environmentAsArray variableDeclaration
    mapfile environmentAsArray < <(
        grep --invert-match '^#' "${envFile}" \
            | grep --invert-match '^\s*$'
    ) # Uses grep to remove commented and blank lines
    for variableDeclaration in "${environmentAsArray[@]}"; do
        export "${variableDeclaration//[$'\r\n']}" # The substitution removes the line breaks
    done
}

loadEnv ../.env.dist # loads default env
loadEnv ../.env

deploy() {
    export COURT_ID=$1
    export JOB_NAME=$2
    export JOB_PREFIX=$3
    export IMAGE_TAG=${4:-latest}

    if [ -z "${COURT_ID}" ]; then
        echo "Missing required first parameter: court_id"
        exit 2
    fi

    if [ -z "${JOB_NAME}" ]; then
        echo "Missing required second parameter: job_name"
        exit 2
    fi

    if [ -z "${JOB_PREFIX}" ]; then
        echo "Missing required third parameter: job_prefix (date/month)"
        exit 2
    fi

    export K8S_JOB_NAME="${JOB_NAME//_/-}-${JOB_PREFIX//\//-}"

    echo "Applying file job.yaml"
    envsubst < job.yaml | kubectl apply -f -
}

if [[ "${BASH_SOURCE[0]}" = "${0}" ]]; then
    deploy "$@"
fi
