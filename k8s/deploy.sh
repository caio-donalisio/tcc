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
    export K8S_JOB_NAME=${1//_/-}
    export JOB_NAME=$2
    export IMAGE_TAG=$3
    shift 3
    export PARAMS=$*

    if [ -z "${PARAMS}" ]; then
        echo "Missing required params for job"
        exit 2
    fi

    echo "Applying file job.yaml"
    kubectl -n crawlers delete job "crawler-${K8S_JOB_NAME}"
    envsubst < job.yaml | kubectl apply -f -
}

if [[ "${BASH_SOURCE[0]}" = "${0}" ]]; then
    deploy "$@"
fi
