#!/bin/bash

# Get to a predictable directory, the directory of this script
cd "$(dirname "$0")"

[ -e environment ] && . ./environment

while true; do
    git pull
    poetry install
    poetry run python -m spacy download en_core_web_sm
    FONTCONFIG_FILE=$PWD/extra/fonts.conf poetry run python -m tle

    (( $? != 42 )) && break

    echo '==================================================================='
    echo '=                       Restarting                                ='
    echo '==================================================================='
done
