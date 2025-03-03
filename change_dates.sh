#!/bin/bash

git filter-branch --force --env-filter '
OLD_DATE="Mar 5, 2025"
NEW_DATE="Feb 28, 2025 17:30:00 +0300"

if [[ $GIT_COMMIT = 9e6dab9* || $GIT_COMMIT = 7eda448* || $GIT_COMMIT = * ]]
then
    export GIT_AUTHOR_DATE="$NEW_DATE"
    export GIT_COMMITTER_DATE="$NEW_DATE"
fi
' --tag-name-filter cat -- --all
