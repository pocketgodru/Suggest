#!/bin/bash
current_branch=$(git symbolic-ref --short HEAD)
git checkout -b temp_branch
git filter-branch --env-filter '
    export GIT_AUTHOR_DATE="2025-02-28T17:30:00 +0300"
    export GIT_COMMITTER_DATE="2025-02-28T17:30:00 +0300"
' --tag-name-filter cat -- --all
git checkout $current_branch
git reset --hard temp_branch
git branch -D temp_branch
git push -f origin $current_branch