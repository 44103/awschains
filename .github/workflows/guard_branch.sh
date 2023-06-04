#!/bin/bash

TARGET_BRANCH=$1
TAG=$2
BRANCHES=$(git branch --contains $TAG)
set -- $BRANCHES
for BRANCH in $BRANCHES; do
  if [[ "$BRANCH" == "$TARGET_BRANCH" ]]; then
    exit 0
  fi
done
echo "You can release only $TARGET_BRANCH branch."
exit 1
