#!/bin/bash

BRANCHES=$(git branch --contains ${{ github.ref_name }})
set -- $BRANCHES
for BRANCH in $BRANCHES; do
  if [[ "$BRANCH" == "$1" ]]; then
    exit 0
  fi
done
echo "You can release only $1 branch."
exit 1
