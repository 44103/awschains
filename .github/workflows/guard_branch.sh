#!/bin/bash

BRANCHES=$(git branch --contains ${{ github.ref_name }})
set -- $BRANCHES
for BRANCH in $BRANCHES; do
  if [[ "$BRANCH" == "main" ]]; then
    exit 0
  fi
done
echo "You can release only main branch."
exit 1
