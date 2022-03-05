#/bin/bash

repeat_num=$1

if [ repeat_num == "" ]; then
  echo "please input number of times to run"
else
  if echo $repeat_num | egrep -q '^[0-9]+$'; then
    # $var is a number
    for (( i=0; i < $repeat_num; i++ )); do
        python3 utils/ACS_update_citation_only.py
    done
  else
    # $var is not a number
    echo "please input a number"
  fi
fi