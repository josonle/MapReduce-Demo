#!/bin/bash
num=$1
[[ -z $num ]] && num=100

for ((i=1;i<=$num;i++))
do
  year=$(expr $RANDOM % 3 + 2015)
  month=$(expr $RANDOM % 12 + 1)
  
  case $month in 
    1 | 3 | 5 | 7 | 8 | 10 | 12)
      day=$(expr $RANDOM % 31 + 1)
      ;;
    2)
      if [[ $year -eq 2016 && $month -eq 2 ]]
      then
        day=$(expr $RANDOM % 29 + 1)
      else
        day=$(expr $RANDOM % 28 + 1)
      fi  
      ;;
    4 | 6 | 9 | 11)
      day=$(expr $RANDOM % 30 + 1)
      ;;
  esac

  if [[ $month -lt 10 ]]
  then 
    month=0$month
  fi

  if [[ $day -lt 10 ]]
  then 
    day=0$day
  fi

  echo "$year-$month-$day:$i"
done
