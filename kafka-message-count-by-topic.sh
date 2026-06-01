#!/bin/bash

export PATH=.:/opt/kafka/bin:$PATH

function format_time() {
   local ts=$1
   date -d "$ts" "+%s"
}

function get_offset() {
   local bs="$1"; local config="$2"; local topic="$3"; local ts="$4"
   kafka-get-offsets.sh --bootstrap-server $bs --command-config $config --topic $topic --time $(format_time "$ts")000
}

declare -A __start_offset_by_partition
declare -A __end_offset_by_partition

function parse_start_offsets() {
   local offsets=$1; local topic; local partition; local offset

   while IFS=: read -r topic partition offset; do
      __start_offset_by_partition[$partition]=$offset
   done <<< $offsets
}

function parse_end_offsets() {
   local offsets=$1; local topic; local partition; local offset

   while IFS=: read -r topic partition offset; do
      __end_offset_by_partition[$partition]=$offset
   done <<< $offsets
}

function compute_statistics() {
   local bs=$1; topic=$2; local ts="$3"; local ts2="$4"

   local total_messages=0

   for pkey in ${!__end_offset_by_partition[@]}; do
      end_offset=${__end_offset_by_partition[$pkey]}

      if [[ -z ${__start_offset_by_partition[$pkey]+x} ]]; then
         echo topic $topic: no start offset for partition $pkey
         (( total_messages = total_messages + end_offset ))

      else
         start_offset=${__start_offset_by_partition[$pkey]}
         (( total_messages = total_messages + end_offset - start_offset ))
      fi
   done


   #local dt_sec
   #(( dt_sec = $(format_time "$ts2") - $(format_time "$ts1" ) ))
   #local avg_mps=$(echo "scale=2; $total_messages/$dt_sec" | bc)

   echo ""
   echo bootstrap: $bs
   echo start time: "$ts1"
   echo end time: "$ts2"
   echo topic: $topic
   echo total messages: $total_messages
   echo ""
}

#set -x

config=$1
topic=$2
ts1=$3
ts2=$4

(( dt_sec = $(format_time "$ts2") - $(format_time "$ts1") ))
if (( dt_sec <= 0 )); then
   echo invalid time interval: "$ts1", "$ts2"
   exit 1
fi

if [[ ! -f $config ]]; then
   echo config file $config not found
   exit 1
fi

bs=$(cat $config | grep "bootstrap.servers" | cut -d= -f2 -)

start_offsets=$(get_offset "$bs" "$config" "$topic" "$ts1")
echo $start_offsets

end_offsets=$(get_offset "$bs" "$config" "$topic" "$ts2")
echo $end_offsets

if [[ ! -z $start_offsets ]]; then
   parse_start_offsets $start_offsets
else
   echo no messages before "$ts1" UTC or timestamp is past last message on topic $topic
fi

if [[ ! -z $end_offsets ]]; then
   parse_end_offsets $end_offsets
else
   echo no messages before "$ts2" UTC or timestamp is past last message on topic $topic
fi

compute_statistics $bs $topic "$ts1" "$ts2"
