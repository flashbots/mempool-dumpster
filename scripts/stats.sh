#!/bin/bash
#
# print stats (lines and disk usage) for all CSV files in a directory/subdirectory
#
set -o errexit
set -o nounset
set -o pipefail

# requires directory as argument
if [ $# -eq 0 ]
  then
    echo "Usage: $0 <directory>"
    exit 1
fi

# requires directory to exist
if [ ! -d "$1" ]; then
  echo "Directory $1 does not exist"
  exit 1
fi

# iterate over all CSV files in any subdirectory
for x in $(ls -1 -- $1/**/*.csv); do
	size=$( du --si -s $x | cut -f1 )
	lines=$( cat $x | wc -l )
	printf "%s \t %'10d \t %8s \n" $x $lines $size
done

