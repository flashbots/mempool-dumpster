#!/bin/bash

# Iterate over files in current directory, and print filename, lines and size
for x in $(ls -1); do
	size=$( du --si -s $x | cut -f1 )
	size_app=$( du --si -s --apparent-size $x | cut -f1 )
	lines=$( cat $x | wc -l )
	printf "%s \t %'10d \t %s \t %s \n" $x $lines $size $size_app
done

#for x in $(ls -d -1 -- */); do
#	size=$( du --si -s $x | cut -f1 )
#	files=$( find $x -type f | wc -l )
#	echo -e "$x \t $size \t $files"
#done
