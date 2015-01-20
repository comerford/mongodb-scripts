#!/bin/bash
#
# Tool to pre-allocate MongoDB data files for MMAPv0/1 storage engine
# 
# Requirements: fallocate command from Google ftools - https://code.google.com/p/linux-ftools/#fallocate
# 
# Author: Adam Comerford (adam@comerford.cc)
#
# Options:
#  
# -s - size of data files to allocate in MB, not including namespace file (default: 192) 
# -n - name of database (default: data)
# -p - where the fallocate command lives (if not in PATH)
# -d - where to place the files (default: /data/db)

# Error/safety checks
# 
# TODO - check for the existence of the fallocate command (if not, then prompt to install util-linux or equivalent)
# TODO - check for existing files first, if they exist, bail out and carp
# TODO - check for sufficient free space on target device
# TODO - add smallfiles option (divide by 4)
     
# set the defaults
SIZE=192
NAME="data"
BINPATH="/usr/bin"
DBPATH="/data/db"  


# Parse arguments, overwrite defaults when necessary, error if invalid arg passed
while getopts ":s:n:p:d:" opt; do
  case $opt in
    s) SIZE="$OPTARG"
    ;;
    n) NAME="$OPTARG"
	;;
    p) BINPATH="$OPTARG"
	;;
    d) DBPATH="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done



# Create namespace first - always needed

$BINPATH/fallocate -l $((1024 * 1024 * 16)) $DBPATH/$NAME.ns

# calculate the number of files that will be required
# 4032 is the magic number, anything beyond that will have multiples of 2048
NUMFILES=0

if [ $SIZE -le 4032 ] ;
	then
	# only a few cases to deal with here
	if [ $SIZE -le 192 ] ;
		then
	    NUMFILES=2
	elif [ $SIZE -le 448 ] ;
		then
		NUMFILES=3
	elif [ $SIZE -le 960 ] ;
		then
		NUMFILES=4
	elif [ $SIZE -le 1984 ] ;
		then 
		NUMFILES=5
	else
		NUMFILES=6
	fi 	 
else
	# for larger than 4032, will always be 7 plus however many 2048 files are needed additionally
	NUMFILES=$(( (($SIZE - 4032)/2048) + 7 ))
fi	

ALLOCATED=0      
while [ $ALLOCATED -lt $NUMFILES ]; do
  	case $ALLOCATED in
	0)
	  $BINPATH/fallocate -l $((1024 * 1024 * 64)) $DBPATH/$NAME.$ALLOCATED
	  ((ALLOCATED++))
	  ;;
	1)
	  $BINPATH/fallocate -l $((1024 * 1024 * 128)) $DBPATH/$NAME.$ALLOCATED
	  ((ALLOCATED++))
	  ;;  
	2) 
	  $BINPATH/fallocate -l $((1024 * 1024 * 256)) $DBPATH/$NAME.$ALLOCATED
      ((ALLOCATED++))
	  ;;  
	3) 
	  $BINPATH/fallocate -l $((1024 * 1024 * 512)) $DBPATH/$NAME.$ALLOCATED
      ((ALLOCATED++))
	  ;;  
	4) 
	  $BINPATH/fallocate -l $((1024 * 1024 * 1024)) $DBPATH/$NAME.$ALLOCATED
      ((ALLOCATED++))
	  ;;
	*) 
	  $BINPATH/fallocate -l $((1024 * 1024 * 2048)) $DBPATH/$NAME.$ALLOCATED
      ((ALLOCATED++))
	  ;;
	esac
done
