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
# -f - path to fallocate binary (default is to just call the fallocate command, look in PATH)
# -n - name of database (default: data)
# -d - where to place the files (default: /data/db)

# Error/safety checks
# 
# TODO - check for existing files first, if they exist, bail out and carp
# TODO - check for sufficient free space on target device
# TODO - add smallfiles option (divide by 4)
     
# set the defaults
SIZE=192
NAME="data"
FBINARY="fallocate"
DBPATH="/data/db"  


# Parse arguments, overwrite defaults when necessary, error if invalid arg passed
while getopts ":s:n:p:d:" opt; do
  case $opt in
    s) SIZE="$OPTARG"
    ;;
    n) NAME="$OPTARG"
	;;
    f) FBINARY="$OPTARG"
	;;
    d) DBPATH="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

command -v $FBINARY >/dev/null || { echo "fallocate command not found in PATH, cannot continue, please install util-linux package or similar, or provide the full path to the command."; exit 1; }

# Create namespace first - always needed

$FBINARY -l $((1024 * 1024 * 16)) $DBPATH/$NAME.ns

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
	  $FBINARY -l $((1024 * 1024 * 64)) $DBPATH/$NAME.$ALLOCATED
	  ((ALLOCATED++))
	  ;;
	1)
	  $FBINARY -l $((1024 * 1024 * 128)) $DBPATH/$NAME.$ALLOCATED
	  ((ALLOCATED++))
	  ;;  
	2) 
	  $FBINARY -l $((1024 * 1024 * 256)) $DBPATH/$NAME.$ALLOCATED
          ((ALLOCATED++))
	  ;;  
	3) 
	  $FBINARY -l $((1024 * 1024 * 512)) $DBPATH/$NAME.$ALLOCATED
          ((ALLOCATED++))
	  ;;  
	4) 
	  $FBINARY -l $((1024 * 1024 * 1024)) $DBPATH/$NAME.$ALLOCATED
          ((ALLOCATED++))
	  ;;
	*) 
	  $FBINARY -l $((1024 * 1024 * 2048)) $DBPATH/$NAME.$ALLOCATED
          ((ALLOCATED++))
	  ;;
	esac
done
