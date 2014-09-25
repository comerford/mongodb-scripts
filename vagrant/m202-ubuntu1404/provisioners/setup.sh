#!/bin/sh

set -e

HOME="/home/m202"
COURSE="m202"
MONGOPROC="$HOME/mongoProc"

echo 'Setting up VM...'

#echo 'Updating system... this may take a while'
#apt-get -y update > /dev/null 2>&1
#DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" upgrade > /dev/null 2>&1

#echo 'Done! Rebooting...'

#reboot
