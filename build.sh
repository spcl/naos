#!/bin/bash

read -p "Flavour (release, fastdebug, slowdebug)? " flavour

while true; do
  read -p "Clean? (yes, no) " clean
  case $clean in
    [Yy]* ) make CONF=$flavour clean; time make CONF=$flavour images; break;;
    [Nn]* ) time make CONF=$flavour images; break;;
        * ) echo "Invalid flavour... Try again (release, fastdebug, slowdebug). ";;
  esac
done
paplay /usr/share/sounds/freedesktop/stereo/complete.oga
