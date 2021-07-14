#!/bin/bash


#To compile JDK with debug symbols:
# Choose one that fits your needs. 
#bash ./configure --disable-warnings-as-errors --with-native-debug-symbols=internal  --with-debug-level=slowdebug --with-target-bits=64
#bash ./configure --disable-warnings-as-errors --with-native-debug-symbols=internal  --with-debug-level=fastdebug --with-target-bits=64
#bash ./configure --disable-warnings-as-errors --with-native-debug-symbols=none  --with-debug-level=release --with-target-bits=64


# Default compilation option is "release".
bash ./configure --disable-warnings-as-errors  --with-target-bits=64

