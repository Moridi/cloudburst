#!/bin/bash

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

if [ -z "$1" ]; then
  echo "Usage: ./$0 build"
  echo ""
  echo "You must run this from the project root directory."
  exit 1
fi

if [ "$1" = "y" ] || [ "$1" = "yes" ]; then
  ./scripts/clean.sh
  ./scripts/build.sh
fi

# We need this on Ubuntu to make sure that the packages are found when
# attempting to execute the tests.
export PYTHONPATH=$PYTHONPATH:$(pwd)
CORES="3,7,11,15,19,23,27,31,35,39,43,47,51,55,59,63,67,71,75,79,83,87,91,95"

taskset -c $CORES python3 cloudburst/server/scheduler/server.py conf/cloudburst-local.yml &

nthreads=$3
for (( c=0; c<$nthreads; c++ ))
do 
    echo "initializing executor with tid=$c" 
   taskset -c $CORES python3 cloudburst/server/executor/server.py conf/cloudburst-local.yml $c &
done