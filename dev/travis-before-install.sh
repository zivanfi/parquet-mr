# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -e
date
sudo apt-get update -qq
sudo apt-get install -qq build-essential pv autoconf automake libtool curl make \
   g++ unzip libboost-dev libboost-test-dev libboost-program-options-dev \
   libevent-dev automake libtool flex bison pkg-config g++ libssl-dev xmlstarlet
date
mkdir protobuf_install
pushd protobuf_install
wget https://github.com/google/protobuf/archive/v3.5.1.tar.gz -O protobuf-3.5.1.tar.gz
tar xzf protobuf-3.5.1.tar.gz
cd protobuf-3.5.1
./autogen.sh
./configure
make
sudo make install
sudo ldconfig
protoc --version
popd
date
pwd
wget -nv http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz
tar zxf thrift-0.9.3.tar.gz
cd thrift-0.9.3
chmod +x ./configure
./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang --without-php --without-nodejs
sudo make install
cd ..
branch_specific_script="dev/travis-before-install-${TRAVIS_BRANCH}.sh"
if [[ -e "$branch_specific_script" ]]
then
  . "$branch_specific_script"
fi
date
