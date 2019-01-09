set -e
#date
#sudo apt-get update -qq
#sudo apt-get install build-essential
#sudo apt-get install pv
#date
#mkdir protobuf_install
#pushd protobuf_install
#wget https://github.com/google/protobuf/archive/v3.5.1.tar.gz -O protobuf-3.5.1.tar.gz
#tar xzf protobuf-3.5.1.tar.gz
#cd protobuf-3.5.1
#sudo apt-get install autoconf automake libtool curl make g++ unzip
#./autogen.sh
#./configure
#make
#sudo make install
#sudo ldconfig
#protoc --version
#popd
#date
#pwd
#sudo apt-get install -qq libboost-dev libboost-test-dev libboost-program-options-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev
#wget -nv http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz
#tar zxf thrift-0.9.3.tar.gz
#cd thrift-0.9.3
#chmod +x ./configure
#./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang --without-php --without-nodejs
#sudo make install
#cd ..
TRAVIS_BRANCH=master
branch_specific_script="dev/travis-before-install-${TRAVIS_BRANCH}.sh"
if [[ -e "$branch_specific_script" ]]
then
    . "$branch_specific_script"
fi
date
