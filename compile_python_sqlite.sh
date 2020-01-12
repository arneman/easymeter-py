#!/bin/bash
#set current python version here !
PYTHON_VERS=3.8.1
PYTHON_VERS_SHORT=3.8

cd ~ 

sudo apt-get update -y
sudo apt-get install build-essential tk-dev libncurses5-dev libncursesw5-dev libreadline6-dev libdb5.3-dev libgdbm-dev libsqlite3-dev libssl-dev libbz2-dev libexpat1-dev liblzma-dev zlib1g-dev libffi-dev -y
wget https://www.python.org/ftp/python/$PYTHON_VERS/Python-$PYTHON_VERS.tar.xz
tar xf Python-$PYTHON_VERS.tar.xz
cd Python-$PYTHON_VERS
./configure --enable-optimizations --enable-loadable-sqlite-extensions
make -j 4
sudo make altinstall
cd ..
sudo rm -r Python-$PYTHON_VERS
rm Python-$PYTHON_VERS.tar.xz

#install update-alternatives
sudo update-alternatives --install /usr/local/bin/python3 python3 /usr/local/bin/python$PYTHON_VERS_SHORT 1
echo "choose python $PYTHON_VERS_SHORT if you want to set it as default. Otherwise exit with CTRL+C"
sudo update-alternatives --config python3

#sudo apt-get --purge remove build-essential tk-dev libncurses5-dev libncursesw5-dev libreadline6-dev libdb5.3-dev libgdbm-dev libsqlite3-dev libssl-dev libbz2-dev libexpat1-dev liblzma-dev zlib1g-dev libffi-dev -y
#sudo apt-get autoremove -y
#sudo apt-get clean