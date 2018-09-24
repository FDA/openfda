#!/bin/bash

# The set of commands run from a base Ubuntu server to prepare a development image.
# They should be run from an SSH connection with agent forwarding enabled.

export DEBCONF_FRONTEND=noninteractive
sudo apt-get -y update
sudo apt-get -y install docker.io
sudo ln -sf /usr/bin/docker.io /usr/local/bin/docker
sudo sed -i '$acomplete -F _docker docker' /etc/bash_completion.d/docker.io

sudo docker pull dockerfile/java:oracle-java7
sudo docker pull dockerfile/nodejs

sudo apt-get -y install python-pip python-dev npm libxslt1-dev python-lxml
sudo apt-get -y install git
sudo apt-get -y install xfsprogs
sudo apt-get -y install unzip
sudo apt-get -y install zbar-tools gnumeric

sudo pip install awscli

(
  mkdir -p $HOME/pkg
  pushd $HOME/pkg
  wget http://nodejs.org/dist/v0.10.32/node-v0.10.32-linux-x64.tar.gz
  tar -xzf ../node-v0.10.32-linux-x64.tar.gz
  ln -s node-v0.10.32-linux-x64 node-current
  popd
)

echo 'export PATH=$PATH:$HOME/.local/bin:$HOME/pkg/node-current/bin' >> ~/.bashrc
export PATH=$PATH:$HOME/.local/bin:$HOME/pkg/node-current/bin

sudo mkdir /media/ebs
sudo mount /dev/xvdf /media/ebs
sudo chmod 777 /media/ebs

(
  git clone git@github.com:iodine/openfda-internal
  cd openfda-internal
  ./bootstrap.sh
  ln -s /media/ebs ./data
)
