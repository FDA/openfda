#!/bin/bash

# The set of commands run from a base Ubuntu server to prepare docker and
# other executables.

export DEBCONF_FRONTEND=noninteractive
sudo apt-get -y update
sudo apt-get -y install docker.io
sudo ln -sf /usr/bin/docker.io /usr/local/bin/docker
sudo sed -i '$acomplete -F _docker docker' /etc/bash_completion.d/docker.io

sudo docker pull dockerfile/java:oracle-java7
sudo docker pull dockerfile/nodejs

sudo apt-get -y install python-pip
sudo pip install awscli
