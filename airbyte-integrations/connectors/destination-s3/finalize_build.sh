#!/bin/bash
set -e

echo "Running destination-s3 docker custom steps..."

ARCH=$(uname -m)
echo "SGX $ARCH OK"

if [ "$ARCH" == "x86_64" ] || [ "$ARCH" = "amd64" ]; then
  echo "$ARCH"
  yum install lzop lzo lzo-devel -y
elif [ "$ARCH" = "aarch64" ]; then
  echo "installing x86_64 packages on aarch64"
  rpm --ignorearch -Uvh --nodeps $(dnf repoquery --forcearch=x86_64 --location lzop)
  rpm --ignorearch -Uvh --nodeps $(dnf repoquery --forcearch=x86_64 --location lzo)
  rpm --ignorearch -Uvh --nodeps $(dnf repoquery --forcearch=x86_64 --location lzo-devel)
else
  echo "can't install lzo for arch $ARCH!!"
fi

yum clean all
