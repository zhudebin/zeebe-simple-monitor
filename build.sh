#! /bin/bash

build() {
  echo "build -----"
  mvn clean package -DskipTests

  docker build -t zeebe-simple-monitor:${version} .
}

push() {
  echo "push image -----"
  docker tag zeebe-simple-monitor:${version}   ccr.ccs.tencentyun.com/sre_pcg/zeebe-simple-monitor:${version}
  docker push ccr.ccs.tencentyun.com/sre_pcg/zeebe-simple-monitor:${version}
}

version=2.1.0

build


if [ $# -gt 0 ] && [ $1=='push' ]; then

  push

fi







