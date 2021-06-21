#! /bin/bash

version=2.0

docker tag zeebe-simple-monitor:${version}   csighub.tencentyun.com/sre/zeebe-simple-monitor:${version}
docker push csighub.tencentyun.com/sre/zeebe-simple-monitor:${version}

