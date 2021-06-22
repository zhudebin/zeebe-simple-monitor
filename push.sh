#! /bin/bash

version=2.0

docker tag zeebe-simple-monitor:${version}   ccr.ccs.tencentyun.com/sre_pcg/zeebe-simple-monitor:${version}
docker push ccr.ccs.tencentyun.com/sre_pcg/zeebe-simple-monitor:${version}

