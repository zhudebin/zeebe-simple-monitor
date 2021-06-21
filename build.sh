#! /bin/bash

version=2.0

mvn clean package -DskipTests

docker build -t zeebe-simple-monitor:${version} .