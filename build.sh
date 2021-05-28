#! /bin/bash

mvn clean package -DskipTests

docker build -t zeebe-simple-monitor:1.5 .