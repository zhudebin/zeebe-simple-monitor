#! /bin/bash

FROM openjdk:11-jdk-buster

RUN mkdir -p /usr/local/zeebe-simple-monitor/config

ADD target/zeebe-simple-monitor-0.20.1.jar /usr/local/zeebe-simple-monitor/zeebe-simple-monitor.jar

WORKDIR /usr/local/zeebe-simple-monitor

ENTRYPOINT ["java","-jar","/usr/local/zeebe-simple-monitor/zeebe-simple-monitor.jar"]