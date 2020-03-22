FROM maven:3.6.3-jdk-8
COPY ./ /tmp/
WORKDIR /tmp
RUN mvn clean package

FROM openjdk:8
LABEL maintainer="dylanturn@gmail.com"
RUN addgroup -g 1001 -S stitcher && adduser -u 1001 -S stitcher -G stitcher
RUN mkdir /opt && chown -R stitcher:stitcher /opt
RUN mkdir /logs && chown -R stitcher:stitcher /logs
USER stitcher

RUN mkdir /opt/stitch
RUN mkdir /opt/stitch/libs
RUN mkdir /opt/stitch/bin

ADD /tmp/target/libs/* /opt/stitch/libs/*
ADD /tmp/target/stitch-1.0.jar /opt/stitch/bin/stitch.jar