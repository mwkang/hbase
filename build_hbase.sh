#!/bin/bash

export MAVEN_OPTS="-Xms4g -Xmx4g"

mvn clean package -DskipTests -Drat.skip=true -Drat.ignoreErrors=true -Dcheckstyle.skip=true -Dhadoop.profile=3.0 -Dadditionalparam=-Xdoclint:none assembly:single
