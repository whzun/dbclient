#!/bin/sh
cd `dirname $0`
rlwrap java -Dfile.encoding=UTF-8 -jar ${artifactId}-${version}.jar $*