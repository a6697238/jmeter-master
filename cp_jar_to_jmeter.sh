#!/bin/bash

target_path='/apps/apache-jmeter-3.0-modify'
temp_path='/apps/jmeter-work-dir'
find . -type f -iname '*scala-library*.jar' -exec cp {}  $target_path/lib \;
find . -type f -iname '*kafka*0.8.2.2*.jar' -exec cp {}  $target_path/lib \;
find . -type f -iname '*metrics-core*.jar' -exec cp {}  $target_path/lib \;
find . -type f -iname '*fastjson*.jar' -exec cp {}  $target_path/lib \;

# replace summarizer.class
mvn package -Dmaven.test.skip=true
cp -r ./target/classes/org/apache/jmeter $temp_path/ApacheJMeter_core/org/apache/jmeter


echo 'enter into jmeter_core_jar'
cd $temp_path
pwd

echo 'zip ApacheJMeter_core.jar ...'
zip -qr ApacheJMeter_core.jar .

cp ApacheJMeter_core.jar $target_path/lib/ext

echo 'run jmeter...'
sh $target_path/bin/jmeter -n -t $target_path/bin/test-random.jmx


cd /apps
tar -zcf apache-jmeter-3.0.tar.gz apache-jmeter-3.0
ls -lh
echo 'scp tar to remote host ...'

echo 'tar -zxf remote host jmeter...'
for i in 186 187 188
do
    scp apache-jmeter-3.0.tar.gz "root@172.17.103.$i:/apps"
    ssh "root@172.17.103.$i" "tar -zxf /apps/apache-jmeter-3.0.tar.gz -C /apps"

    echo "root@172.17.103.$i synchronization is complete"
done

