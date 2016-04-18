# dumbo-hbase
[Dumbo](https://github.com/klbostee/dumbo) is a Python module that makes writing and running Hadoop
Streaming programs very easy. More generally, Dumbo can be considered
a convenient Python API for writing MapReduce programs.

However, dumbo cannot set hbase table as input or output. This tool to allow using dumbo over hbase.

### How to use
#### Build hbase-dumbo.jar
```shell
mvn package
cp target/dumbomr-0.0.1-SNAPSHOT-jar-with-dependencies.jar /path/to/hbase-dumbo.jar
```

#### Write dumbo jobs
```python
import dumbo
import json


# Use hbase table as input, and HDFS as output
def hbase_input_mapper(key, data):
    columns = json.loads(data)
    for family in columns:
        cf = columns[family]
        for qualifier in cf:
            yield key, (family, qualifier, cf[qualifier])

# Use hbase table as input, and hbase as output
def hbase_output_mapper(key, data):
    columns = json.loads(data)
    yield key, json.dumps(columns)


if __name__ == "__main__":
#    dumbo.run(hbase_input_mapper)
    dumbo.run(hbase_output_mapper)
```

**Note**
- If Hbase table is input, the key is the rowkey, the value is the json of columnfamilies.
- If output is Hbase table, the format is the same as input. Rowkey as output key, json format columnfamilies as output value.

#### Running command
I wrote a script to create dumbo command to make it easier to remember.
All you have to do is copy a config file, and modify some fields.

```
[input]
sc=/hbase/pctest

[env]
hadoop=/usr/local/hadoop-2.4.1/
#hadoop_path=/usr/local/hadoop-2.4.1/
#hadoop_path=/opt/hadoop-1.2.1/
#hadoop_lib=/usr/local/hadoop-2.4.1/share/hadoop/tools/lib/
hadooplib=/usr/local/hadoop-2.4.1/share/hadoop/tools/lib/

[main]
main_file=mr.py

#output_dir=/user/hadoop/pc/output
output=/user/hadoop/pc/output
outputformat=text
overwrite=yes
nummaptasks=1
numreducetasks=0
hbase_input=yes
hbase_output=yes
memlimit=2048000000

[hbase]
input=pctest
output=test
hbase.zookeeper.quorum=localhost
hbase.mapred.tablecolumns=f:domain,f:html
#hbase.mapred.tablecolumns="da:domain"
#columnfamilies=data

[dependence]
files=comp_date_cluster.cfg

[libegg]
libegg=1.egg
libegg=2.egg
➜  script git:(master) ✗ cat config.cfg
[input]
# hdfs input path, if you have more write more lines, key is useless
input=/hbase/pctest1
input=/hbase/pctest2

[env]
# hbase environment. For local test, just comment them.
hadoop=/usr/local/hadoop-2.4.1/
hadooplib=/usr/local/hadoop-2.4.1/share/hadoop/tools/lib/

[main]
# your main function
main_file=mr.py

# hdfs output path
output=/user/hadoop/pc/output

# hdfs output format(text, sequencefile), no need for hbase.
outputformat=text

overwrite=yes
nummaptasks=1
numreducetasks=0

# hbase as input, output, yes or no
hbase_input=yes
hbase_output=yes

# memory limit for one task
memlimit=2048000000

[hbase]
input=pctest
output=test
hbase.zookeeper.quorum=localhost

# only these columns are needed for input
hbase.mapred.tablecolumns=f:domain,f:html

# only these columnfamilies are needed for output
#columnfamilies=d,f

[dependence]
# mapreduce dependencies
files=comp_date_cluster.cfg

[libegg]
libegg=1.egg
libegg=2.egg
```

Then run dumbo job.
```shell
python runner.py config.cfg
```
