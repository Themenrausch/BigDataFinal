# 更新流程

第一次：

```shell
git clone git@github.com:Themenrausch/BigDataFinal.git
```

后续：

```shell
git checkout master
git pull
git checkout -b feature/xxx (-b只有第一次创建分支要加，后续再在这个分支上工作只是去这个分支)
```

对项目进行改动之后：

```shell
git add .
git commit -m "xxx"
git push -u origin feature/xxx (-u以及之后的内容也只有第一次push这个分支需要加)
```

GitHub 提示 `Compare & pull request` 

# 粗略介绍：一个大概率能用的原始版本。

## 文件介绍

`smallsample.ipynb` 中是小样本情况下的直观处理方案，同时也能生成一份供正确性参考的答案。

`dingdongji.hql` 用于建立数据库、数据表。

`Redcode.scala` 是从 `smallsample.ipynb` 翻译过来的，开发环境下运行的版本。

`integeratedRedcode.scala` 是 `Redcode.scala` 的集群环境版本。

`BigDataBuilder.scala` 是模拟数据生成脚本，生成方式为把已有样例数据按副本复制很多份，同时重新映射手机号和基站编号。

`dingdongji_big_external.hql` 是 `dingdongji.hql` 的集群版本，仅是将数据读取位置改为hdfs。

`check_contacts_include` 是hive结果校验脚本，即尝试使用hive独立完成任务，与spark处理结果比较是否一致。

## 使用介绍

用到了 `Hadoop` `Hive` `Spark` 

使用流程是先

```shell
$HADOOP_HOME/sbin/start-all.sh
$SPARK_HOME/sbin/start-all.sh
service mysqld restart
hive --service metastore &
hive --service hiveserver2 &
```

之后确保 `cdinfo_fixed.txt` `infected.txt` `dingdongji.hql` 都在 `/data` 下，然后

```shell
hive -f /data/dingdongji.hql
```

建立数据库与数据表。

接下来根据自己的 `dagoujiao.jar` 的位置，执行如下命令

```shell
spark-submit --master yarn --deploy-mode client --class com.tipdm.covid19.Redcode /data/BigDataFinal/out/artifacts/dagoujiao/dagoujiao.jar dingdongji.cdinfo dingdongji.infected dingdongji.infected_info dingdongji.all_timerange dingdongji.infected_timerange dingdongji.contacts include robust hdfs://master:8020/user/root/out
```

或者

```shell
spark-submit \
--master yarn \
--deploy-mode cluster \
--files $HIVE_HOME/conf/hive-site.xml \
--num-executors 4 \
--executor-cores 3 \
--executor-memory 1G \
--conf spark.sql.shuffle.partitions=64 \
--class com.tipdm.covid19.Redcode \
/data/BigDataFinal/Hausaufgabe/out/artifacts/dagoujiao/dagoujiao.jar \
dingdongji.cdinfo \
dingdongji.infected \
dingdongji.infected_info \
dingdongji.all_timerange \
dingdongji.infected_timerange \
dingdongji.contacts \
include \
robust \
hdfs://master:8020/user/root/out
```

各个参数意义为

```shell
spark-submit --master yarn --deploy-mode client --class com.tipdm.covid19.Redcode /data/BigDataFinal/Hausaufgabe/out/artifacts/dagoujiao/dagoujiao.jar <cdinfo_fixed表名> <infected表名> <infected_info表名> <all_timerange表名> <infected_timerange表名> <contacts表名> <是否包括感染者自己> <是否采用稳健方法> <结果存放路径>
```

更具体的介绍直接看 `Redcode.scala` 的代码注释罢。

完成后执行以下指令导出结果文件

```shell
hdfs dfs -cat hdfs://master:8020/user/root/out/part-* > /data/redmark01.txt
```

处理结果校验流程如下
首先检查文件格式，假设结果文件为 `\data` 下的 `remark01.txt`，执行以下指令

```shell
awk 'NF != 1 || $1 !~ /^[0-9]+$/ {print "bad line:", NR, $0; bad=1} END{exit bad}' /data/redmark01.txt
sort -n -c /data/redmark01.txt
sort -n /data/redmark01.txt | uniq -d | head
```

指令依次为：检查“手机号逐行排列、没有空行、没有乱字符”、检查手机号升序、检查重复手机号，若无输出，则说明正确

然后执行

```shell
hive -f /data/check_contacts_include.hql
```

调用hive校验脚本，若正确，则结果日志中会出现

```shell
missing_in_spark    0
extra_in_spark      0
duplicate_phone_groups_in_contacts    0
```

以下为模拟数据生成脚本使用方式
执行命令调用脚本

```shell
spark-submit \
--master yarn \
--deploy-mode cluster \
--num-executors 4 \
--executor-cores 3 \
--executor-memory 1G \
--class com.tipdm.covid19.BigDataBuilder \
/data/BigDataFinal/DataGenerator/out/artifacts/dagoujiao/dagoujiao.jar \
hdfs://master:8020/user/root/redcode/sample/cdinfo_fixed.txt \
hdfs://master:8020/user/root/redcode/sample/infected.txt \
4818 \
1 \
hdfs://master:8020/user/root/redcode/generated_8G \
32
```

即从hdfs上的样例 `cdinfo_fixed.txt` 和 `infected.txt` 读取数据，把 `cdinfo` 复制100份；只把第1份里的感染者写入新的 infected_big；生成结果写到 `hdfs://master:8020/user/root/redcode/generated_100`；输出时使用 4 个分区。

# 当前方法的问题

目前方法的缺陷是不能用 `--deploy-mod cluster` ，这几乎一定会影响处理速度吧？解释是我们教学平台用的集群不是真集群？解决方案有如下选择：

1. 能搞真集群吗？课件里有说相关内容吗？
2. 修改代码中涉及路径、存储的部分使得项目不依赖 `Hive` ，直接处理 `HDFS` 层面的文件，但是这带来的问题似乎是不能用 `spark.sql` ，我暂时不确定会带来多大的影响。
3. 换方法（真的吗？）

# TODO

1. 目前只是肉眼看了前二十行，处理结果与 `python` 的结果一致，还没有完全核对过，也许应该严格核对一下？（done?）
2. 如前文所述，需要处理一下 `cluster` 的问题。（done?）
3. 目前还没有生成大规模数据，没有在大规模数据上试过，需要试一试。（done?）
4. 这一版本的代码中有许多屎山，包括各种 `count()` `cache()` 等，当时加这些是因为程序处理完数据之后，`spark-shell` 警告我找不到本来应该出现的数据表，处理这一异常的过程中，我考虑了缓存、惰性计算等各种因素，最后正常了，但我也不知道哪些是能删的。具体哪些是潜在屎山还是看代码注释吧。
