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
spark-submit --master yarn --deploy-mode client --class com.tipdm.covid19.Redcode /data/BigDataFinal/Hausaufgabe/out/artifacts/dagoujiao/dagoujiao.jar dingdongji.cdinfo dingdongji.infected dingdongji.infected_info dingdongji.all_timerange dingdongji.infected_timerange dingdongji.contacts include robust hdfs://master:8020/user/root/out
```

各个参数意义为

```shell
spark-submit --master yarn --deploy-mode client --class com.tipdm.covid19.Redcode /data/BigDataFinal/Hausaufgabe/out/artifacts/dagoujiao/dagoujiao.jar <cdinfo_fixed表名> <infected表名> <infected_info表名> <all_timerange表名> <infected_timerange表名> <contacts表名> <是否包括感染者自己> <是否采用稳健方法> <结果存放路径>
```

更具体的介绍直接看 `Redcode.scala` 的代码注释罢。

# 当前方法的问题

目前方法的缺陷是不能用 `--deploy-mod cluster` ，这几乎一定会影响处理速度吧？解释是我们教学平台用的集群不是真集群？解决方案有如下选择：

1. 能搞真集群吗？课件里有说相关内容吗？
2. 修改代码中涉及路径、存储的部分使得项目不依赖 `Hive` ，直接处理 `HDFS` 层面的文件，但是这带来的问题似乎是不能用 `spark.sql` ，我暂时不确定会带来多大的影响。
3. 换方法（真的吗？）

# TODO

1. 目前只是肉眼看了前二十行，处理结果与 `python` 的结果一致，还没有完全核对过，也许应该严格核对一下？
2. 如前文所述，需要处理一下 `cluster` 的问题。
3. 目前还没有生成大规模数据，没有在大规模数据上试过，需要试一试。
4. 这一版本的代码中有许多屎山，包括各种 `count()` `cache()` 等，当时加这些是因为程序处理完数据之后，`spark-shell` 警告我找不到本来应该出现的数据表，处理这一异常的过程中，我考虑了缓存、惰性计算等各种因素，最后正常了，但我也不知道哪些是能删的。具体哪些是潜在屎山还是看代码注释吧。