drop database if exists dingdongji cascade;
create database dingdongji;
use dingdongji;

create external table cdinfo (
    cellid bigint,
    times bigint,
    register int,
    phone bigint
)
row format delimited
fields terminated by ','
stored as textfile
location 'hdfs://master:8020/user/root/redcode/generated_512mb/cdinfo_fixed_big';

create external table infected (
    phone bigint
)
row format delimited
fields terminated by ','
stored as textfile
location 'hdfs://master:8020/user/root/redcode/generated_512mb/infected_big';

select count(*) from cdinfo;
select count(*) from infected;