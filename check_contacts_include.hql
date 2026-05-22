set hive.cli.print.header=false;
set hive.resultset.use.unique.column.names=false;

use dingdongji;

drop table if exists checker_cdinfo;
drop table if exists checker_infected;
drop table if exists checker_timerange;
drop table if exists checker_expected;

create table checker_cdinfo as
select
    cast(phone as bigint) as phone,
    cast(cellid as bigint) as cellid,
    cast(times as bigint) as times,
    cast(register as int) as register
from cdinfo;

create table checker_infected as
select distinct cast(phone as bigint) as phone
from infected;

create table checker_timerange as
select
    e.phone,
    e.cellid,
    e.times as enter_time,
    min(x.times) as exit_time
from (
    select
        phone,
        cellid,
        times,
        register,
        lag(register) over(partition by phone, cellid order by times) as prev_register
    from checker_cdinfo
) e
join checker_cdinfo x
    on e.phone = x.phone
   and e.cellid = x.cellid
   and x.register = 2
   and x.times >= e.times
where e.register = 1
  and (e.prev_register is null or e.prev_register <> 1)
group by e.phone, e.cellid, e.times;

create table checker_expected as
select distinct a.phone as phone
from checker_timerange i
join checker_infected inf
    on i.phone = inf.phone
join checker_timerange a
    on a.cellid = i.cellid
   and a.enter_time <= i.exit_time
   and a.exit_time >= i.enter_time;

select 'expected_count', count(*) from checker_expected;
select 'spark_contacts_count', count(*) from contacts;

select 'missing_in_spark', count(*)
from checker_expected e
left join contacts c
    on e.phone = c.phone
where c.phone is null;

select 'extra_in_spark', count(*)
from contacts c
left join checker_expected e
    on c.phone = e.phone
where e.phone is null;

select 'duplicate_phone_groups_in_contacts', count(*)
from (
    select phone
    from contacts
    group by phone
    having count(*) > 1
) t;
