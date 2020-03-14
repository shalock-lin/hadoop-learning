### Hive 主流文件存储格式对比

### 1、存储文件的压缩比测试

##### 1.1 测试数据 

~~~
https://github.com/liufengji/Compression_Format_Data

log.txt 大小为18.1 M
~~~

##### 1.2 TextFile

* 创建表，存储数据格式为**TextFile**

~~~sql
create table log_text (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string
)
row format delimited fields terminated by '\t'
stored as textfile ;
~~~

* 向表中加载数据

~~~sql
load data local inpath '/home/hadoop/log.txt' into table log_text ;
~~~

* 查看表的数据量大小

~~~shell
dfs -du -h /user/hive/warehouse/log_text;

+------------------------------------------------+--+
|                   DFS Output                   |
+------------------------------------------------+--+
| 18.1 M  /user/hive/warehouse/log_text/log.txt  |
+------------------------------------------------+--+
~~~



##### 1.3 Parquet

* 创建表，存储数据格式为 **parquet**

~~~sql
create table log_parquet  (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string
)
row format delimited fields terminated by '\t'
stored as parquet;
~~~

* 向表中加载数据

~~~sql
insert into table log_parquet select * from log_text;
~~~

* 查看表的数据量大小

~~~shell
dfs -du -h /user/hive/warehouse/log_parquet;

+----------------------------------------------------+--+
|                     DFS Output                     |
+----------------------------------------------------+--+
| 13.1 M  /user/hive/warehouse/log_parquet/000000_0  |
+----------------------------------------------------+--+
~~~



##### 1.4  ORC

- 创建表，存储数据格式为ORC

```sql
create table log_orc  (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string
)
row format delimited fields terminated by '\t'
stored as orc  ;
```

- 向表中加载数据

```sql
insert into table log_orc select * from log_text ;
```

- 查看表的数据量大小

```shell
dfs -du -h /user/hive/warehouse/log_orc;
+-----------------------------------------------+--+
|                  DFS Output                   |
+-----------------------------------------------+--+
| 2.8 M  /user/hive/warehouse/log_orc/000000_0  |
+-----------------------------------------------+--+
```

##### 1.5 存储文件的压缩比总结

~~~
ORC >  Parquet >  textFile
~~~



### 2、存储文件的查询速度测试

##### 2.1  TextFile

~~~sql
select count(*) from log_text;
+---------+--+
|   _c0   |
+---------+--+
| 100000  |
+---------+--+
1 row selected (16.99 seconds)
~~~



##### 2.2 Parquet

~~~sql
select count(*) from log_parquet;
+---------+--+
|   _c0   |
+---------+--+
| 100000  |
+---------+--+
1 row selected (17.994 seconds)
~~~



##### 2.3 ORC

~~~sql
select count(*) from log_orc;
+---------+--+
|   _c0   |
+---------+--+
| 100000  |
+---------+--+
1 row selected (15.943 seconds)
~~~

##### 2.4 存储文件的查询速度总结

~~~
ORC > TextFile > Parquet
~~~



### 3、存储和压缩结合

* 使用压缩的优势是可以最小化所需要的磁盘存储空间，以及减少磁盘和网络io操作

* 官网地址
  * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC

* ORC支持三种压缩：ZLIB,SNAPPY,NONE。最后一种就是不压缩，==orc默认采用的是ZLIB压缩==。



##### 3.1  创建一个非压缩的的ORC存储方式表

* 1、创建一个非压缩的的ORC表

~~~
create table log_orc_none (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string
)
row format delimited fields terminated by '\t'
stored as orc tblproperties("orc.compress"="NONE") ;
~~~

* 2、加载数据

~~~sql
insert into table log_orc_none select * from log_text ;
~~~

* 3、查看表的数据量大小

~~~shell
dfs -du -h /user/hive/warehouse/log_orc_none;
+----------------------------------------------------+--+
|                     DFS Output                     |
+----------------------------------------------------+--+
| 7.7 M  /user/hive/warehouse/log_orc_none/000000_0  |
+----------------------------------------------------+--+
~~~



##### 3.2  创建一个snappy压缩的ORC存储方式表

* 1、创建一个snappy压缩的的ORC表

~~~sql
create table log_orc_snappy (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string
)
row format delimited fields terminated by '\t'
stored as orc tblproperties("orc.compress"="SNAPPY") ;
~~~

* 2、加载数据

~~~sql
insert into table log_orc_snappy select * from log_text ;
~~~

* 3、查看表的数据量大小

~~~shell
dfs -du -h /user/hive/warehouse/log_orc_snappy;
+------------------------------------------------------+--+
|                      DFS Output                      |
+------------------------------------------------------+--+
| 3.8 M  /user/hive/warehouse/log_orc_snappy/000000_0  |
+------------------------------------------------------+--+
~~~



##### 3.3  创建一个ZLIB压缩的ORC存储方式表

* 不指定压缩格式的就是默认的采用ZLIB压缩
  * 可以参考上面创建的 log_orc 表
* 查看表的数据量大小

~~~shell
dfs -du -h /user/hive/warehouse/log_orc;
+-----------------------------------------------+--+
|                  DFS Output                   |
+-----------------------------------------------+--+
| 2.8 M  /user/hive/warehouse/log_orc/000000_0  |
+-----------------------------------------------+--+
~~~

##### 3.4 存储方式和压缩总结

* orc 默认的压缩方式ZLIB比Snappy压缩的还小。

* 在实际的项目开发当中，hive表的数据存储格式一般选择：orc或parquet。

* 由于snappy的压缩和解压缩 效率都比较高，==压缩方式一般选择snappy==






