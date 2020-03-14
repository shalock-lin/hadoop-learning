
==========================================================================================================
create table if not exists test_file_format(
empid int, 
deptid int,
sex string,
salary double
)row format delimited 
fields terminated by ' ';

--加载数据
load data local inpath '/home/hadoop/test.txt' into table test_file_format;

################################################   textFile ###############################################
create table test_text(
empid int, 
deptid int,
sex string,
salary double
)row format delimited 
fields terminated by '\t' 
stored as textfile;

insert overwrite table test_text select * from test_file_format;  



################################################   sequencefile ###############################################
create table test_sequencefile(
empid int, 
deptid int,
sex string,
salary double
)row format delimited 
fields terminated by '\t' 
stored as sequencefile;

insert overwrite table test_sequencefile select * from test_file_format;  


################################################   orc ###############################################
create table test_orc(
empid int, 
deptid int,
sex string,
salary double
)row format delimited 
fields terminated by '\t' 
stored as orc;

insert overwrite table test_orc select * from test_file_format;  


################################################   orc ###############################################
create table test_parquet(
empid int, 
deptid int,
sex string,
salary double
)row format delimited 
fields terminated by '\t' 
stored as parquet;

insert overwrite table test_parquet select * from test_file_format;  




textfile 存储空间消耗比较大，并且压缩的text 无法分割和合并 查询的效率最低,可以直接存储，加载数据的速度最高
sequencefile 存储空间消耗最大,压缩的文件可以分割和合并 查询效率高，需要通过text文件转化来加载
rcfile 存储空间最小，查询的效率最高 ，需要通过text文件转化来加载，加载的速度最低
orc  效率比rcfile高，是rcfile的改良版本