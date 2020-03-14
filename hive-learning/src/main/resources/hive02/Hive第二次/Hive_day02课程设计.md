# 大数据分析利器之Hive

## 一、课前准备

1. 3节点Hadoop集群

2. 已安装Hive

## 二、课堂主题

本堂课主要围绕hive的DDL操作和DML操作进行讲解。主要包括以下几个方面

1. 对Hive表的DDL和DML操作

2. Hive表数据导入和数据导出方式

## 三、课堂目标

1. 掌握Hive数据导入

2. 掌握Hive数据导出

4. 掌握Hive的静态分区和动态分区

5. 理解Hive中的分桶表作用

## 四、知识要点

### 1. Hive的分桶表

![](assets/2019-07-16_17-01-51.png)

#### 1.1 分桶表原理

- 分桶是相对分区进行更细粒度的划分

    - Hive表或分区表可进一步的分桶

    - ==分桶将整个数据内容按照某列取hash值，对桶的个数取模的方式决定该条记录存放在哪个桶当中；具有相同hash值的数据进入到同一个文件中==

    - 比如按照name属性分为3个桶，就是对name属性值的hash值对3取摸，按照取模结果对数据分桶。
      - 取模结果为==0==的数据记录存放到一个文件
      
      - 取模结果为==1==的数据记录存放到一个文件
      
      - 取模结果为==2==的数据记录存放到一个文件
      
#### 1.2 作用

- 1、取样sampling更高效。没有分区的话需要扫描整个数据集。

- 2、提升某些查询操作效率，例如map side join

#### 1.3 案例演示：创建分桶表

- 在创建分桶表之前要执行的命令
- ==set hive.enforce.bucketing=true;==  开启对分桶表的支持
- ==set mapreduce.job.reduces=4;==      设置与桶相同的reduce个数（默认只有一个reduce）

- 进入hive客户端然后执行以下命令

```sql
use myhive;
set hive.enforce.bucketing=true; 
set mapreduce.job.reduces=4;  

-- 创建分桶表
create table myhive.user_buckets_demo(id int, name string)
clustered by(id) 
into 4 buckets 
row format delimited fields terminated by '\t';

-- 创建普通表
create table user_demo(id int, name string)
row format delimited fields terminated by '\t';
```

- 准备数据文件 buckets.txt

```
#在linux当中执行以下命令
cd /kkb/install/hivedatas/
vim user_bucket.txt

1	anzhulababy1
2	anzhulababy2
3	anzhulababy3
4	anzhulababy4
5	anzhulababy5
6	anzhulababy6
7	anzhulababy7
8	anzhulababy8
9	anzhulababy9
10	anzhulababy10
```

- 加载数据到普通表 user_demo 中

```sql
load data local inpath '/kkb/install/hivedatas/user_bucket.txt'  overwrite into table user_demo; 
```
  - 4、加载数据到桶表user_buckets_demo中

```sql
insert into table user_buckets_demo select * from user_demo;
```

- hdfs上查看表的数据目录

![](assets/2019-07-16_16-30-09.png)

- 抽样查询桶表的数据
  - tablesample抽样语句语法：tablesample(bucket  x  out  of  y)
    - x表示从第几个桶开始取数据
    - y与进行采样的桶数的个数、每个采样桶的采样比例有关；

  ```sql
select * from user_buckets_demo tablesample(bucket 1 out of 2);
  -- 需要采样的总桶数=4/2=2个
  -- 先从第1个桶中取出数据
  -- 1+2=3，再从第3个桶中取出数据
  ```

### 2. Hive数据导入

#### 2.1 直接向表中插入数据（强烈不推荐使用）

  ```sql
hive (myhive)> create table score3 like score;
hive (myhive)> insert into table score3 partition(month ='201807') values ('001','002','100');
  ```

#### 2.2 通过load加载数据（必须掌握）

- 语法：

```sql
 hive> load data [local] inpath 'dataPath' overwrite | into table student [partition (partcol1=val1,…)]; 
```

- 通过load方式加载数据

```sql
hive (myhive)> load data local inpath '/kkb/install/hivedatas/score.csv' overwrite into table score3 partition(month='201806');
```

#### 2.3 通过查询加载数据（必须掌握）

- 通过查询方式加载数据

```sql
hive (myhive)> create table score5 like score;
hive (myhive)> insert overwrite table score5 partition(month = '201806') select s_id,c_id,s_score from score;
```

#### 2.4 查询语句中创建表并加载数据（as select）

- 将查询的结果保存到一张表当中去

```sql
hive (myhive)> create table score6 as select * from score;
```

#### 2.5 创建表时指定location

- 创建表，并指定在hdfs上的位置

```sql
hive (myhive)> create external table score7 (s_id string,c_id string,s_score int) row format delimited fields terminated by '\t' location '/myscore7';
```

- 上传数据到hdfs上，我们也可以直接在hive客户端下面通过dfs命令来进行操作hdfs的数据

```sql
hive (myhive)> dfs -mkdir -p /myscore7;
hive (myhive)> dfs -put /kkb/install/hivedatas/score.csv /myscore7;
```

- 查询数据

```sql
hive (myhive)> select * from score7;
```

#### 2.6 export导出与import 导入 hive表数据（内部表操作）

```sql
hive (myhive)> create table teacher2 like teacher;
hive (myhive)> export table teacher to  '/kkb/teacher';
hive (myhive)> import table teacher2 from '/kkb/teacher';
```



### 3. Hive数据导出

#### 3.1 insert 导出

- 将查询的结果导出到本地

```sql
insert overwrite local directory '/kkb/install/hivedatas/stu' select * from stu;
```

- 将查询的结果**格式化**导出到本地

```sql
insert overwrite local directory '/kkb/install/hivedatas/stu2' row format delimited fields terminated by ',' select * from stu;
```

- 将查询的结果导出到HDFS上==(没有local)==

```sql
insert overwrite directory '/kkb/hivedatas/stu' row format delimited fields terminated by  ','  select * from stu;
```

#### 3.2  Hive Shell 命令导出

- 基本语法：

  - hive -e "sql语句" >   file
  - hive -f  sql文件   >    file

```shell
hive -e 'select * from myhive.stu;' > /kkb/install/hivedatas/student1.txt
```

#### 3.3 export导出到HDFS上

```sql
export table  myhive.stu to '/kkb/install/hivedatas/stuexport';
```



### 4. Hive的静态分区和动态分区

#### 4.1 静态分区

- 表的分区字段的值需要开发人员手动给定

- 创建分区表

```sql
use myhive;
create table order_partition(
order_number string,
order_price  double,
order_time string
)
partitioned BY(month string)
row format delimited fields terminated by '\t';
```


- 准备数据
  

```shell
cd /kkb/install/hivedatas
vim order.txt 

10001	100	2019-03-02
10002	200	2019-03-02
10003	300	2019-03-02
10004	400	2019-03-03
10005	500	2019-03-03
10006	600	2019-03-03
10007	700	2019-03-04
10008	800	2019-03-04
10009	900	2019-03-04
```

- 加载数据到分区表

```sql
load data local inpath '/kkb/install/hivedatas/order.txt' overwrite into table order_partition partition(month='2019-03');
```

  - 4、查询结果数据	

```sql
select * from order_partition where month='2019-03';
结果为：
  
10001   100.0   2019-03-02      2019-03
10002   200.0   2019-03-02      2019-03
10003   300.0   2019-03-02      2019-03
10004   400.0   2019-03-03      2019-03
10005   500.0   2019-03-03      2019-03
10006   600.0   2019-03-03      2019-03
10007   700.0   2019-03-04      2019-03
10008   800.0   2019-03-04      2019-03
10009   900.0   2019-03-04      2019-03
```


#### 4.2 动态分区

- 按照需求实现把数据自动导入到表的不同分区中，==不需要手动指定==

- **需求：根据分区字段不同的值，自动将数据导入到分区表不同的分区中** 

- 创建表

```sql
--创建普通表
create table t_order(
    order_number string,
    order_price  double, 
    order_time   string
)row format delimited fields terminated by '\t';

--创建目标分区表
create table order_dynamic_partition(
    order_number string,
    order_price  double    
)partitioned BY(order_time string)
row format delimited fields terminated by '\t';
```

- 准备数据

```shell
cd /kkb/install/hivedatas
vim order_partition.txt

10001	100	2019-03-02 
10002	200	2019-03-02
10003	300	2019-03-02
10004	400	2019-03-03
10005	500	2019-03-03
10006	600	2019-03-03
10007	700	2019-03-04
10008	800	2019-03-04
10009	900	2019-03-04
```

- 向普通表t_order加载数据

```sql
load data local inpath '/kkb/install/hivedatas/order_partition.txt' overwrite into table t_order;
```

- 动态加载数据到分区表中

```sql
-- 要想进行动态分区，需要设置参数
-- 开启动态分区功能
hive> set hive.exec.dynamic.partition=true; 
-- 设置hive为非严格模式
hive> set hive.exec.dynamic.partition.mode=nonstrict; 
hive> insert into table order_dynamic_partition partition(order_time) select order_number, order_price, order_time from t_order;
```

- 查看分区

```sql
hive> show partitions order_dynamic_partition;
```

![1569313506031](assets/1569313506031.png)




### 5. Hive的查询语法

#### 5.1 基本查询

- 注意
  - SQL 语言==大小写不敏感==
  - SQL 可以写在一行或者多行
  - ==关键字不能被缩写也不能分行==
  - 各子句一般要分行写
  - 使用缩进提高语句的可读性

##### 5.1.1 查询全表和特定列

- 全表查询

```sql
select * from stu;
```

- 选择特定列查询

```sql
select id,name from stu;
```

##### 5.1.2 列起别名

- 重命名一个列

  - 紧跟列名，也可以在列名和别名之间加入关键字 ‘as’ 

- 案例实操

```sql
select id,name as stuName from stu;
```

##### 5.1.3 常用函数

- 求总行数（count）

```sql
 select count(*) cnt from score;
```

- 求分数的最大值（max）

```sql
select max(s_score) from score;
```

- 求分数的最小值（min）

```sql
select min(s_score) from score;
```

- 求分数的总和（sum）

```sql
select sum(s_score) from score;
```

- 求分数的平均值（avg）

```sql
select avg(s_score) from score;
```

##### 5.1.4 limit 语句

- imit子句用于限制返回的行数。

```sql
 select  * from score limit 5;
```

##### 5.1.5 where 语句

- 使用 where 子句，将不满足条件的行过滤掉
- ==where 子句紧随from子句==
- 案例实操

```sql
select * from score where s_score > 60;
```

##### 5.1.6 算术运算符

| 运算符 | 描述           |
| ------ | -------------- |
| A+B    | A和B 相加      |
| A-B    | A减去B         |
| A*B    | A和B 相乘      |
| A/B    | A除以B         |
| A%B    | A对B取余       |
| A&B    | A和B按位取与   |
| A\|B   | A和B按位取或   |
| A^B    | A和B按位取异或 |
| ~A     | A按位取反      |

##### 5.1.7 比较运算符

|         操作符          | 支持的数据类型 |                             描述                             |
| :---------------------: | :------------: | :----------------------------------------------------------: |
|           A=B           |  基本数据类型  |             如果A等于B则返回true，反之返回false              |
|          A<=>B          |  基本数据类型  | 如果A和B都为NULL，则返回true，其他的和等号（=）操作符的结果一致，如果任一为NULL则结果为NULL |
|       A<>B, A!=B        |  基本数据类型  | A或者B为NULL则返回NULL；如果A不等于B，则返回true，反之返回false |
|           A<B           |  基本数据类型  | A或者B为NULL，则返回NULL；如果A小于B，则返回true，反之返回false |
|          A<=B           |  基本数据类型  | A或者B为NULL，则返回NULL；如果A小于等于B，则返回true，反之返回false |
|           A>B           |  基本数据类型  | A或者B为NULL，则返回NULL；如果A大于B，则返回true，反之返回false |
|          A>=B           |  基本数据类型  | A或者B为NULL，则返回NULL；如果A大于等于B，则返回true，反之返回false |
| A [NOT] BETWEEN B AND C |  基本数据类型  | 如果A，B或者C任一为NULL，则结果为NULL。如果A的值大于等于B而且小于或等于C，则结果为true，反之为false。如果使用NOT关键字则可达到相反的效果。 |
|        A IS NULL        |  所有数据类型  |           如果A等于NULL，则返回true，反之返回false           |
|      A IS NOT NULL      |  所有数据类型  |          如果A不等于NULL，则返回true，反之返回false          |
|    IN(数值1, 数值2)     |  所有数据类型  |                  使用 IN运算显示列表中的值                   |
|     A [NOT] LIKE B      |  STRING 类型   | B是一个SQL下的简单正则表达式，如果A与其匹配的话，则返回true；反之返回false。B的表达式说明如下：‘x%’表示A必须以字母‘x’开头，‘%x’表示A必须以字母’x’结尾，而‘%x%’表示A包含有字母’x’,可以位于开头，结尾或者字符串中间。如果使用NOT关键字则可达到相反的效果。like不是正则，而是通配符 |
|  A RLIKE B, A REGEXP B  |  STRING 类型   | B是一个正则表达式，如果A与其匹配，则返回true；反之返回false。匹配使用的是JDK中的正则表达式接口实现的，因为正则也依据其中的规则。例如，正则表达式必须和整个字符串A相匹配，而不是只需与其字符串匹配。 |

##### 5.1.8 逻辑运算符

|  操作符  |  操作  |                   描述                    |
| :------: | :----: | :---------------------------------------: |
| A AND  B | 逻辑并 |    如果A和B都是true则为true，否则false    |
| A  OR  B | 逻辑或 | 如果A或B或两者都是true则为true，否则false |
|  NOT  A  | 逻辑否 |      如果A为false则为true,否则false       |

#### 5.2 分组

##### 5.2.1 Group By 语句

- Group By语句通常会和==聚合函数==一起使用，按照一个或者多个列对结果进行分组，然后对每个组执行聚合操作。

- 案例实操：

  - 计算每个学生的平均分数

  ```sql
  select s_id, avg(s_score) from score group by s_id;
  ```

  - 计算每个学生最高的分数

  ```sql
  select s_id, max(s_score) from score group by s_id;
  ```

##### 5.2.2 Having语句

- having 与 where 不同点

  - where针对==表中的列发挥作用==，查询数据；==having针对查询结果中的列==发挥作用，筛选数据
  - where后面==不能写分组函数==，而having后面可以==使用分组函数==
  - having只用于group by分组统计语句

- 案例实操

  - 求每个学生的平均分数

  ```sql
  select s_id, avg(s_score) from score group by s_id;
  ```

  - 求每个学生平均分数大于60的人

  ```sql
  select s_id, avg(s_score) as avgScore from score group by s_id having avgScore > 60;
  ```

#### 5.3 join语句

##### 5.3.1 等值 join

- Hive支持通常的SQL JOIN语句，但是只支持等值连接，==不支持非等值连接==。

- 案例实操

  - 根据学生和成绩表，查询学生姓名对应的成绩

  ```sql
  select * from stu left join score on stu.id = score.s_id
  ```

##### 5.3.2 表的别名

- 好处

  - 使用别名可以简化查询。
  - 使用表名前缀可以提高执行效率。

- 案例实操：合并老师与课程表

  ```sql
-- hive当中创建course表并加载数据
  create table course (c_id string, c_name string, t_id string) 
  row format delimited fields terminated by '\t';
  
  load data local inpath '/kkb/install/hivedatas/course.csv' overwrite into table course;
  
  select * from teacher t join course c on t.t_id = c.t_id;
  ```

##### 5.3.3 内连接 inner join

- 内连接：只有进行连接的两个表中都存在与连接条件相匹配的数据才会被保留下来。
  
  - join默认是inner  join
  
- 案例实操

  ```sql
  select * from teacher t inner join course c  on t.t_id = c.t_id;
  ```

##### 5.3.4 左外连接 left outer join

- 左外连接：

  - join操作符==左边表中==符合where子句的所有记录将会被返回。
  - 右边表的指定字段没有符合条件的值的话，那么就使用null值替代。

- 案例实操：查询老师对应的课程

  ```sql
 select * from teacher t left outer join course c on t.t_id = c.t_id;
  ```

##### 5.3.5 右外连接 right outer join

- 右外连接：

  - join操作符==右边表中==符合where子句的所有记录将会被返回。
  - 左边表的指定字段没有符合条件的值的话，那么就使用null值替代。

- 案例实操

  ```sql
   select * from teacher t right outer join course c on t.t_id = c.t_id;
  ```

##### 5.3.6 满外连接 full outer join

- 满外连接：

  - 将会返回==所有表中==符合where语句条件的所有记录。
  - 如果任一表的指定字段没有符合条件的值的话，那么就使用null值替代。

- 案例实操

  ```sql
  select * from teacher t full outer join course c on t.t_id = c.t_id;
  ```

##### 5.3.7 多表连接 

- **多个表使用join进行连接**

- ==注意：==连接 n个表，至少需要n-1个连接条件。例如：连接三个表，至少需要两个连接条件。

- 案例实操

  - 多表连接查询，查询老师对应的课程，以及对应的分数，对应的学生

  ```sql
  select * from teacher t 
  left join course c on t.t_id = c.t_id 
  left join score s on c.c_id = s.c_id 
  left join stu on s.s_id = stu.id;
  ```

#### 5.4 排序

##### 5.4.1 order by 全局排序

- 全局排序，只有一个reduce
- 使用 ORDER BY 子句排序
  - asc ( ascend) 升序 (默认)
  - desc (descend) 降序
- order by 子句在select语句的结尾

- 案例实操

  - 查询学生的成绩，并按照分数降序排列

  ```sql
  select * from score s order by s_score desc ;
  ```

##### 5.4.2 按照别名排序

- 按照学生分数的平均值排序

  ```sql
  select s_id, avg(s_score) avgscore from score group by s_id order by avgscore desc; 
  ```

##### 5.4.3 每个MapReduce内部排序（Sort By）局部排序

- sort by：每个reducer内部进行排序，对全局结果集来说不是排序。

- 设置reduce个数

  ```
  set mapreduce.job.reduces=3;
  ```

- 查看reduce的个数

  ```sql
  set mapreduce.job.reduces;
  ```

- 查询成绩按照成绩降序排列

  ```sql
  select * from score s sort by s.s_score;
  ```

- 将查询结果导入到文件中（按照成绩降序排列）

  ```sql
  insert overwrite local directory '/kkb/install/hivedatas/sort' select * from score s sort by s.s_score;
  ```

##### 5.4.4 distribute by 分区排序

- distribute by：

  - 类似MR中partition，==采集hash算法，在map端将查询的结果中hash值相同的结果分发到对应的reduce文件中==。
  - 结合sort by使用。

- 注意

  - Hive要求 **distribute by** 语句要写在 **sort by** 语句之前。

- 案例实操

  - 先按照学生 sid 进行分区，再按照学生成绩进行排序

  - 设置reduce的个数

  ```sql
  set mapreduce.job.reduces=3;
  ```

    - 通过distribute by  进行数据的分区,，将不同的sid 划分到对应的reduce当中去

    ```sql
  insert overwrite local directory '/kkb/install/hivedatas/distribute' select * from score distribute by s_id sort by s_score;
    ```

##### 5.4.5 cluster by

- 当distribute by和sort by字段相同时，可以使用cluster by方式代替

- 除了distribute by 的功能外，还会对该字段进行排序，所以cluster by = distribute by + sort by

  ```sql
  --以下两种写法等价
  insert overwrite local directory '/kkb/install/hivedatas/distribute_sort' select * from score distribute by s_score sort by s_score;
  
  insert overwrite local directory '/kkb/install/hivedatas/cluster' select * from score  cluster by s_score;
  ```

## 五、拓展点、未来计划、行业趋势（10分钟）

- 平时工作中，要多查看[Hive官方文档](<https://cwiki.apache.org/confluence/display/Hive/Home>)
- Hive可视化工具dbeaver


## 六、总结



![](assets/hive表的DDL和DML操作.png)

## 七、作业

* 1、创建一张hive的表，加载数据，最后把查询的结果数据导入到本地文件中
* 2、创建一张hive外部分区表，实现从一个已知的表中查询数据，动态插入到指定的分区中

## 八、互动问答



## 九、题库 - 本堂课知识点

* 1、hive表的DDL操作
* 2、hive表数据的导入和导出
* 3、hive的静态分区和动态分区