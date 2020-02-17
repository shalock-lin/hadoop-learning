# 大数据分析利器之Hive

## 一、课前准备

1. 安装hive环境
2. 掌握hive sql常见的DDL和DML操作


## 二、课堂主题

本堂课主要围绕hive的高级操作进行讲解。主要包括以下几个方面

4. hive的SerDe介绍和使用

## 三、课堂目标

4. 掌握hive的SerDe




## 四、知识要点

### 3. hive的SerDe

#### 1 hive的SerDe是什么

​	Serde是 ==Serializer/Deserializer==的简写。hive使用Serde进行行对象的序列与反序列化。最后实现把文件内容映射到 hive 表中的字段数据类型。

​	为了更好的阐述使用 SerDe 的场景，我们需要了解一下 Hive 是如何读数据的(类似于 HDFS 中数据的读写操作)：

```
HDFS files –> InputFileFormat –> <key, value> –> Deserializer –> Row object

Row object –> Serializer –> <key, value> –> OutputFileFormat –> HDFS files
```



#### 2 hive的SerDe 类型

- Hive 中内置==org.apache.hadoop.hive.serde2== 库，内部封装了很多不同的SerDe类型。
- hive创建表时， 通过自定义的SerDe或使用Hive内置的SerDe类型指定数据的序列化和反序列化方式。

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name 
[(col_name data_type [COMMENT col_comment], ...)] [COMMENT table_comment] [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)] 
[CLUSTERED BY (col_name, col_name, ...) 
[SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS] 
[ROW FORMAT row_format] 
[STORED AS file_format] 
[LOCATION hdfs_path]
```

- 如上创建表语句， 使用==row format 参数说明SerDe的类型。==

- 你可以创建表时使用用户**自定义的Serde或者native Serde**， **如果 ROW FORMAT没有指定或者指定了 ROW FORMAT DELIMITED就会使用native Serde**。
- [Hive SerDes](https://cwiki.apache.org/confluence/display/Hive/SerDe): 
  - Avro (Hive 0.9.1 and later) 
  - ORC (Hive 0.11 and later) 
  - RegEx 
  - Thrift 
  - Parquet (Hive 0.13 and later) 
  - CSV (Hive 0.14 and later) 
  - MultiDelimitSerDe 

## 五、拓展点、未来计划、行业趋势 

## 六、总结

