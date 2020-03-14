# 大数据分析利器之Hive

### 1. Hive修改表结构 

#### 1.1 修改表的名称

- 修改表名称语法

```sql
alter table  old_table_name  rename to  new_table_name;
```

~~~sql
hive> alter table stu3 rename to stu4;
~~~

#### 1.2 增加/修改/替换列

- 查看表结构

```sql
hive> desc stu4;
hive> desc formatted stu4;
```

* [官网文档](<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-AlterColumn>)
* 增加列

~~~sql
hive> alter table stu4 add columns(address string);
~~~

* 修改列

~~~sql
hive> alter table stu4 change column address address_id int;
~~~



### 2. Hive客户端JDBC操作

#### 2.1 启动hiveserver2

- node03执行以下命令启动hiveserver2的服务端

```shell
cd /kkb/install/hive-1.1.0-cdh5.14.2/
nohup bin/hive --service hiveserver2 2>&1 &
```

#### 2.2 引入依赖

- 创建maven工程，引入依赖

```xml
   <repositories>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
    </repositories>
    <dependencies>
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-exec</artifactId>
            <version>1.1.0-cdh5.14.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-jdbc</artifactId>
            <version>1.1.0-cdh5.14.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-cli</artifactId>
            <version>1.1.0-cdh5.14.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.6.0-cdh5.14.2</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.0</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>UTF-8</encoding>
                    <!--    <verbal>true</verbal>-->
                </configuration>
            </plugin>
        </plugins>
    </build>
```

#### 2.3 代码开发

```java
import java.sql.*;

public class HiveJDBC {
    private static String url="jdbc:hive2://192.168.52.120:10000/myhive";
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        //获取数据库连接
        Connection connection = DriverManager.getConnection(url, "hadoop","");
        //定义查询的sql语句
        String sql="select * from stu";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                //获取id字段值
                int id = rs.getInt(1);
                //获取deptid字段
                String name = rs.getString(2);
                System.out.println(id+"\t"+name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```


