# hive的综合案例实战

## 1、需求描述

统计youtube影音视频网站的常规指标，各种TopN指标：

--统计视频观看数Top10

--统计视频类别热度Top10

--统计视频观看数Top20所属类别

--统计视频观看数Top50所关联视频的所属类别Rank

--统计每个类别中的视频热度Top10

--统计每个类别中视频流量Top10

--统计上传视频最多的用户Top10以及他们上传的视频

--统计每个类别视频观看数Top10

## 2、项目表字段

### 1、数据结构

1．视频表

| 字段          | 备注       | 详细描述               |
| ------------- | ---------- | ---------------------- |
| video id      | 视频唯一id | 11位字符串             |
| uploader      | 视频上传者 | 上传视频的用户名String |
| age           | 视频年龄   | 视频在平台上的整数天   |
| category      | 视频类别   | 上传视频指定的视频分类 |
| length        | 视频长度   | 整形数字标识的视频长度 |
| views         | 观看次数   | 视频被浏览的次数       |
| rate          | 视频评分   | 满分5分                |
| ratings       | 流量       | 视频的流量，整型数字   |
| conments      | 评论数     | 一个视频的整数评论数   |
| related   ids | 相关视频id | 相关视频的id，最多20个 |



2．用户表

| 字段     | 备注         | 字段类型 |
| -------- | ------------ | -------- |
| uploader | 上传者用户名 | string   |
| videos   | 上传视频数   | int      |
| friends  | 朋友数量     | int      |

## 3、ETL原始数据清洗

- 通过观察原始数据形式，可以发现，视频可以有多个所属分类category，每个所属分类用&符号分割，且分割的两边有空格字符
- 同时相关视频也是可以有多个元素，多个相关视频又用“\t”进行分割。
- 为了分析数据时方便对存在多个子元素的数据进行操作，我们首先进行数据重组清洗操作。即：将所有的类别用“&”分割，同时去掉两边空格，多个相关视频id也使用“&”进行分割。

- 三件事情
  - 长度不够9的删掉
  - 视频类别删掉空格
  - 相关视频的分割符用&

- 创建maven工程，并导入jar包

```
	<repositories>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
    </repositories>
    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.6.0-mr1-cdh5.14.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.6.0-cdh5.14.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.6.0-cdh5.14.2</version>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-core</artifactId>
            <version>2.6.0-cdh5.14.2</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/junit/junit -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.11</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testng</groupId>
            <artifactId>testng</artifactId>
            <version>RELEASE</version>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.38</version>
            <scope>compile</scope>
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

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>2.4.3</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <minimizeJar>true</minimizeJar>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            <!--  <plugin>
                  <artifactId>maven-assembly-plugin </artifactId>
                  <configuration>
                      <descriptorRefs>
                          <descriptorRef>jar-with-dependencies</descriptorRef>
                      </descriptorRefs>
                      <archive>
                          <manifest>
                              <mainClass></mainClass>
                          </manifest>
                      </archive>
                  </configuration>
                  <executions>
                      <execution>
                          <id>make-assembly</id>
                          <phase>package</phase>
                          <goals>
                              <goal>single</goal>
                          </goals>
                      </execution>
                  </executions>
              </plugin>-->
        </plugins>
    </build>
```

1、代码开发：ETLUtil

```
public class VideoUtil {
    /**
     * 对我们的数据进行清洗的工作，
     * 数据切割，如果长度小于9 直接丢掉
     * 视频类别中间空格 去掉
     * 关联视频，使用 &  进行分割
     * @param line
     * @return
     * FM1KUDE3C3k  renetto	736	News & Politics	1063	9062	4.57	525	488	LnMvSxl0o0A&IKMtzNuKQso&Bq8ubu7WHkY&Su0VTfwia1w&0SNRfquDfZs&C72NVoPsRGw
     */
    public  static String washDatas(String line){
        if(null == line || "".equals(line)) {
            return null;
        }
        //判断数据的长度，如果小于9，直接丢掉
        String[] split = line.split("\t");
        if(split.length <9){
            return null;
        }
        //将视频类别空格进行去掉
        split[3] =  split[3].replace(" ","");
        StringBuilder builder = new StringBuilder();
        for(int i =0;i<split.length;i++){
            if(i <9){
                //这里面是前面八个字段
                builder.append(split[i]).append("\t");
            }else if(i >=9  && i < split.length -1){
                builder.append(split[i]).append("&");
            }else if( i == split.length -1){
                builder.append(split[i]);
            }
        }
        return  builder.toString();
    }
}
```



2、代码开发：ETLMapper

```
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class VideoMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    private Text  key2 ;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        key2 = new Text();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = VideoUtils.washDatas(value.toString());
        if(null != s ){
            key2.set(s);
            context.write(key2,NullWritable.get());
        }
    }
}
```



3、代码开发：ETLRunner

```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VideoMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "washDatas");
        job.setJarByClass(VideoMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(args[0]));

        job.setMapperClass(VideoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(args[1]));
        //注意，我们这里没有自定义reducer，会使用默认的一个reducer类
        job.setNumReduceTasks(7);
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }
    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new VideoMain(), args);
        System.exit(run);
    }
}
```

## 4、项目建表并加载数据

### 1、创建表

创建表：youtubevideo_ori，youtubevideo_user_ori，

创建表：youtubevideo_orc，youtubevideo_user_orc

youtubevideo_ori：

开启分桶表功能

```sql
set hive.enforce.bucketing=true;
set mapreduce.job.reduces=-1;

create database youtube;
use youtube;
create table youtubevideo_ori(
    videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>)
row format delimited 
fields terminated by "\t"
collection items terminated by "&"
stored as textfile;
```

youtubevideo_user_ori：

```sql
create table youtubevideo_user_ori(
    uploader string,
    videos int,
    friends int)
clustered by (uploader) into 24 buckets
row format delimited 
fields terminated by "\t" 
stored as textfile;
```

然后把原始数据插入到orc表中

youtubevideo_orc：

```sql
create table youtubevideo_orc(
    videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>)
clustered by (uploader) into 8 buckets 
row format delimited fields terminated by "\t" 
collection items terminated by "&" 
stored as orc;
```

youtubevideo_user_orc：

```sql
create table youtubevideo_user_orc(
    uploader string,
    videos int,
    friends int)
clustered by (uploader) into 24 buckets 
row format delimited 
fields terminated by "\t" 
stored as orc;
```

### 2、导入ETL之后的数据

youtubevideo_ori：

```
load data inpath "/youtubevideo/output/video/2008/0222" into table youtubevideo_ori;
```

youtubevideo_user_ori：

```
load data inpath "/youtubevideo/user/2008/0903" into table youtubevideo_user_ori;
```

### 3、向ORC表插入数据

youtubevideo_orc：

```
insert overwrite table youtubevideo_orc select * from youtubevideo_ori;
```

youtubevideo_user_orc：

```
insert into table youtubevideo_user_orc select * from youtubevideo_user_ori;
```

## 5、业务分析

### 1、统计视频观看数Top10

思路：使用order by按照views字段做一个全局排序即可，同时我们设置只显示前10条。

最终代码：

```sql
select 
    videoId, 
    uploader, 
    age, 
    category, 
    length, 
    views, 
    rate, 
    ratings, 
    comments 
from 
    youtubevideo_orc 
order by 
    views 
desc limit 
    10;
```



### 2、统计视频类别热度Top10

思路：

1) 即统计每个类别有多少个视频，显示出包含视频最多的前10个类别。

2) 我们需要按照类别group by聚合，然后count组内的videoId个数即可。

3) 因为当前表结构为：一个视频对应一个或多个类别。所以如果要group by类别，需要先将类别进行列转行(展开)，然后再进行count即可。

4) 最后按照热度排序，显示前10条。

最终代码：

```sql
select 
    category_name as category, 
    count(t1.videoId) as hot 
from (
    select 
        videoId,
        category_name 
    from 
        youtubevideo_orc lateral view explode(category) t_catetory as category_name) t1 
group by 
    t1.category_name 
order by 
    hot 
desc limit 
    10;
```

### 3、统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数

思路：

1) 先找到观看数最高的20个视频所属条目的所有信息，降序排列

2) 把这20条信息中的category分裂出来(列转行)

3) 最后查询视频分类名称和该分类下有多少个Top20的视频

最终代码：

```sql
select 
    category_name as category, 
    count(t2.videoId) as hot_with_views 
from (
    select 
        videoId, 
        category_name 
    from (
        select 
            * 
        from 
            youtubevideo_orc 
        order by 
            views 
        desc limit 
            20) t1 lateral view explode(category) t_catetory as category_name) t2 
group by 
    category_name 
order by 
    hot_with_views 
desc;
```

### 4、 统计视频观看数Top50所关联视频的所属类别Rank

思路：

1)       查询出观看数最多的前50个视频的所有信息(当然包含了每个视频对应的关联视频)，记为临时表t1

t1：观看数前50的视频

```sql
select 
    * 
from 
    youtubevideo_orc 
order by 
    views 
desc limit 
    50;
```

2)       将找到的50条视频信息的相关视频relatedId列转行，记为临时表t2

t2：将相关视频的id进行列转行操作

```sql
select 
    explode(relatedId) as videoId 
from 
	t1;
```

3)       将相关视频的id和youtubevideo_orc表进行inner join操作，记为临时表t4

t4：获得每个视频id、对应类别的结果

```sql
select 
    distinct(t2.videoId), 
    t3.category 
from 
    t2
inner join 
    youtubevideo_orc t3 on t2.videoId = t3.videoId
```

4）由于t4表的category是array<String>，若要获得t4表视频id与它的每个类别的笛卡尔乘积，使用lateral view explode将category炸开，记为临时表t5

t5：得到两列数据，一列是视频id，一列是视频对应的单个类别

```sql
select 
    videoId, 
    category_name 
from
	t4 
	lateral view explode(category) t_catetory as category_name;
```

4) 按照视频类别进行分组，统计每组视频个数，然后排行

```sql
select 
    category_name as category, 
    count(t5.videoId) as hot 
from 
	t5
group by 
    category_name 
order by 
    hot 
desc;
```

将上边语句中t5、t4、t2、t1分别替换原语句，最终hql代码为：

```sql
select 
    category_name as category, 
    count(t5.videoId) as hot 
from (
    select 
        videoId, 
        category_name 
    from (
        select 
            distinct(t2.videoId), 
            t3.category 
        from (
            select 
                explode(relatedId) as videoId 
            from (
                select 
                    * 
                from 
                    youtubevideo_orc 
                order by 
                    views 
                desc limit 
                    50) t1) t2 
        inner join 
            youtubevideo_orc t3 on t2.videoId = t3.videoId) t4 lateral view explode(category) t_catetory as category_name) t5
group by 
    category_name 
order by 
    hot 
desc;
```



### 5、统计每个类别中的视频热度Top10，以Music为例

思路：

1) 要想统计Music类别中的视频热度Top10，需要先找到Music类别，那么就需要将category展开，所以可以创建一张表用于存放categoryId展开的数据。

2) 向category展开的表中插入数据。

3) 统计对应类别（Music）中的视频热度。

最终代码：

创建表类别表：

```sql
create table youtubevideo_category(
    videoId string, 
    uploader string, 
    age int, 
    categoryId string, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int, 
    relatedId array<string>)
row format delimited 
fields terminated by "\t" 
collection items terminated by "&" 
stored as orc;
```

向类别表中插入数据：

```
insert into table youtubevideo_category  
    select 
        videoId,
        uploader,
        age,
        categoryId,
        length,
        views,
        rate,
        ratings,
        comments,
        relatedId 
    from 
        youtubevideo_orc lateral view explode(category) catetory as categoryId;

```

统计Music类别的Top10（也可以统计其他）

```
select 
    videoId, 
    views
from 
    youtubevideo_category 
where 
    categoryId = "Music" 
order by 
    views 
desc limit
    10;

```



### 6、 统计每个类别中视频流量Top10，以Music为例

思路：

1) 创建视频类别展开表（categoryId列转行后的表）

2) 按照ratings排序即可

最终代码：

```
select videoid,views,ratings 
from youtubevideo_category 
where categoryid = "Music" order by ratings desc limit 10;

```

### 7、 统计上传视频最多的用户Top10以及他们上传的观看次数在前20的视频

思路：

1) 先找到上传视频最多的10个用户的用户信息

```sql
select 
    * 
from 
    youtubevideo_user_orc 
order by 
    videos 
desc limit 
    10;
```

2) 通过uploader字段与youtubevideo_orc表进行join，得到的信息按照views观看次数进行排序即可。

最终代码：

```sql
select 
    t2.videoId, 
    t2.views,
    t2.ratings,
    t1.videos,
    t1.friends 
from (
    select 
        * 
    from 
        youtubevideo_user_orc 
    order by 
        videos desc 
    limit 
        10) t1 
join 
    youtubevideo_orc t2
on 
    t1.uploader = t2.uploader 
order by 
    views desc 
limit 
    20;
```

### 8、统计每个类别视频观看数Top10

思路：

1) 先得到categoryId展开的表数据

2) 子查询按照categoryId进行分区，然后分区内排序，并生成递增数字，该递增数字这一列起名为rank列

3) 通过子查询产生的临时表，查询rank值小于等于10的数据行即可。

最终代码：

```sql
select 
    t1.* 
from (
    select 
        videoId,
        categoryId,
        views,
        row_number() over(partition by categoryId order by views desc) rank from youtubevideo_category) t1 
where 
    rank <= 10;
```

