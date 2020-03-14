package com.kkb.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 建立命名空间 weibo
 * 建表
 *          微博内容
 *          关系表
 *          收件箱表
 * 发送微博
 * 关注别人
 * 取消关注
 * 获得所关注人发送的微博
 */
public class HBaseWeibo {
    //微博内容表
    private static final byte[] WEIBO_CONTENT = "weibo:content".getBytes();

    //关系表
    private static final byte[] WEIBO_RELATION = "weibo:relation".getBytes();

    //收件箱表
    private static final byte[] WEIBO_RECEIVE_CONTENT_EMAIL = "weibo:receive_content_email".getBytes();

    public static void main(String[] args) throws IOException {
        //创建namespace
        HBaseWeibo hBaseWeibo = new HBaseWeibo();
//        hBaseWeibo.createNamespace();

        //建微博内容表
//        hBaseWeibo.createTableContent();

        //建关系表
//        hBaseWeibo.createTableRelation();

        //建收件箱表
//        hBaseWeibo.createTableReceiveContentEmails();

        //发送微博
//        hBaseWeibo.publishWeibo("3", "今天天气不错。。3。");
//        hBaseWeibo.publishWeibo("M", "今天天气不错。。M hhaa。");

        //关注别人
//        hBaseWeibo.addAttends("1", "2", "3", "M");

        //取消关注
//        hBaseWeibo.cancelAttends("1", "3");

        //获得所有被关注人发送的微博
        hBaseWeibo.getContent("1");
    }

    /**
     * uid用户在email中所有的值 rowkey
     * 通过rowkey去content表查询具体微博的内容
     *
     * @param uid
     */
    public void getContent(String uid) throws IOException {
        Connection connection = getConnection();
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        Get get = new Get(uid.getBytes());
        //email表，每个单元格有1000个版本，我们要获得每个单元格所有的版本数据
        get.setMaxVersions(5);

        Result result = weibo_email.get(get);
        Cell[] cells = result.rawCells();

        ArrayList<Get> gets = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] bytes = CellUtil.cloneValue(cell);
            //生成查询content表时的get
            Get get1 = new Get(bytes);
            gets.add(get1);
        }

        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        Result[] results = weibo_content.get(gets);

        for(Result result1: results) {
            byte[] content_bytes = result1.getValue("info".getBytes(), "content".getBytes());
            System.out.println(Bytes.toString(content_bytes));
        }

    }

    /**
     * 取消关注 A取消关注 B,C,D这三个用户
     * 其实逻辑与关注B,C,D相反即可
     * 第一步：在weibo:relation关系表当中，在attends列族当中删除B,C,D这三个列
     * 第二步：在weibo:relation关系表当中，在fans列族当中，以B,C,D为rowkey，查找fans列族当中A这个粉丝，给删除掉
     * 第三步：A取消关注B,C,D,在收件箱中，删除取关的人的微博的rowkey
     */
    public void cancelAttends(String uid, String... attends) throws IOException {
        //relation:删除关注的人
        Connection connection = getConnection();
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));

        Delete delete = new Delete(uid.getBytes());
        for(String cancelAttend: attends) {
            delete.addColumn("attends".getBytes(), cancelAttend.getBytes());
        }
        weibo_relation.delete(delete);

        //relation：删除attends的粉丝uid
        for(String cancelAttend: attends) {
            Delete delete1 = new Delete(cancelAttend.getBytes());
            delete1.addColumn("fans".getBytes(), uid.getBytes());
            weibo_relation.delete(delete1);
        }

        //email：删除uid中，attends相关的列
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        Delete delete1 = new Delete(uid.getBytes());
        for(String attend: attends) {
            delete1.addColumns("info".getBytes(), attend.getBytes());
        }
        weibo_email.delete(delete1);

        //释放资源
        weibo_relation.close();
        weibo_email.close();
        connection.close();

    }

    /**
     * A 关注B,C,D
     * 第一：A关注B,C,D 得relation中的rowkey = a，attends列下，添加三个列B,C，D，值也分别是B,C,D
     *
     * 第二：B，C，D多了粉丝A
     * 在relation中，fans列族增加A列，值为A
     *
     * 第三：获得B,C，D发送微博时的rowkey
     *
     * 第四：把上边的rowkey添加到email表 owkey=A  列分别是B,C,D，对应的值，分别是他们发送微博时的rowkey
     *
     * @param uid
     * @param attends
     */
    private void addAttends(String uid, String... attends) throws IOException {
        //第一：A关注B,C,D 得relation中的rowkey = a，attends列下，添加三个列B,C，D，值也分别是B,C,D
        Connection connection = getConnection();
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));

        Put put = new Put(uid.getBytes());
        for(String attend: attends) {
            //被关注人的id作为列名，cell的值
            put.addColumn("attends".getBytes(), attend.getBytes(), attend.getBytes());
        }
        weibo_relation.put(put);

        //第二：B，C，D多了粉丝A 在relation中，fans列族增加A列，值为A
        for(String attend: attends) {
            Put put1 = new Put(attend.getBytes());
            put1.addColumn("fans".getBytes(), uid.getBytes(), uid.getBytes());
            weibo_relation.put(put1);
        }

        //第三：获得B,C，D发送微博时的rowkey
        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        Scan scan = new Scan();

        ArrayList<byte[]> rowkeyBytes = new ArrayList<>();

        for(String attend: attends) {
            PrefixFilter prefixFilter = new PrefixFilter((attend + "_").getBytes());
            scan.setFilter(prefixFilter);

            ResultScanner scanner = weibo_content.getScanner(scan);

            if(null == scanner) {
                continue;
            }

            for (Result result : scanner) {
                //获得发送微博的rowkey
                byte[] row = result.getRow();
                rowkeyBytes.add(row);
            }

        }

        //第四：把上边的rowkey添加到email表 rowkey=A  列分别是B,C,D，对应的值，分别是他们发送微博时的rowkey
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        if(rowkeyBytes.size() > 0) {
            Put put1 = new Put(uid.getBytes());

            for(byte[] rowkeyContent: rowkeyBytes) {
                //rowkeyContent 格式：
                String rowkeyContentStr = Bytes.toString(rowkeyContent);
                String[] split = rowkeyContentStr.split("_");
                put1.addColumn("info".getBytes(), split[0].getBytes(), Long.parseLong(split[1]), rowkeyContent);
            }
            weibo_email.put(put1);
        }

        //释放资源
        weibo_content.close();
        weibo_relation.close();
        weibo_email.close();
        connection.close();
    }

    /**
     *  第一：将uid发送的微博内容存储起来
     *  content
     *
     *  第二：从relation表，获得他有哪些粉丝fan_uids
     *
     *  第三：fan_uids中，每个人fan_uid在收件箱表中，插入数据：uid发送微博的时候的rowkey
     *
     * @param uid 发送微博的人
     * @param content 微博内容
     */
    public void publishWeibo(String uid, String content) throws IOException {
        //第一：将uid发送的微博内容存储起来
        Connection connection = getConnection();
        Table weibo_content = connection.getTable(TableName.valueOf(WEIBO_CONTENT));

        //roweky uid_timestamp
        long timeStamp = System.currentTimeMillis();
        String rowkey = uid+ "_" + timeStamp;

        Put put = new Put(rowkey.getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), timeStamp, content.getBytes());
        weibo_content.put(put);

        //第二：从relation表，获得他有哪些粉丝fan_uids
        Table weibo_relation = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        Get get = new Get(uid.getBytes());
        get.addFamily("fans".getBytes());

        Result result = weibo_relation.get(get);
        if(result.isEmpty()) {
           weibo_content.close();
           weibo_relation.close();
           connection.close();
           return;
        }

        Cell[] cells = result.rawCells();
        ArrayList<byte[]> fan_uids = new ArrayList<>();
        for (Cell cell : cells) {
            //获得列名
            byte[] fan_uid = CellUtil.cloneQualifier(cell);
            fan_uids.add(fan_uid);
        }

        //第三：fan_uids中，每个人fan_uid在收件箱表中，插入数据：uid发送微博的时候的rowkey
        Table weibo_email = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        List<Put> putList = new ArrayList<>();
        for(byte[] fan_uid: fan_uids) {
            Put put1 = new Put(fan_uid);
            put1.addColumn("info".getBytes(), uid.getBytes(), timeStamp,rowkey.getBytes());
            putList.add(put1);
        }
        weibo_email.put(putList);

        //释放连接
        weibo_content.close();
        weibo_relation.close();
        weibo_email.close();
        connection.close();
    }

    /**
     * 表结构：
     *  方法名   createTableReceiveContentEmails
     *  Table Name    weibo:receive_content_email
     *  RowKey    用户ID
     *  ColumnFamily  info
     *  ColumnLabel   用户ID
     *  ColumnValue   取微博内容的RowKey
     *  Version   1000
     */
    public void createTableReceiveContentEmails() throws IOException {
        //获得连接
        Connection connection = getConnection();

        //admin
        Admin admin = connection.getAdmin();

        //创建
        if(!admin.tableExists(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL))) {
            HTableDescriptor weibo_receive_content_email = new HTableDescriptor(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));

            HColumnDescriptor info = new HColumnDescriptor("info");
            //指定最小版本、最大版本
            info.setMinVersions(1000);
            info.setMaxVersions(1000);
            info.setBlockCacheEnabled(true);

            weibo_receive_content_email.addFamily(info);

            admin.createTable(weibo_receive_content_email);
        }

        //关闭连接
        admin.close();
        connection.close();
    }

    /**
     * 创建用户关系表
     *  Table Name    weibo:relations
     *  RowKey    用户ID
     *  ColumnFamily  attends、fans
     *  ColumnLabel   关注用户ID，粉丝用户ID
     *  ColumnValue   用户ID
     *  Version   1个版本
     */
    public void createTableRelation() throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();

        if(!admin.tableExists(TableName.valueOf(WEIBO_RELATION))) {
            HTableDescriptor weibo_relation = new HTableDescriptor(TableName.valueOf(WEIBO_RELATION));
            //attends
            //minversion\max\...
            HColumnDescriptor attends = new HColumnDescriptor("attends");
            attends.setMinVersions(1);
            attends.setMaxVersions(1);
            attends.setBlockCacheEnabled(true);

            //fans
            //minversion\max\...
            HColumnDescriptor fans = new HColumnDescriptor("fans");
            fans.setMinVersions(1);
            fans.setMaxVersions(1);
            fans.setBlockCacheEnabled(true);

            weibo_relation.addFamily(attends);
            weibo_relation.addFamily(fans);
            admin.createTable(weibo_relation);

        }

        admin.close();
        connection.close();
    }

    /**
     * rowkey: uid_timestamp
     * 列族：info
     * 列名：pic, content, title
     * 版本：1个
     */
    public void createTableContent() throws IOException {
        Connection connection = getConnection();

        //获得管理员对象，创建表
        Admin admin = connection.getAdmin();

        if(!admin.tableExists(TableName.valueOf(WEIBO_CONTENT))) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(WEIBO_CONTENT));
            HColumnDescriptor info = new HColumnDescriptor("info");
            info.setMinVersions(1);
            info.setMaxVersions(1);
            info.setBlockCacheEnabled(true);

            hTableDescriptor.addFamily(info);

            admin.createTable(hTableDescriptor);
        }

        //关闭连接
        admin.close();
        connection.close();
    }

    public Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    public void createNamespace() throws IOException {
        Connection connection = getConnection();

        Admin admin = connection.getAdmin();
        NamespaceDescriptor build = NamespaceDescriptor.create("weibo").addConfiguration("creator", "bruce").build();
        admin.createNamespace(build);

        admin.close();
        connection.close();
    }
}
