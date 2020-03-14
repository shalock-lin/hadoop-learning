package com.kkb.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseVersionAndTTL {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);

        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("versionTable");
        if(!admin.tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            //增加列族
            HColumnDescriptor f1 = new HColumnDescriptor("f1");
            //设置版本的确界、ttl
            f1.setMinVersions(3);//
            f1.setMaxVersions(5);//
            f1.setTimeToLive(30);//

            hTableDescriptor.addFamily(f1);

            admin.createTable(hTableDescriptor);
        }

        //插入数据
        Table table = connection.getTable(tableName);
//        for(int i = 0; i < 6; i++) {
//            Put put = new Put("001".getBytes());
//            put.addColumn("f1".getBytes(), "name".getBytes(), ("zhangsan"+i).getBytes());
//            table.put(put);
//        }

        //查询
        Get get = new Get("001".getBytes());
        //获得所有的版本
        get.setMaxVersions();//

        Result result = table.get(get);

//        List<Cell> cells = result.listCells();
        Cell[] cells = result.rawCells();//

        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //关闭连接
        table.close();
        admin.close();
        connection.close();
    }
}
