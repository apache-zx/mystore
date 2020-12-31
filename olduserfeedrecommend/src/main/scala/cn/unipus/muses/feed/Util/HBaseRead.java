package cn.unipus.muses.feed.Util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class HBaseRead {
    private static Configuration conf;
    private static Connection connection;
    private static Admin admin;

    static {

        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
            conf.set("hbase.zookeeper.property.clinetPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //判断表是否存在
    public static boolean isTableExists(String tableName) throws Exception {

        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        return exists;
    }

    //取出hbase表中的数据
    public static void getRow(String tableName, String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        String recommendList = "";
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ", Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            recommendList = Bytes.toString(CellUtil.cloneValue(cell));
        }
        List<String> list = Arrays.asList(recommendList.split(","));
        List<String> top10 = list.subList(0, 10);
        System.out.println(top10);
        table.close();
    }

    public static void close() {

    }

    public static void main(String[] args) throws Exception {
        if(isTableExists("userRecommend")) {
            getRow("userRecommend", "101");
        }
    }

}
