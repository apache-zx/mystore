package cn.unipus.muses.feed.Util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseUtil {

    private static Connection connection = null;
    static PropertiesUtil prop;

    static {
        //使用HBaseConfiguration的单例方法实例化
        Configuration conf = HBaseConfiguration.create();
        String zkQuorum = prop.getValue("zookeeper.quorum");
        String port = prop.getValue("zookeeper.port");
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", port);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据rowkey 查询
     *
     * @param rowkey
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Map<String, Object> getRowKeyToData(String rowkey, String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        Map<String, Object> map = new HashMap<>();
        if (result.rawCells().length != 0) {
            for (Cell cell : result.rawCells()) {
                map.put("uid", Bytes.toString(result.getRow()));
                map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return map;
    }

    public static void main(String[] args) throws Exception{
        Map<String, Object> userRecommend = getRowKeyToData("19999", "userRecommend");
        String uid = userRecommend.get("uid").toString();
        String recommendList = userRecommend.get("recommendList").toString();
        System.out.println(uid+" = "+recommendList);
        System.out.println("=============================================");
        Map<String, Object> newgoods = getRowKeyToData("124", "new_goods_recently");
        String grade = newgoods.get("uid").toString();
        String new_goods = newgoods.get("new_goods").toString();
        System.out.println(grade+" = "+new_goods);

    }
}
