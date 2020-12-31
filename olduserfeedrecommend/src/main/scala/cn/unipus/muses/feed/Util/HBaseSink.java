package cn.unipus.muses.feed.Util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.List;

public class HBaseSink extends RichSinkFunction<Tuple2<String, List<String>>> {
    private static org.apache.hadoop.conf.Configuration configuration;
    private static Connection connection = null;
    private static BufferedMutator mutator;
    private static int count = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("userRecommend"));
        params.writeBufferSize(2 * 1024 * 1024);
        mutator = connection.getBufferedMutator(params);
    }

    @Override
    public void close() throws Exception {
        if (mutator != null) {
            mutator.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(Tuple2<String, List<String>> value, Context context) throws Exception {
        String rowKey = value.f0;
        String key = "recommendList";
        String val = value.f1.toString();
        //写入hbase
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes(key),Bytes.toBytes(val));
        mutator.mutate(put);
        if (count >= 500){
            mutator.flush();
            count = 0;
        }
        count = count + 1;
    }
}
