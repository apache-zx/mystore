package cn.unipus.muses.feed;

import cn.unipus.muses.feed.Util.HBaseSink;
import cn.unipus.muses.feed.bean.DwdGoodFeature;
import cn.unipus.muses.feed.bean.DwdUserFeature;
import cn.unipus.muses.feed.bean.DwdUserLook;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.*;

public class Feed_Recommend {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        String name = "myhive";
        String defaultDatabase = "vof";
        String hiveConfDir = "D:\\myWork\\eclipse-workspace\\feed\\olduserfeedrecommend\\src\\main\\scala\\conf"; // a local path
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
        tableEnv.useDatabase("vof");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        String userFeaturesSql = "select label, user_id, level, feature_count from dwd_user_feature_result";
        String goodsFeaturesSql = "select label , good_id, level, feature_count from dwd_good_feature_result";
        String userLookedSql = "select user_id,  goods_list from dwd_user_look_result";
        Table table = tableEnv.sqlQuery(userFeaturesSql);
        Table table1 = tableEnv.sqlQuery(goodsFeaturesSql);
        Table table2 = tableEnv.sqlQuery(userLookedSql);

        DataStream<DwdGoodFeature> goodStream = tableEnv.toAppendStream(table1, DwdGoodFeature.class)
                .assignTimestampsAndWatermarks(new MyTimeAssigner());

        DataStream<DwdUserFeature> userStream = tableEnv.toAppendStream(table, DwdUserFeature.class)
                .assignTimestampsAndWatermarks(new MyTimeAssigner());

        DataStream<DwdUserLook> userLookStream = tableEnv.toAppendStream(table2, DwdUserLook.class)
                .assignTimestampsAndWatermarks(new MyTimeAssigner());

        //将两条流进行join，根据维度和年级匹配的做推荐，排序规则是交集的个数除以并集
        //user数据格式为（label,user_id,level,feature_count）
        //good数据格式为（label，good_id, level, feature_count）
        //数据转换格式为将user的label和level字段与good的label和level做keyby，相同的数据分到一组，feature_count为user和good的特征数
        //每个人和每个商品都有多个特征，将相同维度和学段的数据做join
        //数据转换为user_id拼上“_”加上good_id ，1 ，user和good的特征数之和，（19994_1466,1,20）
        //用户和商品维度重合的不止一条，按照19994_1466聚合，将第二字段求和，（19994_1466,5,20）
        //同时算出19994与1466相交的维度为5，不同的维度总数为20-5=15；求出相识度为5/15=0.3333,数据转化为（19994_1466,0.333）
        //按照用户分组19994，根据相似度大小排序，算出推荐列表19994[1205,1222,1466....]
        DataStream<Tuple2<String, List<String>>> joinStream = userStream.keyBy("level", "label")
                .intervalJoin(goodStream.keyBy("level", "label"))
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new UserGoodMatchJoinFunction())
                .keyBy(0)
                .sum(1)
                .map(new MyMapper())
                .keyBy(data -> {
                    String[] split = data.f0.split("_");
                    return split[0];
                })
                .process(new MyProcessFunction());


        //将特征匹配的推荐列表去重用户浏览的商品
        SingleOutputStreamOperator<Tuple2<String, List<String>>> connectStream = joinStream.connect(userLookStream)
                .keyBy(data -> data.f0, data -> data.getUser_id())
                .process(new MyConnnectFuntion());

        connectStream.addSink(new HBaseSink());
        connectStream.print();

        env.execute("ss");
    }

    //实现自定义的processJoinFunction，将user_id和good_id拼接起来
    public static class UserGoodMatchJoinFunction extends ProcessJoinFunction<DwdUserFeature, DwdGoodFeature, Tuple3<String, Integer, Long>> {


        @Override
        public void processElement(DwdUserFeature left, DwdGoodFeature right, Context ctx, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
            out.collect(new Tuple3<>(left.getUser_id() + "_" + right.getGood_id(),
                    1,
                    left.getFeature_count() + right.getFeature_count()
            ));
        }
    }

    //实现自定义时间戳和水位线分配器
    public static class MyTimeAssigner implements AssignerWithPunctuatedWatermarks {

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Object lastElement, long extractedTimestamp) {
            return null;
        }

        @Override
        public long extractTimestamp(Object element, long recordTimestamp) {
            return 100000000000L;
        }
    }

    //自定义实现mapFunction，求出商品和用户的交集和并集，得到相似度
    public static class MyMapper implements MapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Double>> {

        @Override
        public Tuple2<String, Double> map(Tuple3<String, Integer, Long> element) throws Exception {
            Double relationRank = element.f1 / (double) (element.f2 - element.f1);
            return new Tuple2<>(element.f0, relationRank);
        }
    }

    //自定义实现processFunction将与同一用户的相关联的商品按relationRank做排序，做推荐；
    public static class MyProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Double>, Tuple2<String, List<String>>> {
        ListState<Tuple2<String, Double>> userRecommendList;

        @Override
        public void open(Configuration parameters) throws Exception {
            userRecommendList = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Double>>("recommend-state", Types.TUPLE(Types.STRING, Types.DOUBLE)));
        }

        @Override
        public void processElement(Tuple2<String, Double> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
            userRecommendList.add(value);
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
            ArrayList<Tuple2<String, Double>> list = Lists.newArrayList(userRecommendList.get());
            //按照相似度排序
            list.sort(new Comparator<Tuple2<String, Double>>() {
                @Override
                public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                    if (o2.f1 > o1.f1) {
                        return 1;
                    } else if (o2.f1 < o1.f1) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            });

            ArrayList<String> sortedList = new ArrayList<>();
            for (Tuple2<String, Double> elem : list) {
                sortedList.add(elem.f0.split("_")[1]);
            }
            out.collect(new Tuple2<>(ctx.getCurrentKey(), sortedList));
        }
    }

    //自定义CoProcessFunction实现去重
    public static class MyConnnectFuntion extends CoProcessFunction<Tuple2<String, List<String>>, DwdUserLook, Tuple2<String, List<String>>> {

        MapState<String, Tuple2<String, List<String>>> recommendList;
        MapState<String, DwdUserLook> lookedList;

        @Override
        public void open(Configuration parameters) throws Exception {
            recommendList = getRuntimeContext().getMapState(new MapStateDescriptor<String, Tuple2<String, List<String>>>("recommend-state", Types.STRING, Types.TUPLE(Types.STRING, Types.LIST(Types.STRING))));
            lookedList = getRuntimeContext().getMapState(new MapStateDescriptor<String, DwdUserLook>("looked-state", Types.STRING, Types.POJO(DwdUserLook.class)));
        }

        @Override
        public void processElement1(Tuple2<String, List<String>> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {

            String key = value.f0;
            recommendList.put(key, value);
            if (lookedList.get(key) != null) {
                //将排序好的list转为linkedHashSet，保证数据顺序不变
                LinkedHashSet<String> recommend = new LinkedHashSet<>(value.f1);
                //将用户看过的数据转为linkedhashset
                String[] split = lookedList.get(key).getGoods_list().split("_");
                LinkedHashSet<String> userLooked = Sets.newLinkedHashSet(Arrays.asList(split));
                //去重操作
                recommend.removeAll(userLooked);
                ArrayList<String> list = Lists.newArrayList(recommend);
                out.collect(new Tuple2<String, List<String>>(key, list));
            }
        }

        @Override
        public void processElement2(DwdUserLook value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
            lookedList.put(value.getUser_id(), value);
        }
    }
}
