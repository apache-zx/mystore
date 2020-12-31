package cn.unipus.muses.feed

import java.lang

import cn.unipus.muses.feed.Util.HBaseOutputFormat
import org.apache.flink.api.common.functions.{GroupReduceFunction, JoinFunction, ReduceFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//定义输入用户特征和商品特征样例类
case class UserFeatures(feature: String, userId: Long, featureSize: Int)
case class GoodFeatures(feature: String, goodId: Long, featureSize: Int)
//定义输出类型样例类
case class UserGoodsRelation(goodId:String , relevance: Double)

object FeedRecommend {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val userInput  = env.readTextFile("d:/soft/input/userFeature.txt")
    val goodInput = env.readTextFile("d:/soft/input/goodFeature.txt")

    //将输入文件map成userFeatures样例类
    val userDataSet = userInput
      .map(data => {
        val arr = data.split("\t")
        UserFeatures(arr(0), arr(1).toLong, arr(2).toInt)
      })
    //.print("user")

    //将输入文价map成goodFeatures样例类
    val goodDataSet = goodInput
      .map(data => {
        val arr = data.split("\t")
        GoodFeatures(arr(0), arr(1).toLong, arr(2).toInt)
      })//.print("goods")

    //将两个dataSet连接起来，按照特征
    val joinDataSet = userDataSet.join(goodDataSet)
        .where(_.feature).equalTo(_.feature)
        .apply( new JoinFunction[UserFeatures, GoodFeatures, (String , Int , Int)] {
          override def join(first: UserFeatures, second: GoodFeatures): (String ,Int , Int) = {
            (first.userId + "_" + second.goodId ,1 , first.featureSize + second.featureSize)
          }
        })
        .groupBy(_._1)
        .reduce(new ReduceFunction[(String, Int, Int)] {
          override def reduce(value1: (String, Int, Int), value2: (String, Int, Int)): (String, Int, Int) = {
            val sameFeatures = value1._2 + value2._2
            (value1._1, sameFeatures, value1._3)
          }
        })
        .map(data => {
          val relevance = (data._2/(data._3 - data._2).toDouble)
          (data._1, data._2 ,relevance)
        })
        .groupBy(_._1.split("_")(0))
        .reduceGroup(new GroupReduceFunction[(String, Int, Double ),(String, List[String])] {
          override def reduce(values: lang.Iterable[(String, Int, Double)], out: Collector[(String, List[String])]): Unit = {
            val allListBuffer:ListBuffer[UserGoodsRelation] = new ListBuffer[UserGoodsRelation]
            val sortedRecommendList: ListBuffer[String] = new ListBuffer[String]
            val iter = values.iterator()
            var key = ""
            while (iter.hasNext){
              val currentItem =iter.next()
              key = currentItem._1.split("_")(0)
              allListBuffer += UserGoodsRelation(currentItem._1.split("_")(1) ,currentItem._3)
            }
            for (elem <- allListBuffer.sortBy(-_.relevance)) {
              sortedRecommendList += elem.goodId
            }
            out.collect(key ,sortedRecommendList.toList)
          }
        })

    joinDataSet.output(new HBaseOutputFormat)

    env.execute()

 }
}
