package cn.unipus.muses.feed.producesource

import java.io.PrintWriter

import scala.collection.mutable.{ListBuffer, Set}
import scala.util.Random

object UserGoodsSource {
  def main(args: Array[String]): Unit = {
    //测试输出用户维度
    for (i <- 100 to 200) {
      // val id = UUID.randomUUID().toString
      val userFeatures = getFeatures()
      println(i + "\t" + userFeatures.toString() + "\t" + userFeatures.size)
    }
    //将文本输出到文件中

    val out1 = new PrintWriter("d:/soft/input/userFeature1.txt")
    for (i <- 10000 to 20000) {
      val userFeatures = getFeatures()
      for (userFeature <- userFeatures) {
        out1.write(i + "\t" + userFeature + "\t" + 100 + "\n")
      }
    }
    out1.close()
    //将商品维度写入文件
    val random = new Random()
    val out2 = new PrintWriter("d:/soft/input/goodFeature1.txt")
    for (i <- 1000 to 2000) {
      val goodFeatures = getFeatures()
      val level = (random.nextInt(9) + 100)
      for (goodFeature <- goodFeatures) {
        out2.write(goodFeature + "\t" + i + "\t" + level + "\t" + goodFeatures.size + "\n")
      }
    }
    out2.close()
    //将用户浏览导入商品表中
    val out3 = new PrintWriter("d:/soft/input/userLook.txt")

    for (i <- 10000 to 20000) {
      //val userFeatures = getFeatures()
        for(k <- 2 to random.nextInt(5) +5){
          val good_id = random.nextInt(1000) + 1000
          for (j <- 5 to  random.nextInt(20) + 5) {
            out3.write(i + "\t 1561646641 \t" + good_id + "\n")
          }
        }
      }

    out3.close()


  }

  def getFeatures(): List[String] = {
    val features = new ListBuffer[String]
    val set = Set[String]()
    val goodsFeatures = Seq(
      "儿童文学", "歌谣", "绘本故事", "人气动画", "科普百科",
      "学科教育", "启蒙教育", "通识教育", "哄睡", "叫早",
      "磨耳朵", "艺术修养", "社交情商", "行为养成", "阅读素养",
      "逻辑思维", "亲子家教", "语感培养", "语音训练", "口头表达",
      "英式", "美式", "家庭生活", "朋友", "日常活动",
      "爱好", "情感与情绪", "故事", "家具", "计划与安排",
      "交通", "食物", "学校", "科普", "卫生与健康",
      "节日活动", "旅游", "天气", "沟通交流", "环境",
      "艺术", "社会", "人物传记", "名著", "动物",
      "植物", "儿童文学", "文化", "人文"
    )
    val length = goodsFeatures.length
    val random = new Random()
    for (i <- 0 to (random.nextInt(20) + 5)) {
      val index = random.nextInt(length)
      set.add(goodsFeatures(index))
    }
    features ++= set.toList
    features.toList
  }
}
