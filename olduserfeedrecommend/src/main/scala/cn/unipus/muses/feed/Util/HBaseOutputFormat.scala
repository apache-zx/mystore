package cn.unipus.muses.feed.Util

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class HBaseOutputFormat extends  OutputFormat[(String, List[String])]{
  val zkServer = "hadoop101,hadoop102,hadoop103"
  val port = "2181"
  var conn: Connection = null
  var mutator: BufferedMutator = null
  var count = 0
  override def configure(parameters: Configuration): Unit = {

  }

  override def open(taskNumber: Int, numTasks: Int):  Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
    config.set(HConstants.ZOOKEEPER_QUORUM, zkServer)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)

    val tableName: TableName = TableName.valueOf("userRecommend")

    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    params.writeBufferSize(1024 * 1024)
    mutator = conn.getBufferedMutator(params)
    count = 0
  }

  override def writeRecord(record: (String, List[String])): Unit = {
    val cf1 = "cf1"
    val RowKey = record._1
    val put: Put = new Put(RowKey.getBytes())
    put.addColumn(cf1.getBytes(), "recommendList".getBytes(), record._2.toString().getBytes())
    mutator.mutate(put)
    if(count >= 10){
      mutator.flush()
      count = 0
    }
    count = count + 1
  }

  override def close(): Unit = {
    try{
      if(conn != null )
        conn.close()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}
