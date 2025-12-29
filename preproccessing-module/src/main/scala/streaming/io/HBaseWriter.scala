package streaming.io

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row}

object HBaseWriter {

  case class HBaseConfig(quorum: String, clientPort: String, znodeParent: String)

  def writeLatestRDD(
                      df: DataFrame,
                      table: String,
                      cf: String,
                      rowKeyFn: Row => String,
                      cols: Seq[(String, Row => Array[Byte])],
                      hcfg: HBaseConfig
                    ): Unit = {

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hcfg.quorum)
    conf.set("hbase.zookeeper.property.clientPort", hcfg.clientPort)
    conf.set("zookeeper.znode.parent", hcfg.znodeParent)

    df.rdd.foreachPartition { part =>
      val iter = part.buffered
      if (iter.hasNext) {
        val conn = ConnectionFactory.createConnection(conf)
        val tbl  = conn.getTable(TableName.valueOf(table))
        try {
          val puts = new java.util.ArrayList[Put]()
          iter.foreach { r =>
            val rk = Bytes.toBytes(rowKeyFn(r))
            val p  = new Put(rk)
            cols.foreach { case (col, f) =>
              val value = f(r)
              if (value != null) p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), value)
            }
            puts.add(p)
          }
          if (!puts.isEmpty) tbl.put(puts)
        } finally {
          tbl.close()
          conn.close()
        }
      }
    }
  }

  def bytesOrNullDouble(v: java.lang.Double): Array[Byte] = if (v == null) null else Bytes.toBytes(v.doubleValue())
  def bytesOrNullLong(v: java.lang.Long): Array[Byte]     = if (v == null) null else Bytes.toBytes(v.longValue())
  def bytesOrNullString(v: String): Array[Byte]           = if (v == null) null else Bytes.toBytes(v)
}
