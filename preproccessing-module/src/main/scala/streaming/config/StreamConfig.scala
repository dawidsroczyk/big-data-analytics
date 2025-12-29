package streaming.config

object StreamConfig {
  val kafkaBootstrap: String = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092")
  val chkBase: String        = sys.env.getOrElse("CHK_BASE", "/checkpoints")

  // HBase
  val hbaseQuorum: String     = sys.env.getOrElse("HBASE_ZK_QUORUM", "hbase")
  val hbasePort: String       = sys.env.getOrElse("HBASE_ZK_PORT", "2181")
  val hbaseZnodeParent: String= sys.env.getOrElse("HBASE_ZNODE_PARENT", "/hbase")

  val silverTable: String = sys.env.getOrElse("HBASE_SILVER_TABLE", "silver_latest")
  val silverCf: String    = sys.env.getOrElse("HBASE_SILVER_CF", "d")

  val goldTable: String   = sys.env.getOrElse("HBASE_GOLD_TABLE", "gold_latest_features")
  val goldCf: String      = sys.env.getOrElse("HBASE_GOLD_CF", "f")
}
