package preprocessing.config

final case class PreprocessingConfig(
                                      rawBasePath: String,
                                      silverBasePath: String,
                                      goldBasePath: String
                                    )

object PreprocessingConfig {

  def fromEnv(): PreprocessingConfig = {
    val rawBase    = sys.env.getOrElse("RAW_BASE_PATH", "hdfs://namenode:8020/bronze/raw")
    val silverBase = sys.env.getOrElse("SILVER_BASE_PATH", "hdfs://namenode:8020/silver")
    val goldBase   = sys.env.getOrElse("GOLD_BASE_PATH", "hdfs://namenode:8020/gold")

    PreprocessingConfig(
      rawBasePath    = rawBase,
      silverBasePath = silverBase,
      goldBasePath   = goldBase
    )
  }
}
