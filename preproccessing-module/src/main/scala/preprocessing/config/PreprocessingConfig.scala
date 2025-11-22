package preprocessing.config

final case class PreprocessingConfig(
                                    rawBasePath: String,
                                    silverBasePath: String,
                                    goldBasePath: String
                                    )

object PreprocessingConfig {

  def fromEnv(): PreprocessingConfig = {
    val rawBase       = sys.env.getOrElse("RAW_BASE_PATH", "hdfs:///data/raw")
    val silverBase    = sys.env.getOrElse("SILVER_BASE_PATH", "hdfs:///data/silver")
    val goldBase      = sys.env.getOrElse("GOLD_BASE_PATH", "hdfs:///data/gold")


    PreprocessingConfig(
      rawBasePath    = rawBase,
      silverBasePath = silverBase,
      goldBasePath   = goldBase
    )
  }
}
