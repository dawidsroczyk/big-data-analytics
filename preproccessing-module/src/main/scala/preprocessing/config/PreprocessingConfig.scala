package preprocessing.config

final case class PreprocessingConfig(
                                    rawBasePath: String,
                                    silverBasePath: String
                                    )

object PreprocessingConfig {

  def fromEnv(): PreprocessingConfig = {
    val rawBase    = sys.env.getOrElse("RAW_BASE_PATH", "data/raw")
    val silverBasePath = sys.env.getOrElse("SILVER_BASE_PATH", "data/silver")

    PreprocessingConfig(
      rawBasePath = rawBase, silverBasePath = silverBasePath
    )
  }
}
