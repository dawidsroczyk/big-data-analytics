package preprocessing.test

import preprocessing.job.{
  WeatherPreprocessingJob,
  TrafficPreprocessingJob,
  AirQualityPreprocessingJob,
  UvPreprocessingJob,
  FullPreprocessingJob
}

/**
 * End-to-end smoke test for the full preprocessing pipeline.
 *
 * This test performs:
 *   1. Weather preprocessing (RAW → SILVER)
 *   2. Traffic preprocessing (RAW → SILVER)
 *   3. Air pollution preprocessing (RAW → SILVER)
 *   4. UV preprocessing (RAW → SILVER)
 *   5. Full feature preprocessing (SILVER → GOLD)
 *
 * If any job throws an exception → the test fails.
 * If all jobs finish successfully → the test prints SUCCESS.
 */
object EndToEndJobsSmokeTest {

  def main(args: Array[String]): Unit = {
    println("=== EndToEndJobsSmokeTest: START ===")

    try {
      println("\n[1/5] Running WeatherPreprocessingJob ...")
      WeatherPreprocessingJob.main(Array.empty)

      println("\n[2/5] Running TrafficPreprocessingJob ...")
      TrafficPreprocessingJob.main(Array.empty)

      println("\n[3/5] Running AirQualityPreprocessingJob ...")
      AirQualityPreprocessingJob.main(Array.empty)

      println("\n[4/5] Running UvPreprocessingJob ...")
      UvPreprocessingJob.main(Array.empty)

      println("\n[5/5] Running FullPreprocessingJob ...")
      FullPreprocessingJob.main(Array.empty)

      println("\n=== EndToEndJobsSmokeTest: SUCCESS — all jobs completed ===")
    } catch {
      case e: Throwable =>
        println("\n=== EndToEndJobsSmokeTest: ERROR — a job failed ===")
        e.printStackTrace()
        sys.exit(1)
    }
  }
}
