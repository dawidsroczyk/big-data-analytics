import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

object RawDataSimulator {

  private val client = HttpClient.newHttpClient()

  private val isoFormatter: DateTimeFormatter =
    DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC)

  def main(args: Array[String]): Unit = {
    val baseUrl = sys.env.getOrElse("API_BASE_URL", "http://localhost:8000/api/v1")
    val lat     = sys.env.getOrElse("LAT", "40.7128")
    val lng     = sys.env.getOrElse("LNG", "-74.0060")

    val weatherDir = Paths.get("data/raw/weather")
    val trafficDir = Paths.get("data/raw/traffic")
    Files.createDirectories(weatherDir)
    Files.createDirectories(trafficDir)

    val iterations = sys.env.getOrElse("RAW_ITERATIONS", "10").toInt

    println(s"Collecting $iterations samples from API: $baseUrl (lat=$lat, lng=$lng)")

    (1 to iterations).foreach { i =>
      val now      = Instant.now()
      val tsString = isoFormatter.format(now).replace(":", "-") // dla bezpieczeństwa w nazwie pliku

      println(s"[$i/$iterations] Fetching at $tsString")

      // WEATHER
      val weatherUrl = s"$baseUrl/weather?lat=$lat&lng=$lng"
      val weatherJson = httpGet(weatherUrl)
      val weatherFile = weatherDir.resolve(s"weather_$tsString.json")
      Files.write(weatherFile, weatherJson.getBytes(StandardCharsets.UTF_8))

      // TRAFFIC
      val trafficUrl = s"$baseUrl/traffic?lat=$lat&lng=$lng"
      val trafficJson = httpGet(trafficUrl)
      val trafficFile = trafficDir.resolve(s"traffic_$tsString.json")
      Files.write(trafficFile, trafficJson.getBytes(StandardCharsets.UTF_8))

      Thread.sleep(5000L)
    }

    println("Done collecting RAW sample data.")
  }

  private def httpGet(url: String): String = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET()
      .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() / 100 != 2) {
      throw new RuntimeException(s"Request to $url failed with status ${response.statusCode()}")
    }
    response.body()
  }
}
