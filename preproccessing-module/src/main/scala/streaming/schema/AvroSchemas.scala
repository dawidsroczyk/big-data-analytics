package streaming.schema

object AvroSchemas {

  val trafficAvroSchemaJson: String =
    """
      |{
      |  "type":"record",
      |  "name":"nifiRecord",
      |  "namespace":"org.apache.nifi",
      |  "fields":[
      |    {"name":"location","type":["string","null"]},
      |    {"name":"free_flow_speed","type":["double","null"]},
      |    {"name":"current_travel_time","type":["int","null"]},
      |    {"name":"free_flow_travel_time","type":["int","null"]},
      |    {"name":"road_closure","type":["boolean","null"]},
      |    {"name":"updated_at","type":["string","null"]},
      |    {"name":"data_provider","type":["string","null"]}
      |  ]
      |}
      |""".stripMargin

  val airQualityAvroSchemaJson: String =
    """
      |{
      |  "type":"record",
      |  "name":"nifiRecord",
      |  "namespace":"org.apache.nifi",
      |  "fields":[
      |    {"name":"location","type":["string","null"]},
      |    {"name":"updated_at","type":["string","null"]},
      |    {"name":"aqi","type":["int","null"]},
      |    {"name":"pm2_5","type":["double","null"]},
      |    {"name":"pm10","type":["double","null"]},
      |    {"name":"no2","type":["double","null"]},
      |    {"name":"so2","type":["double","null"]},
      |    {"name":"o3","type":["double","null"]},
      |    {"name":"co","type":["double","null"]},
      |    {"name":"data_provider","type":["string","null"]}
      |  ]
      |}
      |""".stripMargin

  val uvAvroSchemaJson: String =
    """
      |{
      |  "type":"record",
      |  "name":"nifiRecord",
      |  "namespace":"org.apache.nifi",
      |  "fields":[
      |    {"name":"location","type":["string","null"]},
      |    {"name":"timestamp","type":["string","null"]},
      |    {"name":"uv_index","type":["double","null"]},
      |    {"name":"data_provider","type":["string","null"]}
      |  ]
      |}
      |""".stripMargin
}
