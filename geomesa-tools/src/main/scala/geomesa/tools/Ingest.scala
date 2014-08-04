package geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging

class Ingest() extends Logging {

  def getAccumuloDataStoreConf(config: Config) = Map (
    "instanceId"   ->  sys.env.getOrElse("GEOMESA_INSTANCEID", "instanceId"),
    "zookeepers"   ->  sys.env.getOrElse("GEOMESA_ZOOKEEPERS", "zoo1:2181,zoo2:2181,zoo3:2181"),
    "user"         ->  sys.env.getOrElse("GEOMESA_USER", "admin"),
    "password"     ->  sys.env.getOrElse("GEOMESA_PASSWORD", "admin"),
    "auths"        ->  sys.env.getOrElse("GEOMESA_AUTHS", ""),
    "visibilities" ->  sys.env.getOrElse("GEOMESA_VISIBILITIES", ""),
    "tableName"    ->  config.table
  )

  def defineIngestJob(config: Config): Boolean = {
    val dsConfig = getAccumuloDataStoreConf(config)
    config.format.toUpperCase match {
      case "CSV" | "TSV" =>
        config.method.toLowerCase match {
          case "mapreduce" =>
            true
          case "naive" =>
            new SVIngest(config, dsConfig)
            true
          case _ =>
            logger.error("Error, no such ingest method for CSV or TSV found, no data ingested")
            false
        }
      case "GEOJSON" | "JSON" =>
        config.method.toLowerCase match {
          case "naive" =>
            new GeoJsonIngest(config, dsConfig)
            true
          case _ =>
            logger.error("Error, no such ingest method for GEOJSON or JSON found, no data ingested")
            false
        }
      case "GML" | "KML" =>
        config.method.toLowerCase match {
          case "naive" =>
            true
          case _ =>
            logger.error("Error, no such ingest method for GML or KML found, no data ingested")
            false
        }
      case "SHAPEFILE" | "SHP" =>
        config.method.toLowerCase match {
          case "naive" =>
            true
          case _ =>
            logger.error("Error, no such ingest method for Shapefiles found, no data ingested")
            false
        }
      case _ =>
        logger.error(s"Error, format: \'${config.format}\'")
        false
    }
  }
}