/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package geomesa.tools

/**
 * To run from IntelliJ with command line arguments, hit the following key sequence:
 *
 * ALT+SHIFT+F10, Right, E, Enter, Tab, enter your command line parameters, Enter.
 */
class Tools {}

object Tools extends App {
  val parser = new scopt.OptionParser[Config]("geomesa-tools") {
    head("GeoMesa Tools", "1.0")
    help("help").text("show help command")
    cmd("export") action { (_, c) =>
      c.copy(mode = "export")
    } text ("export is a command") children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create, " +
        "if it does not already exist -- to contain the new data" required(),
      opt[String]("feature").action { (s, c) =>
        c.copy(feature = s) } text "the name of the feature to export" required(),
      opt[String]("format").action { (s, c) =>
        c.copy(format = s) } text "the format to export to (e.g. csv, tsv)" required()
      )
    cmd("list") action { (_, c) =>
      c.copy(mode = "list")
    } text ("list is a command") children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create, " +
        "if it does not already exist -- to contain the new data" required()
      )
    cmd("delete") action { (_, c) =>
      c.copy(mode = "delete")
    } text ("delete is a command") children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create, " +
        "if it does not already exist -- to contain the new data" required(),
      opt[String]("feature").action { (s, c) =>
        c.copy(feature = s) } text "the name of the new feature to be create" required()
      )
    cmd("create") action { (_, c) =>
      c.copy(mode = "create")
    } text ("create is a command") children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create, " +
        "if it does not already exist -- to contain the new data" required(),
      opt[String]("feature").action { (s, c) =>
        c.copy(feature = s) } text "the name of the new feature to be create" required(),
      opt[String]("sft").action { (s, c) =>
        c.copy(sft = s) } text "the string representation of the SimpleFeatureType" required()
      )
    cmd("ingest") action { (_, c) =>
      c.copy(mode = "ingest") } text "Ingest a feature into GeoMesa" children (
      opt[String]("table").action { (s, c) =>
        c.copy(table = s) } text "the name of the Accumulo table to use -- or create, " +
        "if it does not already exist -- to contain the new data" optional(),
      arg[String]("<hdfs path>").action { (s, c) =>
        c.copy(pathHDFS = s) } text "HDFS path of file to ingest" optional(),
      opt[String]("typeName").action { (s, c) =>
        c.copy(typeName = s) } text "Name of the feature type to be ingested" required(),
      opt[String]('s', "spec").action { (s, c) =>
        c.copy(spec = s) } text "the specification for the file" required(),
      opt[String]("latitude").action { (s, c) =>
        c.copy(latField = s) } text "Name of latitude field" required(),
      opt[String]("longitude").action { (s, c) =>
        c.copy(lonField = s) } text "Name of longitude field" required(),
      opt[String]("datetime").action { (s, c) =>
        c.copy(dtField = s) } text "Name of the datetime field" required(),
      opt[String]("dtformat").action { (s, c) =>
        c.copy(dtFormat = s) } text "Format of the datetime field" required(),
      opt[String]('m', "method").action { (s, c) =>
        c.copy(method = s) } text "the method used to ingest, e.g.: mapreduce" required(),
      opt[String]("file").action { (s, c) =>
        c.copy(file = s) } text "the file you wish to ingest, e.g.: ~/capelookout.csv" required(),
      opt[String]("format").action { (s, c) =>
        c.copy(format = s) } text "format of ingest file" required()
      )
  }

  parser.parse(args, Config()) map { config =>
    config.mode match {
      case "export" => {
        val ft = new FeaturesTool(config.table)
        ft.exportFeatures()
      }
      case "list" => {
        val ft = new FeaturesTool(config.catalog)
        ft.listFeatures()
      }
      case "delete" => {
        val ft = new FeaturesTool(config.catalog)
        if (ft.deleteFeature(config.feature)) {
          println(s"Feature \'${config.feature}\' successfully deleted.")
        } else {
          println(s"There was an error deleting feature \'${config.feature}\'." +
            " Please check that your configuration settings are correct and try again.")
        }
      }
      case "create" => {
        //create --catalog test_jdk2pq_create --feature testing --sft id:String:indexed=true,dtg:Date,geom:Point:srid=4326
        val ft = new FeaturesTool(config.catalog)
        if (ft.createFeatureType(config.feature, config.sft)) {
          println(s"Feature \'${config.feature}\' with featureType \'${config.sft}\' successfully created.")
        } else {
          println(s"There was an error creating feature \'${config.feature}\' with featureType \'${config.sft}\'." +
            " Please check that your configuration settings are correct and try again.")
        }
      }
      case "ingest" =>
        println("Ingesting...")
        Ingest.defineIngestJob(config)
    }
  } getOrElse {
    Console.printf(s"Error: command not recognized.")
  }
}

case class Config(mode: String = null, instanceId: String = null,
                  zookeepers: String = null, user: String = null,
                  password: String = null, authorizations: String = null, visibilities: String = null,
                  table: String = null, pathHDFS: String = null, spec: String = null,
                  latField: String = null, lonField: String = null,
                  dtField: String = null, dtFormat: String = null,
                  method: String = null, file: String = null, typeName: String = null,
                  format: String = null, catalog: String = null,
                  feature: String = null, sft: String = null)



