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
      )
    cmd("features") action { (_, c) =>
      c.copy(mode = "features")
    } text ("features is a command") children(
      )
    cmd("ingest") action { (_, c) =>
      c.copy(mode = "ingest") } text "Ingest a feature into GeoMesa" children (
      opt[String]('i', "instanceId").action { (s, c) =>
        c.copy(instanceId = s) } text "the ID (name) of the Accumulo instance, e.g:  mycloud" required(),
      opt[String]('z', "zookeepers").action { (s, c) =>
        c.copy(zookeepers = s) } text "the comma-separated list of Zookeeper nodes that" +
        " support your Accumulo instance, e.g.:  zoo1:2181,zoo2:2181,zoo3:2181" required(),
      opt[String]('u', "user").action { (s, c) =>
        c.copy(user = s) } text "the Accumulo user that will own the connection, e.g.:  root" required(),
      opt[String]('p', "password").action { (s, c) =>
        c.copy(password = s) } text "the password for the Accumulo user that will own the connection," +
        " e.g.:  guest" required(),
      arg[String]("<authorizations>").action { (s, c) =>
        c.copy(authorizations = s) } text "the (optional) list of comma-separated Accumulo authorizations that" +
        " should be applied to all data written or read by this Accumulo user; note that this is NOT the list of" +
        " low-level database permissions such as 'Table.READ', but more a series of text tokens that decorate cell" +
        " data, e.g.:  Accounting,Purchasing,Testing" optional(),
      opt[String]('t', "table").action { (s, c) =>
        c.copy(table = s) } text "the name of the Accumulo table to use, or create, " +
        "if it does not already exist -- to contain the new data",
      arg[String]("<hdfs path>").action { (s, c) =>
        c.copy(pathHDFS = s) } text "HDFS path of file to ingest" optional(),
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
      case "export" =>
        println("exporting")
      case "feature" =>
        println("feature-ing")
      case "ingest" =>
        println("Ingesting...")
        Ingest.defineIngestJob(config)
    }
  } getOrElse {
    Console.printf("I don't know what you're trying to do right now.")
  }
}

case class Config(mode: String = null, instanceId: String = null,
                  zookeepers: String = null, user: String = null,
                  password: String = null, authorizations: String = null,
                  table: String = null, pathHDFS: String = null, spec: String = null,
                  latField: String = null, lonField: String = null,
                  dtField: String = null, dtFormat: String = null,
                  method: String = null, file: String = null,
                  format: String = null)

