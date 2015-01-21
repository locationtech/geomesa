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
package org.locationtech.geomesa.tools.commands

import java.io._
import java.util.Date

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.core.stats.RasterQueryStatTransform
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.locationtech.geomesa.tools.AccumuloProperties
import org.locationtech.geomesa.tools.commands.QueryStatsCommand.{Command, QueryStatsParameters}

class QueryStatsCommand(parent: JCommander) extends Command with AccumuloProperties {

  val params = new QueryStatsParameters()
  parent.addCommand(Command, params)

  override def execute() = {
    val queryRecords = createCoverageStore(params)
      .getQueryRecords(params.numRecords * RasterQueryStatTransform.NUMBER_OF_CQ_DATA_TYPES)
    val fw = getFileWriter
    val out = new BufferedWriter(fw)
    queryRecords.foreach(str => {
      out.write(str)
      out.newLine()
    })
    out.close()
  }

  def createCoverageStore(config: QueryStatsParameters): AccumuloCoverageStore = {
    val password = getPassword(params.password)
    val auths = Option(params.auths)
    AccumuloCoverageStore(params.user, password,
                          params.instance,
                          params.zookeepers,
                          params.table,
                          auths.getOrElse(""),
                          params.visibilities)
  }

  def getFileWriter: FileWriter = {
    val date = new Date().toString.replaceAll(" ", "_")
    Option(params.file) match {
      case Some(file) => new FileWriter(file)
      case None => new FileWriter(s"./queryStats-$date.csv")
    }
  }
}

object QueryStatsCommand {
  val Command = "queryStats"

  @Parameters(commandDescription = "Export queries and statistics about the last X number of queries to a CSV file.")
  class QueryStatsParameters extends RasterParams {
    @Parameter(names = Array("-num", "--number-of-records"), description = "Number of query records to export from Accumulo")
    var numRecords: Int = 1000

    @Parameter(names = Array("-o", "--output"), description = "Name of the file to output to")
    var file: String = null
  }
}
