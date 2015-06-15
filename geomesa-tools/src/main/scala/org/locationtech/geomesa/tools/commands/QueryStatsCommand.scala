/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import java.io._
import java.util.Date

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.accumulo.stats.RasterQueryStatTransform
import org.locationtech.geomesa.raster.data.AccumuloRasterStore
import org.locationtech.geomesa.tools.AccumuloProperties
import org.locationtech.geomesa.tools.commands.QueryStatsCommand.QueryStatsParameters

class QueryStatsCommand(parent: JCommander) extends Command(parent) with AccumuloProperties {
  override val command: String = "querystats"
  override val params = new QueryStatsParameters()

  override def execute() = {
    val queryRecords = createRasterStore
      .getQueryRecords(params.numRecords * RasterQueryStatTransform.NUMBER_OF_CQ_DATA_TYPES)
    val fw = getFileWriter
    val out = new BufferedWriter(fw)
    queryRecords.foreach(str => {
      out.write(str)
      out.newLine()
    })
    out.close()
  }

  def createRasterStore: AccumuloRasterStore = {
    val password = getPassword(params.password)
    val auths = Option(params.auths)
    AccumuloRasterStore(params.user, password,
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
  @Parameters(commandDescription = "Export queries and statistics about the last X number of queries to a CSV file.")
  class QueryStatsParameters extends RasterParams {
    @Parameter(names = Array("-num", "--number-of-records"), description = "Number of query records to export from Accumulo")
    var numRecords: Int = 1000

    @Parameter(names = Array("-o", "--output"), description = "Name of the file to output to")
    var file: String = null
  }
}
