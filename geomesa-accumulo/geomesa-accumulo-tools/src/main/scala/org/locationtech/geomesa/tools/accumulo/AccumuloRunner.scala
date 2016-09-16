/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo

import org.apache.accumulo.server.client.HdfsZooInstance
import org.locationtech.geomesa.tools.accumulo.commands._
import org.locationtech.geomesa.tools.accumulo.commands.stats._
import org.locationtech.geomesa.tools.common.commands.{Command, GenerateAvroSchemaCommand, VersionCommand}
import org.locationtech.geomesa.tools.common.{Prompt, Runner}

import scala.util.control.NonFatal
import scala.xml.XML

object AccumuloRunner extends Runner {

  override val scriptName: String = "geomesa"

  override val commands: List[Command] = List(
    new CreateCommand(jc),
    new DeleteCatalogCommand(jc),
    new DeleteFeaturesCommand(jc),
    new DeleteRasterCommand(jc),
    new DescribeCommand(jc),
    new EnvironmentCommand(jc),
    new ExplainCommand(jc),
    new ExportCommand(jc),
    new HelpCommand(jc),
    new IngestCommand(jc),
    new IngestRasterCommand(jc),
    new KeywordCommand(jc),
    new ListCommand(jc),
    new RemoveSchemaCommand(jc),
    new TableConfCommand(jc),
    new VersionCommand(jc),
    new QueryRasterStatsCommmand(jc),
    new GetSftCommand(jc),
    new GenerateAvroSchemaCommand(jc),
    new StatsAnalyzeCommand(jc),
    new StatsBoundsCommand(jc),
    new StatsCountCommand(jc),
    new StatsTopKCommand(jc),
    new StatsHistogramCommand(jc),
    new AddIndexCommand(jc)
  )

  /**
    * Loads accumulo properties for instance and zookeepers from the accumulo installation found via
    * the system path in ACCUMULO_HOME in the case that command line parameters are not provided
    */
  override def resolveEnvironment(command: Command): Unit = {
    lazy val zookeepers = {
      val accumuloSiteXml = Option(System.getProperty("geomesa.tools.accumulo.site.xml"))
          .getOrElse(s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml")
      try {
        (XML.loadFile(accumuloSiteXml) \\ "property")
            .filter(x => (x \ "name").text == "instance.zookeeper.host")
            .map(y => (y \ "value").text)
            .head
      } catch {
        case NonFatal(e) => null
      }
    }

    Option(command.params).collect { case p: AccumuloConnectionParams => p }.foreach { params =>
      if (params.instance == null) {
        params.instance = HdfsZooInstance.getInstance().getInstanceName
      }
      if (params.zookeepers == null) {
        params.zookeepers = zookeepers
      }
      if (params.password == null) {
        params.password = Prompt.readPassword()
      }
    }
  }
}
