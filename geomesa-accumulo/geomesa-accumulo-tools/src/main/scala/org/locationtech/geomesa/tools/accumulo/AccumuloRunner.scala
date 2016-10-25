/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo

import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
import org.apache.accumulo.server.client.HdfsZooInstance
import org.locationtech.geomesa.tools.accumulo.commands._
import org.locationtech.geomesa.tools.accumulo.commands.stats._
import org.locationtech.geomesa.tools.common.commands.{Command, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.common.{Prompt, Runner}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal
import scala.xml.XML

object AccumuloRunner extends Runner {

  override val scriptName: String = "geomesa"

  override val commands: List[Command] = List(
    new AddAttributeIndexCommand(jc),
    new CreateSchemaCommand(jc),
    new DeleteCatalogCommand(jc),
    new DeleteFeaturesCommand(jc),
    new DeleteRasterCommand(jc),
    new GetSchemaCommand(jc),
    new EnvironmentCommand(jc),
    new ExplainCommand(jc),
    new ExportCommand(jc),
    new HelpCommand(jc),
    new IngestCommand(jc),
    new IngestRasterCommand(jc),
    new KeywordCommand(jc),
    new GetNamesCommand(jc),
    new RemoveSchemaCommand(jc),
    new TableConfCommand(jc),
    new AccumuloVersionCommand(jc),
    new QueryRasterStatsCommmand(jc),
    new GetSftConfigCommand(jc),
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

    val params = Option(command.params)

    params.collect {
      case p: RequiredCredentialsParams if p.password == null => p
      case p: OptionalCredentialsParams if p.password == null && p.user != null => p
    }.foreach(_.password = Prompt.readPassword())

    params.collect { case p: InstanceNameParams => p }.foreach { p =>
      if (p.zookeepers == null) {
        p.zookeepers = zookeepers
      }
      if (p.instance == null) {
        p.instance = try {
          // This block checks for the same system property which Accumulo uses for Zookeeper timeouts.
          //  If it is set, we use it.  Otherwise, a timeout of 5 seconds is used.
          val lookupTime: Long =
            Option(System.getProperty("instance.zookeeper.timeout")).flatMap{s =>
              Try { java.lang.Long.parseLong(s) }.toOption
            }.getOrElse(5000L)

          logger.debug(s"Looking up Accumulo Instance Id in Zookeeper for $lookupTime milliseconds.")
          logger.debug("You can specify the Instance Id via the command line or\n" +
            "change the Zookeeper timeout by setting the system property 'instance.zookeeper.timeout'.")

          import scala.concurrent.duration._
          Await.result(Future(HdfsZooInstance.getInstance().getInstanceName)(ExecutionContext.global),  lookupTime.millis)
        } catch {
          case NonFatal(e) => logger.warn(s"Exception getting zoo instance: ${e.toString}"); null
        }
      }
    }
  }
}
