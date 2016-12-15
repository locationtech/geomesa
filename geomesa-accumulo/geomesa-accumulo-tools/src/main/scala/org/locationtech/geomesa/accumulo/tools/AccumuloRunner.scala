/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.tools

import java.io.FileNotFoundException

import com.beust.jcommander.{JCommander, ParameterException}
import org.apache.accumulo.server.client.HdfsZooInstance
import org.locationtech.geomesa.accumulo.tools.data._
import org.locationtech.geomesa.accumulo.tools.export.{AccumuloBinExportCommand, AccumuloExportCommand}
import org.locationtech.geomesa.accumulo.tools.ingest.{AccumuloIngestCommand, IngestRasterCommand}
import org.locationtech.geomesa.accumulo.tools.stats._
import org.locationtech.geomesa.accumulo.tools.status._
import org.locationtech.geomesa.tools.export.GenerateAvroSchemaCommand
import org.locationtech.geomesa.tools.status.{EnvironmentCommand, HelpCommand, VersionCommand}
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{Command, ConvertCommand, Runner}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.concurrent._
import scala.util.Try
import scala.util.control.NonFatal
import scala.xml.XML

object AccumuloRunner extends Runner {

  override val name: String = "geomesa"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new AddAttributeIndexCommand,
    new AccumuloCreateSchemaCommand,
    new AccumuloDeleteCatalogCommand,
    new AccumuloDeleteFeaturesCommand,
    new DeleteRasterCommand,
    new AccumuloDescribeSchemaCommand,
    new EnvironmentCommand,
    new AccumuloExplainCommand,
    new AccumuloExportCommand,
    new HelpCommand(this, jc),
    new AccumuloIngestCommand,
    new IngestRasterCommand,
    new AccumuloKeywordsCommand,
    new AccumuloGetTypeNamesCommand,
    new AccumuloRemoveSchemaCommand,
    new TableConfCommand(this, jc),
    new AccumuloVersionRemoteCommand,
    new VersionCommand,
    new AccumuloGetSftConfigCommand,
    new GenerateAvroSchemaCommand,
    new AccumuloStatsAnalyzeCommand,
    new AccumuloStatsBoundsCommand,
    new AccumuloStatsCountCommand,
    new AccumuloStatsTopKCommand,
    new AccumuloStatsHistogramCommand,
    new AddIndexCommand,
    new ConvertCommand,
    new AccumuloBinExportCommand
  )

  /**
    * Loads geomesa system properties from geomesa-site.xml
    * Loads accumulo properties for instance and zookeepers from the accumulo installation found via
    * the system path in ACCUMULO_HOME in the case that command line parameters are not provided
    */
  override def resolveEnvironment(command: Command): Unit = {
    lazy val zookeepers = {
      val accumuloSiteXml = SystemProperty("geomesa.tools.accumulo.site.xml").option.getOrElse {
        s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml"
      }

      try {
        (XML.loadFile(accumuloSiteXml) \\ "property")
            .filter(x => (x \ "name").text == "instance.zookeeper.host")
            .map(y => (y \ "value").text)
            .head
      } catch {
        case e: FileNotFoundException => throw new ParameterException("Accumulo Site XML was not found, unable to locate zookeepers. " +
          "Please provide the --zookeepers parameter or ensure the file $ACCUMULO_HOME/conf/accumulo-site.xml exists or optionally, " +
          "specify a different configuration file with the System Property 'geomesa.tools.accumulo.site.xml'.", e)
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
      try {
        // This block checks for the same system property which Accumulo uses for Zookeeper timeouts.
        //  If it is set, we use it.  Otherwise, a timeout of 5 seconds is used.
        //  Don't give default to GeoMesaSystemProperty so .option will throw a None
        val lookupTime: Long = SystemProperty("instance.zookeeper.timeout").option.flatMap{ p =>
          Try { java.lang.Long.parseLong(p) }.toOption
        }.getOrElse(5000L)

        Command.user.debug(s"Looking up Accumulo Instance Id in Zookeeper for $lookupTime milliseconds.")
        Command.user.debug("You can specify the Instance Id via the command line or\n" +
          "change the Zookeeper timeout by setting the system property 'instance.zookeeper.timeout'.")

        import scala.concurrent.duration._
        lazy val ff: Either[Throwable, String] = try {
          // This call not only provides the instance name but in situations where the instance
          // name is provided, this allows us to check that we have access to Accumulo dependencies.
          val i = HdfsZooInstance.getInstance().getInstanceName
          Right(i)
        } catch {
          case e: Throwable => Left(e)
        }
        val future = Await.result(Future(ff)(ExecutionContext.global),  lookupTime.millis)

        future match {
          case Right(instanceId) => if (p.instance == null) p.instance = instanceId
          case Left(e) => throw e
        }
      } catch {
        case e @ ( _: ClassNotFoundException | _: NoClassDefFoundError ) =>
          throw new ParameterException("Unable to locate Accumulo instance. Please ensure that $ACCUMULO_HOME is set " +
            "correctly and/or provide the instance name with the parameter --instance.", e)
        case e: TimeoutException =>
          throw new ParameterException("Unable to connect to Zookeeper instance " +
          "within timeout period. Consider increasing the Zookeeper timeout by setting the system property " +
          "'instance.zookeeper.timeout' (default: 5000ms).", e)
        case NonFatal(e) => Command.user.warn(s"Exception getting zoo instance: ${e.toString}"); null
      }
    }
  }
}
