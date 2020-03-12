/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools

import com.beust.jcommander.{JCommander, ParameterException}
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.xml.XML

object AccumuloRunner extends RunnerWithAccumuloEnvironment {

  override val name: String = "geomesa-accumulo"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new data.AccumuloAgeOffCommand,
    new data.AccumuloCompactCommand,
    new data.AccumuloManagePartitionsCommand(this, jc),
    new data.AddAttributeIndexCommand,
    new data.AddIndexCommand,
    new data.TableConfCommand(this, jc),
    new export.AccumuloExplainCommand,
    new export.AccumuloExportCommand,
    new export.AccumuloPlaybackCommand,
    new ingest.AccumuloDeleteFeaturesCommand,
    new ingest.AccumuloIngestCommand,
    new schema.AccumuloCreateSchemaCommand,
    new schema.AccumuloDeleteCatalogCommand,
    new schema.AccumuloRemoveSchemaCommand,
    new schema.AccumuloUpdateSchemaCommand,
    new status.AccumuloDescribeSchemaCommand,
    new status.AccumuloGetSftConfigCommand,
    new status.AccumuloGetTypeNamesCommand,
    new status.AccumuloVersionRemoteCommand,
    new stats.AccumuloStatsAnalyzeCommand,
    new stats.AccumuloStatsBoundsCommand,
    new stats.AccumuloStatsConfigureCommand,
    new stats.AccumuloStatsCountCommand,
    new stats.AccumuloStatsTopKCommand,
    new stats.AccumuloStatsHistogramCommand,
    // common commands, placeholders for script functions
    new ConvertCommand,
    new ConfigureCommand,
    new ClasspathCommand,
    new EnvironmentCommand,
    new GenerateAvroSchemaCommand,
    new HelpCommand(this, jc),
    new ScalaConsoleCommand,
    new VersionCommand
  )
}

trait RunnerWithAccumuloEnvironment extends Runner {

  /**
    * Loads geomesa system properties from geomesa-site.xml
    * Loads accumulo properties for instance and zookeepers from the accumulo installation found via
    * the system path in ACCUMULO_HOME in the case that command line parameters are not provided
    */
  override def resolveEnvironment(command: Command): Unit = {
    val params = Option(command.params)

    // If password not supplied, and not using keytab, prompt for it
    // Error if both password and keytab supplied
    params.collect {
      case p: AccumuloConnectionParams if p.password != null && p.keytab != null =>
        throw new ParameterException("Cannot specify both password and keytab")
      case p: AccumuloConnectionParams if p.password == null && p.keytab == null => p
    }.foreach(_.password = Prompt.readPassword())

    // attempt to look up the instance
    params.collect { case p: InstanceNameParams => p }.foreach { p =>
      if (p.zookeepers == null) {
        p.zookeepers = {
          val accumuloSiteXml = SystemProperty("geomesa.tools.accumulo.site.xml").option.getOrElse {
            s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml"
          }

          try {
            (XML.loadFile(accumuloSiteXml) \\ "property")
                .filter(x => (x \ "name").text == "instance.zookeeper.host")
                .map(y => (y \ "value").text)
                .head
          } catch {
            case e: Throwable => throw new ParameterException("Accumulo Site XML was not found or was unable to be read, unable to locate zookeepers. " +
                "Please provide the --zookeepers parameter or ensure the file $ACCUMULO_HOME/conf/accumulo-site.xml exists or optionally, " +
                "specify a different configuration file with the System Property 'geomesa.tools.accumulo.site.xml'.", e)
          }
        }
      }
    }
  }

  override def environmentErrorInfo(): Option[String] = {
    if (sys.env.get("ACCUMULO_HOME").isEmpty || sys.env.get("HADOOP_HOME").isEmpty) {
      Option("\nWarning: ACCUMULO_HOME and/or HADOOP_HOME are not set as environment variables." +
        "\nGeoMesa tools will not run without the appropriate Accumulo and Hadoop jars in the tools classpath." +
        "\nPlease ensure that those jars are present in the classpath by running 'geomesa classpath'." +
        "\nTo take corrective action, please place the necessary jar files in the lib directory of geomesa-tools.")
    } else { None }
  }
}
