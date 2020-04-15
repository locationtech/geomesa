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
import org.locationtech.geomesa.accumulo.data.AccumuloClientConfig
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.utils.Prompt

object AccumuloRunner extends RunnerWithAccumuloEnvironment {

  import org.locationtech.geomesa.accumulo.tools

  override val name: String = "geomesa-accumulo"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new tools.data.AccumuloAgeOffCommand,
    new tools.data.AccumuloCompactCommand,
    new tools.data.AccumuloManagePartitionsCommand(this, jc),
    new tools.data.AddAttributeIndexCommand,
    new tools.data.AddIndexCommand,
    new tools.data.TableConfCommand(this, jc),
    new tools.export.AccumuloExplainCommand,
    new tools.export.AccumuloExportCommand,
    new tools.export.AccumuloPlaybackCommand,
    new tools.ingest.AccumuloDeleteFeaturesCommand,
    new tools.ingest.AccumuloIngestCommand,
    new tools.schema.AccumuloCreateSchemaCommand,
    new tools.schema.AccumuloDeleteCatalogCommand,
    new tools.schema.AccumuloRemoveSchemaCommand,
    new tools.schema.AccumuloUpdateSchemaCommand,
    new tools.status.AccumuloDescribeSchemaCommand,
    new tools.status.AccumuloGetSftConfigCommand,
    new tools.status.AccumuloGetTypeNamesCommand,
    new tools.status.AccumuloVersionRemoteCommand,
    new tools.stats.AccumuloStatsAnalyzeCommand,
    new tools.stats.AccumuloStatsBoundsCommand,
    new tools.stats.AccumuloStatsConfigureCommand,
    new tools.stats.AccumuloStatsCountCommand,
    new tools.stats.AccumuloStatsTopKCommand,
    new tools.stats.AccumuloStatsHistogramCommand,
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
    * Loads Accumulo properties from `accumulo-client.properties` or `accumulo.conf`
    */
  override def resolveEnvironment(command: Command): Unit = {
    lazy val config = AccumuloClientConfig.load()

    Option(command.params).collect { case p: AccumuloConnectionParams => p }.foreach { p =>
      if (p.user == null) {
        p.user = config.principal.getOrElse(throw new ParameterException("Parameter '--user' is required"))
      }
      if (p.password != null && p.keytab != null) {
        // error if both password and keytab supplied
        throw new ParameterException("Cannot specify both password and keytab")
      } else if (p.password == null && p.keytab == null &&
          (config.authType.isEmpty || config.authType.contains(AccumuloClientConfig.PasswordAuthType))) {
        // if password not supplied, and not using keytab, prompt for it
        p.password = config.token.getOrElse(Prompt.readPassword())
      }
    }

    Option(command.params).collect { case p: InstanceNameParams => p }.foreach { p =>
      if (p.instance == null) {
        config.instance.foreach(p.instance = _)
      }
      if (p.zookeepers == null) {
        config.zookeepers.foreach(p.zookeepers = _)
        if (p.zkTimeout == null) {
          config.zkTimeout.foreach(p.zkTimeout = _)
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
