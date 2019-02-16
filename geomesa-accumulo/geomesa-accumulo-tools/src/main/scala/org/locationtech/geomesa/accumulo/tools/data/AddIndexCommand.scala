/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import com.beust.jcommander.{Parameter, Parameters}
import org.apache.hadoop.util.ToolRunner
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.data.AddIndexCommand.AddIndexParameters
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, GeoMesaFeatureIndexFactory}
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.jobs.accumulo.index.{WriteIndexArgs, WriteIndexJob}
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{Configs, InternalConfigs}
import org.locationtech.geomesa.utils.index.IndexMode

import scala.util.control.NonFatal

/**
  *
  * 1. Add the new index in write-only mode
  * 2. Pause and indicate that the user should bounce live ingestion to pick up the changes -
  *    after this it will be writing to both the new and old index
  * 3. Migrate data through a m/r job, with an optional CQL filter for what gets migrated
  * 4. Turn old index off, put new index in read/write mode
  * 5. Pause and indicate that the user should bounce live ingestion again
  */
class AddIndexCommand extends AccumuloDataStoreCommand {

  override val name = "add-index"
  override val params = new AddIndexParameters

  override def execute(): Unit = {
    // We instantiate the class at runtime to avoid classpath dependencies from commands that are not being used.
    new AddIndexCommandExecutor(params).run()
  }
}

object AddIndexCommand {

  @Parameters(commandDescription = "Add or update indices for an existing GeoMesa feature type")
  class AddIndexParameters extends AccumuloDataStoreParams with RequiredTypeNameParam with OptionalCqlFilterParam {
    @Parameter(names = Array("--index"), description = "Name of index(es) to add - comma-separate or use multiple flags", required = true)
    var indexNames: java.util.List[String] = _

    @Parameter(names = Array("--no-back-fill"), description = "Do not copy any existing data into the new index")
    var noBackFill: java.lang.Boolean = _
  }
}

class AddIndexCommandExecutor(override val params: AddIndexParameters) extends Runnable with AccumuloDataStoreCommand {

  import org.locationtech.geomesa.index.metadata.GeoMesaMetadata.ATTRIBUTES_KEY
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name = ""

  override def execute(): Unit = {}

  override def run(): Unit = withDataStore(addIndex)

  def addIndex(ds: AccumuloDataStore): Unit  = {

    import scala.collection.JavaConversions._

    val sft = Option(ds.getSchema(params.featureName)).map(SimpleFeatureTypes.mutable).orNull
    require(sft != null, s"Schema '${params.featureName}' does not exist in the specified data store")

    val indices = {
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(sft)
      val copy = builder.buildFeatureType()
      copy.getUserData.putAll(sft.getUserData)
      copy.getUserData.remove(InternalConfigs.INDEX_VERSIONS)
      copy.getUserData.put(Configs.ENABLED_INDICES, params.indexNames.mkString(","))
      GeoMesaFeatureIndexFactory.indices(copy)
    }

    val existing = ds.manager.indices(sft)
    require(indices.map(GeoMesaFeatureIndex.identifier).forall(i => !existing.map(_.identifier).contains(i)),
      s"Requested indices already exist: ${existing.map(_.identifier).mkString("[", "][", "]")}")

    val toDisable = indices.flatMap { i =>
      existing.find(e => e.name == i.name && e.attributes == i.attributes).map { e =>
        (i, IndexId(e.name, e.version, e.attributes, e.mode))
      }
    }

    if (toDisable.nonEmpty) {
      if (!Prompt.confirm("The following index versions will be replaced: " +
        toDisable.map { case (n, o) => s"[$o] by [${GeoMesaFeatureIndex.identifier(n)}]" }.mkString(", ") +
        "Continue? (y/n): ")) {
        return
      }
    }
    if (!Prompt.confirm("If you are ingesting streaming data, you will be required to restart " +
      "the streaming ingestion when prompted. Continue? (y/n): ")) {
      return
    }

    // write a backup meta-data entry in case the process fails part-way
    val backupKey = s"$ATTRIBUTES_KEY.bak"
    ds.metadata.insert(sft.getTypeName, backupKey, ds.metadata.readRequired(sft.getTypeName, ATTRIBUTES_KEY))

    val toKeep = sft.getIndices.filter(i => !toDisable.map(_._2).contains(i))

    if (params.noBackFill != null && params.noBackFill.booleanValue()) {
      Command.user.info("Adding new indices and disabling old ones")
      sft.setIndices(indices ++ toKeep)
      ds.updateSchema(sft.getTypeName, sft)
    } else {
      Command.user.info("Adding new indices in write-only mode")

      // add new index in write-only mode
      sft.setIndices(indices.map(_.copy(mode = IndexMode.Write)) ++ sft.getIndices)
      ds.updateSchema(sft.getTypeName, sft)

      // wait for the user to bounce ingestion
      Prompt.acknowledge("Indices have been added in write-only mode. To pick up the changes, " +
        "please bounce any streaming ingestion. Once ingestion has resumed, press 'enter' to continue.")

      // run migration job
      Command.user.info("Running index back-fill job")

      val args = new WriteIndexArgs(Array.empty)
      args.inZookeepers = params.zookeepers
      args.inInstanceId = params.instance
      args.inUser       = params.user
      args.inPassword   = params.password
      args.inTableName  = params.catalog
      args.inFeature    = params.featureName
      args.inCql        = Option(params.cqlFilter).map(ECQL.toCQL).orNull
      args.indexNames.addAll(indices.map(GeoMesaFeatureIndex.identifier))

      val libjars = Some(AccumuloJobUtils.defaultLibJars, AccumuloJobUtils.defaultSearchPath)
      val result = try { ToolRunner.run(new WriteIndexJob(libjars), args.unparse()) } catch {
        case NonFatal(e) => Command.user.error("Error running back-fill job:", e); -1
      }

      def setReadWrite(): Unit = {
        Command.user.info("Setting index to read-write mode and disabling old indices")
        // set new indices to read-write and turn off disabled indices
        sft.setIndices(indices.map(_.copy(mode = IndexMode.ReadWrite)) ++ toKeep)
        Command.user.info(sft.getIndices.toString)
        ds.updateSchema(sft.getTypeName, sft)
      }

      if (result == 0) {
        setReadWrite()
      } else {
        var response: String = null
        do {
          response = Prompt.read("Index back-fill job failed. You may:\n" +
            "  1. Switch the indices to read-write mode without existing data (you may manually back-fill later)\n" +
            "  2. Roll-back index creation\n" +
            "Select an option: ")
        } while (response != "1" && response != "2")
        response match {
          case "1" => setReadWrite()
          case "2" =>
            val bak = ds.metadata.readRequired(sft.getTypeName, backupKey)
            ds.metadata.insert(sft.getTypeName, ATTRIBUTES_KEY, bak)
        }
      }
    }

    // final bounce
    Command.user.info("Operation complete. Please bounce any streaming ingestion to pick up the changes.")
  }
}
