/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.data

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.geotools.data.{DataStore, DefaultTransaction}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.jdbc.JDBCDataStore
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.GeoToolsDataStoreParams
import org.locationtech.geomesa.geotools.tools.data.GeoToolsUpdateSchemaCommand.GeoToolsUpdateSchemaParams
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
import org.locationtech.geomesa.tools.utils.{NoopParameterSplitter, Prompt}
import org.locationtech.geomesa.tools.{Command, DataStoreCommand, OptionalForceParam, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.io.WithClose

import java.io.IOException
import java.util.Collections

class GeoToolsUpdateSchemaCommand extends DataStoreCommand[DataStore] with GeoToolsDataStoreCommand {

  import scala.collection.JavaConverters._

  override val name = "update-schema"
  override val params = new GeoToolsUpdateSchemaParams()

  override def execute(): Unit = withDataStore(update)

  protected def update(ds: DataStore): Unit = {

    // ensure we have an operation
    params.validate().foreach(e => throw e)

    val sft = try { ds.getSchema(params.featureName) } catch { case _: IOException => null }
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist in the data store")
    }

    var n = 0
    // numbering for our prompts
    def number: Int = { n += 1; n }
    val prompts = new StringBuilder()

    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)

    val updated = builder.buildFeatureType()
    updated.getUserData.putAll(sft.getUserData)

    if (!params.userData.isEmpty) {
      params.userData.asScala.foreach { ud =>
        ud.split(":", 2) match {
          case Array(k, v) =>
            updated.getUserData.put(k, v) match {
              case null => prompts.append(s"\n  $number: Adding user data: '$k=$v'")
              case old  => prompts.append(s"\n  $number: Updating user data: '$k=$v' (was '$old')")
            }
          case _ => throw new ParameterException(s"Invalid user data entry - expected 'key:value': $ud")
        }
      }
    }

    Command.user.info(s"Preparing to update schema '${sft.getTypeName}':$prompts")
    if (params.force || Prompt.confirm("Continue (y/n)? ")) {
      Command.user.info("Updating, please wait...")
      ds match {
        case jdbc: JDBCDataStore =>
          // update schema is not implemented for JDBC stores
          val partitioning =
            Option(jdbc.dialect).collect {
              case d: PartitionedPostgisDialect => d
              case d: PartitionedPostgisPsDialect => d
            }

          val dialect = partitioning.getOrElse {
            throw new RuntimeException(
              "JDBCDataStore does not support schema updates unless using 'dbtype=postgis-partitioned'")
          }

          WithClose(new DefaultTransaction()) { tx =>
            WithClose(jdbc.getConnection(tx)) { cx =>
              dialect.postCreateTable(jdbc.getDatabaseSchema, updated, cx)
            }
          }

        case _ =>
          try { ds.updateSchema(sft.getTypeName, updated) } catch {
            case _: UnsupportedOperationException =>
              throw new RuntimeException(s"${ds.getClass.getSimpleName} does not support schema updates")
          }
      }

      Command.user.info("Update complete")
    }
  }
}

object GeoToolsUpdateSchemaCommand {

  @Parameters(commandDescription = "Update a feature type")
  class GeoToolsUpdateSchemaParams extends RequiredTypeNameParam with OptionalForceParam with GeoToolsDataStoreParams {

    @Parameter(names = Array("--add-user-data"),
      description = "Add a new entry or update an existing entry in the feature type user data, delineated with a colon (:)",
      splitter = classOf[NoopParameterSplitter])
    var userData: java.util.List[String] = Collections.emptyList()

    def validate(): Option[ParameterException] = {
      if (userData.isEmpty) {
        Some(new ParameterException("Please specify an update operation"))
      } else {
        None
      }
    }
  }
}
