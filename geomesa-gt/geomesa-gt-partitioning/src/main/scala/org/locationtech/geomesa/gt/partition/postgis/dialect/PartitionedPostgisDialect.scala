/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

import com.typesafe.scalalogging.StrictLogging
import org.geotools.data.postgis.PostGISDialect
import org.geotools.jdbc.JDBCDataStore
import org.locationtech.geomesa.gt.partition.postgis.dialect.functions.{LogCleaner, TruncateToPartition, TruncateToTenMinutes}
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures._
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables._
import org.locationtech.geomesa.gt.partition.postgis.dialect.triggers.{DeleteTrigger, InsertTrigger, UpdateTrigger, WriteAheadTrigger}
import org.locationtech.geomesa.utils.geotools.{Conversions, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

import java.sql.{Connection, DatabaseMetaData}
import scala.collection.mutable.ArrayBuffer

class PartitionedPostgisDialect(store: JDBCDataStore) extends PostGISDialect(store) with StrictLogging {

  // order of calls from JDBCDataStore during create schema:
  //  encodeCreateTable
  //  encodeTableName
  //  encodePrimaryKey
  //  encodeColumnName
  //  encodeColumnType
  //  encodePostColumnCreateTable
  //  encodePostCreateTable
  //  postCreateTable

  // order of calls during remove schema:
  //  preDropTable
  //  "DROP TABLE" + encodeTableName
  //  postDropTable

  // state for checking when we want to use the write_ahead table in place of the main view
  private val replaceTableName = new ThreadLocal[Boolean]() {
    override def initialValue(): Boolean = false
  }

  // filter out the partition tables from exposed feature types
  override def getDesiredTablesType: Array[String] = Array("VIEW")

  override def encodeTableName(raw: String, sql: StringBuffer): Unit = {
    sql.append(s""""${if (replaceTableName.get) { raw + WriteAheadTableSuffix } else { raw }}"""")
    replaceTableName.remove()
  }

  override def encodePrimaryKey(column: String, sql: StringBuffer): Unit = {
    encodeColumnName(null, column, sql)
    // make our primary key a string instead of the default integer
    sql.append(" character varying NOT NULL")
  }

  override def postCreateTable(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit = {
    val info = TypeInfo(schemaName, sft)

    super.postCreateTable(schemaName, sft, cx)

    implicit val ex: ExecutionContext = new ExecutionContext(cx)
    try {
      val commands = ArrayBuffer.empty[Sql]
      commands += WriteAheadTable
      commands += WriteAheadTrigger
      commands += PartitionTables
      commands += MainView
      commands += InsertTrigger
      commands += UpdateTrigger
      commands += DeleteTrigger
      commands += PrimaryKeyTable
      commands += AnalyzeQueueTable
      commands += SortQueueTable
      commands += TruncateToTenMinutes
      commands += TruncateToPartition
      commands += RollWriteAheadLog
      commands += PartitionWriteAheadLog
      commands += MergeWriteAheadPartitions
      if (info.partitions.maxPartitions.isDefined) {
        commands += DropAgedOffPartitions
      }
      commands += PartitionMaintenance
      commands += AnalyzePartitions
      commands += PartitionSort
      commands += LogCleaner

      commands.foreach(_.create(info))
    } finally {
      ex.close()
    }
  }

  /**
   * Re-create the PLGP/SQL functions and procedures associated with a feature type. This can be used
   * to 'upgrade in place' if the functions are changed.
   *
   * @param schemaName database schema, e.g. "public"
   * @param sft feature type
   * @param cx connection
   */
  def recreateFunctions(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit = {
    val info = TypeInfo(schemaName, sft)
    implicit val ex: ExecutionContext = new ExecutionContext(cx)
    try {
      val commands = ArrayBuffer.empty[Sql]
      commands += TruncateToTenMinutes
      commands += TruncateToPartition
      commands += RollWriteAheadLog
      commands += PartitionWriteAheadLog
      commands += MergeWriteAheadPartitions
      if (info.partitions.maxPartitions.isDefined) {
        commands += DropAgedOffPartitions
      }
      commands += PartitionMaintenance
      commands += AnalyzePartitions
      commands += PartitionSort
      commands += LogCleaner
      commands.foreach(_.drop(info))
      commands.foreach(_.create(info))
    } finally {
      ex.close()
    }
  }

  override def postCreateFeatureType(
      sft: SimpleFeatureType,
      metadata: DatabaseMetaData,
      schemaName: String,
      cx: Connection): Unit = {
    // normally views get set to read-only, override that here since we use triggers to delegate writes
    sft.getUserData.remove(JDBCDataStore.JDBC_READ_ONLY)
  }

  override def preDropTable(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit = {
    // due to the JDBCDataStore hard-coding "DROP TABLE" we have to redirect it away from the main view
    replaceTableName.set(true)
    val info = TypeInfo(schemaName, sft)

    implicit val ex: ExecutionContext = new ExecutionContext(cx)
    try {
      Seq(
        PartitionMaintenance,
        PartitionSort,
        LogCleaner,
        PrimaryKeyTable,
        MainView,
        InsertTrigger,
        UpdateTrigger,
        DeleteTrigger,
        PartitionTables
      ).foreach(_.drop(info))
    } finally {
      ex.close()
    }
  }

  override def postDropTable(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit = {
    val tables = Tables(sft, schemaName)
    // rename the sft so that configuration is applied to the write ahead table
    super.postDropTable(schemaName, SimpleFeatureTypes.renameSft(sft, tables.writeAhead.name.raw), cx)
  }
}

object PartitionedPostgisDialect {

  object Config extends Conversions {

    val IntervalHours                  = "pg.partitions.interval.hours"
    val MaxPartitions                  = "pg.partitions.max"
    val WriteAheadTableSpace           = "pg.partitions.tablespace.wa"
    val WriteAheadPartitionsTableSpace = "pg.partitions.tablespace.wa-partitions"
    val MainTableSpace                 = "pg.partitions.tablespace.main"
    val CronMinute                     = "pg.partitions.cron.minute"

    implicit class ConfigConversions(val sft: SimpleFeatureType) extends AnyVal {
      def getIntervalHours: Int = Option(sft.getUserData.get(IntervalHours)).map(int).getOrElse(6)
      def getMaxPartitions: Option[Int] = Option(sft.getUserData.get(MaxPartitions)).map(int)
      def getWriteAheadTableSpace: Option[String] = Option(sft.getUserData.get(WriteAheadTableSpace).asInstanceOf[String])
      def getWriteAheadPartitionsTableSpace: Option[String] = Option(sft.getUserData.get(WriteAheadPartitionsTableSpace).asInstanceOf[String])
      def getMainTableSpace: Option[String] = Option(sft.getUserData.get(MainTableSpace).asInstanceOf[String])
      def getCronMinute: Option[Int] = Option(sft.getUserData.get(CronMinute).asInstanceOf[String]).map(int)
    }
  }
}
