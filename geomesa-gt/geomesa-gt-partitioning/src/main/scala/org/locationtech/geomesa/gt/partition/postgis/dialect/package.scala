/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.Config
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.index.TemporalIndexCheck
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType

import java.io.Closeable
import java.sql.{Connection, PreparedStatement}
import scala.util.Try
import scala.util.control.NonFatal

package object dialect {

  private[dialect] val WriteAheadTableSuffix            = "_wa"
  private[dialect] val PartitionedWriteAheadTableSuffix = "_wa_partition"
  private[dialect] val PartitionedTableSuffix           = "_partition"
  private[dialect] val AnalyzeTableSuffix               = "_analyze_queue"
  private[dialect] val SortTableSuffix                  = "_sort_queue"

  /**
   * Wrapper to facilitate jdbc calls.
   *
   * Note: does not do any validation, sql must be sanitized by the caller.
   *
   * @param cx connection
   */
  class ExecutionContext(val cx: Connection) extends Closeable with StrictLogging {

    private val statement = cx.createStatement()
    private var rollback = false

    /**
     * Execute a sql command
     *
     * @param sql sql string
     */
    def execute(sql: String): Unit = {
      try {
        logger.debug(sql)
        statement.execute(sql)
      } catch {
        case NonFatal(e) => rollback = true; throw e
      }
    }

    /**
     * Execute an update statements
     *
     * @param sql sql string, with arguments denoted by '?'
     * @param args arguments to the update statement, must match the argument placeholders in the sql string
     */
    def executeUpdate(sql: String, args: Seq[Any]): Unit = {
      val ps: PreparedStatement = cx.prepareStatement(sql)
      try {
        var i = 1
        args.foreach { arg =>
          ps.setObject(i, arg)
          i += 1
        }
        logger.debug(s"$sql, ${args.mkString(", ")}")
        ps.executeUpdate()
      } catch {
        case NonFatal(e) => rollback = true; throw e
      } finally {
        ps.close()
      }
    }

    override def close(): Unit = {
      if (!cx.getAutoCommit) {
        if (rollback) {
          Try(cx.rollback())
        } else {
          cx.commit()
        }
      }
      statement.close()
    }
  }

  /**
   * All the info for a feature type used by the partitioning scheme
   *
   * @param schema database schema (e.g. "public")
   * @param name feature type name
   * @param tables table names
   * @param cols columns
   * @param partitions partition config
   */
  case class TypeInfo(schema: String, name: String, tables: Tables, cols: Columns, partitions: PartitionInfo)

  object TypeInfo {
    def apply(schema: String, sft: SimpleFeatureType): TypeInfo = {
      val tables = Tables(sft, schema)
      val columns = Columns(sft)
      val partitions = PartitionInfo(sft)
      TypeInfo(schema, sft.getTypeName, tables, columns, partitions)
    }
  }

  /**
   * Tables used by the partitioning system
   *
   * @param schema database schema name (e.g. "public")
   * @param view primary view of all partitions
   * @param writeAhead write ahead table
   * @param writeAheadPartitions recent partitions table
   * @param mainPartitions main partitions table
   * @param analyzeQueue analyze queue table
   * @param sortQueue sort queue table
   */
  case class Tables(
      schema: String,
      view: TableConfig,
      writeAhead: TableConfig,
      writeAheadPartitions: TableConfig,
      mainPartitions: TableConfig,
      analyzeQueue: TableConfig,
      sortQueue: TableConfig)

  object Tables {

    import Config.ConfigConversions

    def apply(sft: SimpleFeatureType, schema: String): Tables = {
      val view = TableConfig(schema, sft.getTypeName, None)
      // we disable autovacuum for write ahead tables, as they are transient and get dropped fairly quickly
      val writeAhead = TableConfig(schema, view.name.raw + WriteAheadTableSuffix, sft.getWriteAheadTableSpace, vacuum = false)
      val writeAheadPartitions = TableConfig(schema, view.name.raw + PartitionedWriteAheadTableSuffix, sft.getWriteAheadPartitionsTableSpace, vacuum = false)
      val mainPartitions = TableConfig(schema, view.name.raw + PartitionedTableSuffix, sft.getMainTableSpace)
      val analyzeQueue = TableConfig(schema, view.name.raw + AnalyzeTableSuffix, sft.getMainTableSpace)
      val sortQueue = TableConfig(schema, view.name.raw + SortTableSuffix, sft.getMainTableSpace)
      Tables(schema, view, writeAhead, writeAheadPartitions, mainPartitions, analyzeQueue, sortQueue)
    }
  }

  /**
   * Table configuration
   *
   * @param name table name
   * @param tablespace table space
   * @param storage storage opts (auto vacuum)
   */
  case class TableConfig(name: TableName, tablespace: TableSpace, storage: String)

  object TableConfig {
    def apply(schema: String, name: String, tablespace: Option[String], vacuum: Boolean = true): TableConfig = {
      val storage = if (vacuum) { "" } else { " WITH (autovacuum_enabled = false)" }
      TableConfig(TableName(schema, name), TableSpace(tablespace), storage)
    }
  }

  /**
   * Table name
   *
   * @param full fully qualified and escaped name
   * @param unqualified escaped name without the schema
   * @param raw unqualified name without escaping
   */
  case class TableName(full: String, unqualified: String, raw: String)

  object TableName {
    def apply(schema: String, raw: String): TableName = TableName(s""""$schema"."$raw"""", s""""$raw"""", raw)
  }

  sealed trait TableSpace {

    /**
     * Name of the table space
     *
     * @return
     */
    def name: String

    /**
     * Sql to specify the tablespace when creating a table
     *
     * @return
     */
    def table: String

    /**
     * Sql to specify the table space when creating an index
     *
     * @return
     */
    def index: String
  }

  object TableSpace {

    def apply(tablespace: Option[String]): TableSpace = {
      tablespace match {
        case None => DefaultTableSpace
        case Some(t) => NamedTableSpace(t)
      }
    }

    case object DefaultTableSpace extends TableSpace {
      override val name: String = ""
      override val table: String = ""
      override val index: String = ""
    }

    case class NamedTableSpace(name: String) extends TableSpace {
      override val table: String = s" TABLESPACE $name"
      override val index: String = s" USING INDEX TABLESPACE $name"
    }
  }

  /**
   * Columns names for the feature type
   *
   * @param dtg primary dtg column
   * @param geom primary geometry column
   * @param geoms all geometry cols (including primary)
   * @param indexed any indexed columns
   * @param all all columns
   */
  case class Columns(
      dtg: ColumnName,
      geom: ColumnName,
      geoms: Seq[ColumnName],
      indexed: Seq[ColumnName],
      all: Seq[ColumnName]
    )

  object Columns {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConverters._

    def apply(original: SimpleFeatureType): Columns = {
      val sft = SimpleFeatureTypes.copy(original)
      TemporalIndexCheck.validateDtgField(sft)
      val dtg = sft.getDtgField.map(ColumnName.apply).getOrElse {
        throw new IllegalArgumentException("Must include a date-type attribute when using a partitioned store")
      }
      val geom = Option(sft.getGeomField).map(ColumnName.apply).getOrElse {
        throw new IllegalArgumentException("Must include a geometry-type attribute when using a partitioned store")
      }
      val geoms = sft.getAttributeDescriptors.asScala.collect { case d: GeometryDescriptor => ColumnName(d) }
      // TODO we could use covering indices, but BRIN indices don't support them
      //  val includes =
      //    Seq(dtg) ++ sft.getAttributeDescriptors.asScala.collect { case d if d.isIndexValue() => ColumnName(d) }
      //    .distinct
      def indexed(d: AttributeDescriptor): Boolean = d.getLocalName != geom.raw && d.getLocalName != dtg.raw &&
          Option(d.getUserData.get(AttributeOptions.OptIndex).asInstanceOf[String]).exists { i =>
            Seq("true", IndexCoverage.FULL, IndexCoverage.JOIN).map(_.toString).exists(_.equalsIgnoreCase(i))
          }

      val indices = sft.getAttributeDescriptors.asScala.collect { case d if indexed(d) => ColumnName(d) }
      val all = sft.getAttributeDescriptors.asScala.map(ColumnName.apply)
      Columns(dtg, geom, geoms, indices, all)
    }
  }

  /**
   * Column name
   *
   * @param name escaped name
   * @param raw unescaped name
   */
  case class ColumnName(name: String, raw: String)

  object ColumnName {
    def apply(d: AttributeDescriptor): ColumnName = apply(d.getLocalName)
    def apply(raw: String): ColumnName = ColumnName(s""""$raw"""", raw)
  }

  /**
   * Partition config
   *
   * @param hoursPerPartition number of hours to keep in each partition, must be a divisor of 24
   * @param maxPartitions max number of partitions to keep
   * @param cronMinute minute of each 10 minute chunk that partition maintenance will run
   */
  case class PartitionInfo(hoursPerPartition: Int, maxPartitions: Option[Int], cronMinute: Option[Int])

  object PartitionInfo {

    import Config.ConfigConversions

    def apply(sft: SimpleFeatureType): PartitionInfo = {
      val hours = sft.getIntervalHours
      require(hours > 0 && hours <= 24, s"Partition interval must be between 1 and 24 hours: $hours hours")
      require(24 % hours == 0, s"Partition interval must be a divisor of 24 hours: $hours hours")
      val cronMinute = sft.getCronMinute
      require(cronMinute.forall(m => m >= 0 && m < 9), s"Cron minute must be between 0 and 8: ${cronMinute.orNull}")
      PartitionInfo(hours, sft.getMaxPartitions, cronMinute)
    }
  }

  /**
   * Trait for modelling sql objects
   */
  trait Sql {

    /**
     * Execute sql defined by this class
     *
     * @param info type info
     */
    def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit

    /**
     * Drop things created by this class
     *
     * @param info type info
     */
    def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit
  }

  /**
   * Trait for modeling a list of sql statements
   */
  trait SqlStatements extends Sql {

    override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit =
      createStatements(info).foreach(ex.execute)

    override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit =
      dropStatements(info).foreach(ex.execute)

    /**
     * The sql defined by this class
     *
     * @param info type info
     * @return
     */
    protected def createStatements(info: TypeInfo): Seq[String]

    /**
     * The sql to drop things created by this class
     *
     * @param info type info
     * @return
     */
    protected def dropStatements(info: TypeInfo): Seq[String]
  }

  /**
   * A function definition
   */
  trait SqlFunction extends SqlStatements {

    /**
     * The name of the function
     *
     * @param info type info
     * @return
     */
    def name(info: TypeInfo): String

    override protected def dropStatements(info: TypeInfo): Seq[String] =
      Seq(s"""DROP FUNCTION IF EXISTS "${name(info)}";""")
  }

  /**
   * A procedure definition
   */
  trait SqlProcedure extends SqlStatements {

    /**
     * The name of the procedure
     *
     * @param info type info
     * @return
     */
    def name(info: TypeInfo): String

    override protected def dropStatements(info: TypeInfo): Seq[String] =
      Seq(s"""DROP PROCEDURE IF EXISTS "${name(info)}";""")
  }

  /**
   * A cron'd function or procedure
   */
  trait CronSchedule extends SqlStatements {

    /**
     * The name of the cron job in the cron.job tables
     *
     * @param info type info
     * @return
     */
    def jobName(info: TypeInfo): String

    /**
     * The cron schedule, e.g. "* * * * *"
     *
     * @param info type info
     * @return
     */
    protected def schedule(info: TypeInfo): String

    /**
     * The statement to execute for each cron run
     *
     * @param info type info
     * @return
     */
    protected def invocation(info: TypeInfo): String

    override protected def createStatements(info: TypeInfo): Seq[String] =
      Seq(s"SELECT cron.schedule('${jobName(info)}', '${schedule(info)}', '${invocation(info)}');")

    abstract override protected def dropStatements(info: TypeInfo): Seq[String] =
      Seq(s"SELECT cron.unschedule('${jobName(info)}');") ++ super.dropStatements(info)
  }

}
