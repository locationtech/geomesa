/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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
import java.util.regex.Pattern
import scala.util.Try
import scala.util.control.NonFatal

package object dialect {

  private val DoubleQuote = "\""
  private val NamePattern = Pattern.compile(DoubleQuote, Pattern.LITERAL)

  private val SingleQuote = "'"
  private val LiteralPattern = Pattern.compile(SingleQuote, Pattern.LITERAL)

  private[dialect] val WriteAheadTableSuffix            = SqlLiteral("_wa")
  private[dialect] val PartitionedWriteAheadTableSuffix = SqlLiteral("_wa_partition")
  private[dialect] val PartitionedTableSuffix           = SqlLiteral("_partition")
  private[dialect] val SpillTableSuffix                 = SqlLiteral("_spill")
  private[dialect] val AnalyzeTableSuffix               = SqlLiteral("_analyze_queue")
  private[dialect] val SortTableSuffix                  = SqlLiteral("_sort_queue")

  /**
   * Escape a sql identifier
   *
   * @param name name
   * @return
   */
  def escape(name: String): String = quoteAndReplace(name, NamePattern, DoubleQuote)

  /**
   * Escape a sql identifier, creating the identifier based on different parts, concatenated
   * together with `_`
   *
   * @param name0 first part
   * @param name1 second part
   * @param nameN additional parts
   * @return
   */
  def escape(name0: String, name1: String, nameN: String*): String =
    escape((Seq(name0, name1) ++ nameN).mkString("_"))

  /**
   * Quote a literal string
   *
   * @param string string literal
   * @return
   */
  def literal(string: String): String = quoteAndReplace(string, LiteralPattern, SingleQuote)

  /**
   * Quote a literal string, creating the string based on different parts, concatenated
   * together with `_`
   *
   * @param string0 first value
   * @param string1 seconds value
   * @param stringN additional values
   * @return
   */
  def literal(string0: String, string1: String, stringN: String*): String =
    literal((Seq(string0, string1) ++ stringN).mkString("_"))

  private def quoteAndReplace(string: String, pattern: Pattern, quote: String): String =
    s"$quote${pattern.matcher(string).replaceAll(quote + quote)}$quote"

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
   * @param typeName feature type name
   * @param tables table names
   * @param cols columns
   * @param partitions partition config
   * @param userData feature type user data
   */
  case class TypeInfo(
      schema: SchemaName,
      typeName: String,
      tables: Tables,
      cols: Columns,
      partitions: PartitionInfo,
      userData: Map[String, String] = Map.empty)

  object TypeInfo {

    import scala.collection.JavaConverters._

    def apply(schema: String, sft: SimpleFeatureType): TypeInfo = {
      val tables = Tables(sft, schema)
      val columns = Columns(sft)
      val partitions = PartitionInfo(sft)
      val userData = sft.getUserData.asScala.map { case (k, v) => String.valueOf(k) -> String.valueOf(v) }
      TypeInfo(SchemaName(schema), sft.getTypeName, tables, columns, partitions, userData.toMap)
    }
  }

  case class SchemaName(quoted: String, raw: String) extends SqlIdentifier

  object SchemaName {
    def apply(schema: String): SchemaName = SchemaName(escape(schema), schema)
  }

  /**
   * Tables used by the partitioning system
   *
   * @param view primary view of all partitions
   * @param writeAhead write ahead table
   * @param writeAheadPartitions recent partitions table
   * @param mainPartitions main partitions table
   * @param analyzeQueue analyze queue table
   * @param sortQueue sort queue table
   */
  case class Tables(
      view: TableConfig,
      writeAhead: TableConfig,
      writeAheadPartitions: TableConfig,
      mainPartitions: TableConfig,
      spillPartitions: TableConfig,
      analyzeQueue: TableConfig,
      sortQueue: TableConfig)

  object Tables {

    import Config.ConfigConversions

    def apply(sft: SimpleFeatureType, schema: String): Tables = {
      val view = TableConfig(schema, sft.getTypeName, None)
      // we disable autovacuum for write ahead tables, as they are transient and get dropped fairly quickly
      val writeAhead = TableConfig(schema, view.name.raw + WriteAheadTableSuffix.raw, sft.getWriteAheadTableSpace, vacuum = false)
      val writeAheadPartitions = TableConfig(schema, view.name.raw + PartitionedWriteAheadTableSuffix.raw, sft.getWriteAheadPartitionsTableSpace, vacuum = false)
      val mainPartitions = TableConfig(schema, view.name.raw + PartitionedTableSuffix.raw, sft.getMainTableSpace)
      val spillPartitions = TableConfig(schema, view.name.raw + SpillTableSuffix.raw, sft.getMainTableSpace)
      val analyzeQueue = TableConfig(schema, view.name.raw + AnalyzeTableSuffix.raw, sft.getMainTableSpace)
      val sortQueue = TableConfig(schema, view.name.raw + SortTableSuffix.raw, sft.getMainTableSpace)
      Tables(view, writeAhead, writeAheadPartitions, mainPartitions, spillPartitions, analyzeQueue, sortQueue)
    }
  }

  /**
   * Table configuration
   *
   * @param name table name
   * @param tablespace table space
   * @param storage storage opts (auto vacuum)
   */
  case class TableConfig(name: TableIdentifier, tablespace: Option[TableSpace], storage: Storage)

  object TableConfig {
    def apply(schema: String, name: String, tablespace: Option[String], vacuum: Boolean = true): TableConfig = {
      val storage = if (vacuum) { Storage.Nothing } else { Storage.AutoVacuumDisabled }
      val ts = tablespace.collect { case t if t.nonEmpty => TableSpace(t) }
      TableConfig(TableIdentifier(schema, name), ts, storage)
    }
  }

  sealed trait Storage {
    def opts: String
  }

  object Storage {
    case object Nothing extends Storage {
      override val opts: String = ""
    }
    case object AutoVacuumDisabled extends Storage {
      override val opts: String = " WITH (autovacuum_enabled = false)"
    }
  }

  /**
   * Tablespace
   *
   * @param quoted escaped name
   * @param raw raw name
   */
  case class TableSpace(quoted: String, raw: String) extends SqlIdentifier

  object TableSpace {
    def apply(tablespace: String): TableSpace = TableSpace(escape(tablespace), tablespace)
  }

  /**
   * Table name
   *
   * @param qualified fully qualified and escaped name
   * @param quoted escaped name without the schema
   * @param raw unqualified name without escaping
   */
  case class TableIdentifier(qualified: String, quoted: String, raw: String) extends QualifiedSqlIdentifier {
    def asRegclass: String = s"${literal(qualified)}::regclass"
  }

  object TableIdentifier {
    def apply(schema: String, raw: String): TableIdentifier =
      TableIdentifier(s"${escape(schema)}.${escape(raw)}", escape(raw), raw)
  }

  /**
   * A table name, but without any schema information
   *
   * @param quoted escaped name
   * @param raw name without escaping
   */
  case class TableName(quoted: String, raw: String) extends SqlIdentifier

  object TableName {
    def apply(name: String): TableName  = TableName(escape(name), name)
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
      Columns(dtg, geom, geoms.toSeq, indices.toSeq, all.toSeq)
    }
  }

  /**
   * Column name
   *
   * @param quoted escaped name
   * @param raw unescaped name
   */
  case class ColumnName(quoted: String, raw: String) extends SqlIdentifier

  object ColumnName {
    def apply(d: AttributeDescriptor): ColumnName = apply(d.getLocalName)
    def apply(raw: String): ColumnName = ColumnName(escape(raw), raw)
  }

  /**
   * Partition config
   *
   * @param hoursPerPartition number of hours to keep in each partition, must be a divisor of 24
   * @param pagesPerRange number of pages to add to each range in the BRIN index
   * @param maxPartitions max number of partitions to keep
   * @param cronMinute minute of each 10 minute chunk that partition maintenance will run
   */
  case class PartitionInfo(
      hoursPerPartition: Int,
      pagesPerRange: Int,
      maxPartitions: Option[Int],
      cronMinute: Option[Int]
    )

  object PartitionInfo {

    import Config.ConfigConversions

    def apply(sft: SimpleFeatureType): PartitionInfo = {
      val hours = sft.getIntervalHours
      require(hours > 0 && hours <= 24, s"Partition interval must be between 1 and 24 hours: $hours hours")
      require(24 % hours == 0, s"Partition interval must be a divisor of 24 hours: $hours hours")
      val cronMinute = sft.getCronMinute
      require(cronMinute.forall(m => m >= 0 && m < 9), s"Cron minute must be between 0 and 8: ${cronMinute.orNull}")
      val pagesPerRange = sft.getPagesPerRange
      require(pagesPerRange >= 0, s"Pages per range but be a positive number: $pagesPerRange")
      PartitionInfo(hours, pagesPerRange, sft.getMaxPartitions, cronMinute)
    }
  }

  /**
   * Trigger name
   *
   * @param quoted escaped name
   * @param raw raw name
   */
  case class TriggerName(quoted: String, raw: String) extends SqlIdentifier

  object TriggerName {
    def apply(name: String): TriggerName = TriggerName(escape(name), name)
  }

  /**
   * Function name
   *
   * @param quoted escaped name
   * @param raw raw name
   */
  case class FunctionName(quoted: String, raw: String) extends SqlIdentifier

  object FunctionName {
    def apply(name: String): FunctionName = FunctionName(escape(name), name)
  }

  /**
   * Identifier for sql objects
   */
  trait SqlIdentifier {

    /**
     * Escaped and quoted name
     *
     * @return
     */
    def quoted: String

    /**
     * Raw, unqualified name, without escaping
     *
     * @return
     */
    def raw: String

    /**
     * Get as a quoted literal instead of an identifier
     *
     * @return
     */
    def asLiteral: String = literal(raw)
  }

  /**
   * Identifier for qualified sql objects (i.e. with schemas)
   */
  trait QualifiedSqlIdentifier extends SqlIdentifier {

    /**
     * Fully qualified and escaped name
     *
     * @return
     */
    def qualified: String
  }

  /**
   * A sql literal
   *
   * @param quoted escaped and quoted value
   * @param raw raw value
   */
  case class SqlLiteral(quoted: String, raw: String)

  object SqlLiteral {
    def apply(value: String): SqlLiteral = SqlLiteral(literal(value), value)
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
   * A procedure definition
   */
  trait SqlProcedure extends SqlStatements {

    /**
     * The name of the procedure
     *
     * @param info type info
     * @return
     */
    def name(info: TypeInfo): FunctionName

    override protected def dropStatements(info: TypeInfo): Seq[String] =
      Seq(s"DROP PROCEDURE IF EXISTS ${name(info).quoted};")
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
    def name(info: TypeInfo): FunctionName

    override protected def dropStatements(info: TypeInfo): Seq[String] =
      Seq(s"DROP FUNCTION IF EXISTS ${name(info).quoted};")
  }

  /**
   * A table trigger with an associated function definition
   */
  trait SqlTriggerFunction extends SqlFunction {

    /**
     * Name of the trigger
     *
     * @param info type info
     * @return
     */
    def triggerName(info: TypeInfo): TriggerName = TriggerName(s"${name(info).raw}_trigger")

    /**
     * Table that the trigger is applied to
     *
     * @param info type info
     * @return
     */
    protected def table(info: TypeInfo): TableIdentifier

    /**
     * Triggered action (e.g. "INSTEAD OF INSERT", "BEFORE INSERT", "INSTEAD OF DELETE", etc)
     *
     * @return
     */
    protected def action: String

    override protected def createStatements(info: TypeInfo): Seq[String] = {
      // there is no "create or replace" for triggers so we have to wrap it in a check
      val tgName = triggerName(info)
      val create =
        s"""DO $$$$
           |BEGIN
           |  IF NOT EXISTS (SELECT FROM pg_trigger WHERE tgname = ${tgName.asLiteral}) THEN
           |    CREATE TRIGGER ${tgName.quoted}
           |      $action ON ${table(info).qualified}
           |      FOR EACH ROW EXECUTE PROCEDURE ${name(info).quoted}();
           |  END IF;
           |END$$$$;""".stripMargin
      Seq(create)
    }

    override protected def dropStatements(info: TypeInfo): Seq[String] =
      Seq(dropTrigger(info)) ++ super.dropStatements(info)

    private def dropTrigger(info: TypeInfo): String =
      s"DROP TRIGGER IF EXISTS ${triggerName(info).quoted} ON ${table(info).qualified};"
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
    def jobName(info: TypeInfo): SqlLiteral

    /**
     * The cron schedule, e.g. "* * * * *"
     *
     * @param info type info
     * @return
     */
    protected def schedule(info: TypeInfo): SqlLiteral

    /**
     * The statement to execute for each cron run
     *
     * @param info type info
     * @return
     */
    protected def invocation(info: TypeInfo): SqlLiteral

    override protected def createStatements(info: TypeInfo): Seq[String] = {
      val jName = jobName(info).quoted
      val create =
        s"""DO $$$$
           |BEGIN
           |${unscheduleSql(jName)}
           |  PERFORM cron.schedule($jName, ${schedule(info).quoted}, ${invocation(info).quoted});
           |END$$$$;""".stripMargin
      Seq(create)
    }

    abstract override protected def dropStatements(info: TypeInfo): Seq[String] = {
      val jName = jobName(info).quoted
      val drop =
        s"""DO $$$$
           |BEGIN
           |${unscheduleSql(jName)}
           |END$$$$;""".stripMargin
      Seq(drop) ++ super.dropStatements(info)
    }

    protected def unscheduleSql(quotedName: String): String =
      s"""  IF EXISTS (SELECT FROM cron.job WHERE jobname = $quotedName) THEN
         |    PERFORM cron.unschedule($quotedName);
         |  END IF;""".stripMargin
  }

  /**
   * Uses a postgres advisory lock to synchronize create and drop statements
   */
  trait AdvisoryLock extends Sql {

    /**
     * The lock id associated with this object
     *
     * @return
     */
    protected def lockId: Long

    private def lock: String = s"SELECT pg_advisory_lock($lockId);"
    private def unlock: String = s"SELECT pg_advisory_unlock($lockId);"

    abstract override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
      ex.execute(lock)
      try { super.create(info) } finally {
        ex.execute(unlock)
      }
    }

    abstract override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
      ex.execute(lock)
      try { super.drop(info) } finally {
        ex.execute(unlock)
      }
    }

  }
}
