/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

import com.typesafe.scalalogging.StrictLogging
import org.geotools.api.data.Query
import org.geotools.api.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.data.postgis.PostGISDialect
import org.geotools.geometry.jts._
import org.geotools.jdbc.JDBCDataStore
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.gt.partition.postgis.dialect.filter.SplitFilterVisitor
import org.locationtech.geomesa.gt.partition.postgis.dialect.functions.{LogCleaner, TruncateToPartition, TruncateToTenMinutes}
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures._
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables._
import org.locationtech.geomesa.gt.partition.postgis.dialect.triggers.{DeleteTrigger, InsertTrigger, UpdateTrigger, WriteAheadTrigger}
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.utils.geotools.PrimitiveConversions.Conversion
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.geotools.{Conversions, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.locationtech.jts.geom._

import java.sql.{Connection, DatabaseMetaData, ResultSet, Types}
import scala.util.Try
import scala.util.control.NonFatal

class PartitionedPostgisDialect(store: JDBCDataStore) extends PostGISDialect(store) with StrictLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

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
  //  "DROP TABLE " + encodeTableName
  //  postDropTable

  // state for checking when we want to use the write_ahead table in place of the main view
  private val dropping = new ThreadLocal[Boolean]() {
    override def initialValue(): Boolean = false
  }

  private val interceptors = {
    val factory = QueryInterceptorFactory(store)
    sys.addShutdownHook(CloseWithLogging(factory)) // we don't have any API hooks to dispose of things...
    factory
  }

  /**
   * Re-create the PLPG/SQL procedures associated with a feature type. This can be used
   * to 'upgrade in place' if the code is changed.
   *
   * @param schemaName database schema, e.g. "public"
   * @param sft feature type
   * @param cx connection
   */
  def upgrade(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit =
    postCreateTable(schemaName, sft, cx)

  override def getDesiredTablesType: Array[String] = Array("VIEW", "TABLE")

  // filter out the partition tables from exposed feature types
  override def includeTable(schemaName: String, tableName: String, cx: Connection): Boolean = {
    super.includeTable(schemaName, tableName, cx) && !PartitionedPostgisDialect.IgnoredTables.contains(tableName) && {
      val metadata = cx.getMetaData
      val schemaPattern = store.escapeNamePattern(metadata, schemaName)
      val tablePattern = store.escapeNamePattern(metadata, tableName)
      val rs = metadata.getTables(null, schemaPattern, tablePattern, Array("VIEW"))
      try { rs.next() } finally {
        rs.close()
      }
    }
  }

  override def encodeCreateTable(sql: StringBuffer): Unit =
    sql.append("CREATE TABLE IF NOT EXISTS ")

  override def encodeTableName(raw: String, sql: StringBuffer): Unit = {
    if (dropping.get) {
      // redirect from the view as DROP TABLE is hard-coded by the JDBC data store,
      // and cascade the drop to delete any write ahead partitions
      sql.append(escape(raw + WriteAheadTableSuffix.raw)).append(" CASCADE")
      dropping.remove()
    } else {
      sql.append(escape(raw))
    }
  }

  override def encodePrimaryKey(column: String, sql: StringBuffer): Unit = {
    encodeColumnName(null, column, sql)
    // make our primary key a string instead of the default integer
    sql.append(" character varying NOT NULL")
  }

  override def encodePostCreateTable(tableName: String, sql: StringBuffer): Unit = {
    val i = sql.indexOf(tableName)
    if (i == -1) {
      logger.warn(s"Did not find table name '$tableName' in CREATE TABLE statement: $sql")
    } else {
      // rename to the write ahead table
      sql.insert(i + tableName.length, WriteAheadTableSuffix.raw)
    }
  }

  override def postCreateTable(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit = {
    // Throw an error if the sft name is longer than 31 characters
    if (sft.getTypeName.length() > 31) {
      val errorMsg = "Can't create schema: type name exceeds max supported length of 31 characters"
      throw new IllegalArgumentException(errorMsg)
    }

    // note: we skip the call to `super`, which creates a spatial index (that we don't want), and which
    // alters the geometry column types (which we handle in the create statement)
    val info = TypeInfo(schemaName, sft)
    implicit val ex: ExecutionContext = new ExecutionContext(cx)
    try {
      PartitionedPostgisDialect.Commands.foreach(_.create(info))
    } finally {
      ex.close()
    }
  }

  override def postCreateAttribute(
      att: AttributeDescriptor,
      tableName: String,
      schemaName: String,
      cx: Connection): Unit = {

    def withCol(fn: ResultSet => Unit): Unit = {
      val meta = cx.getMetaData
      def escape(name: String): String = store.escapeNamePattern(meta, name)
      WithClose(meta.getColumns(cx.getCatalog, escape(schemaName), escape(tableName), escape(att.getLocalName))) { cols =>
        if (cols.next()) {
          fn(cols)
        } else {
          logger.warn(s"Could not retrieve column metadata for attribute ${att.getLocalName}")
        }
      }
    }

    if (classOf[String].isAssignableFrom(att.getType.getBinding)) {
      withCol { cols =>
        val typeName = cols.getString("TYPE_NAME")
        if ("json".equalsIgnoreCase(typeName) || "jsonb".equalsIgnoreCase(typeName)) {
          att.getUserData.put(SimpleFeatureTypes.AttributeOptions.OptJson, "true")
        }
      }
    } else if (classOf[java.util.List[_]].isAssignableFrom(att.getType.getBinding)) {
      withCol { cols =>
        val arrayType = super.getMapping(cols, cx)
        if (arrayType.isArray) {
          att.getUserData.put(SimpleFeatureTypes.AttributeConfigs.UserDataListType, arrayType.getComponentType.getName)
        } else {
          logger.warn(s"Found a list-type attribute but database type was not an array for ${att.getLocalName}")
        }
      }
    }
  }

  override def postCreateFeatureType(
      sft: SimpleFeatureType,
      metadata: DatabaseMetaData,
      schemaName: String,
      cx: Connection): Unit = {

    // normally views get set to read-only, override that here since we use triggers to delegate writes
    sft.getUserData.remove(JDBCDataStore.JDBC_READ_ONLY)

    // populate tablespaces (deprecated)
    PartitionTablespacesTable.read(cx, metadata, schemaName, sft.getTypeName).foreach { case (k, v) => sft.getUserData.put(k, v) }

    // populate user data
    UserDataTable.read(cx, schemaName, sft.getTypeName).foreach { case (k, v) => sft.getUserData.put(k, v) }

    // populate flags on indexed attributes
    getIndexedColumns(cx, sft.getTypeName).foreach { attribute =>
      Option(sft.getDescriptor(attribute)).foreach(_.getUserData.put(AttributeOptions.OptIndex, "true"))
    }
  }

  override def preDropTable(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit = {
    // due to the JDBCDataStore hard-coding "DROP TABLE" we have to redirect it away from the main view,
    // and we can't drop the write ahead table so that it has something to drop
    dropping.set(true)
    val info = TypeInfo(schemaName, sft)

    implicit val ex: ExecutionContext = new ExecutionContext(cx)
    try {
      PartitionedPostgisDialect.Commands.reverse.filter(_ != WriteAheadTable).foreach(_.drop(info))
    } finally {
      ex.close()
    }
  }

  override def postDropTable(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit = {
    val info = TypeInfo(schemaName, sft)

    implicit val ex: ExecutionContext = new ExecutionContext(cx)
    try {
      WriteAheadTable.drop(info) // drop the write ahead name sequence
    } finally {
      ex.close()
    }

    // rename the sft so that configuration is applied to the write ahead table
    super.postDropTable(schemaName, SimpleFeatureTypes.renameSft(sft, info.tables.writeAhead.name.raw), cx)
  }

  override def splitFilter(filter: Filter, schema: SimpleFeatureType): Array[Filter] = {
    val simplified = SplitFilterVisitor(filter, SftUserData.FilterWholeWorld.get(schema))
    val query = new Query(schema.getTypeName, simplified)
    interceptors(schema).foreach(_.rewrite(query))
    super.splitFilter(query.getFilter, schema)
  }

  override def registerClassToSqlMappings(mappings: java.util.Map[Class[_], Integer]): Unit = {
    super.registerClassToSqlMappings(mappings)
    mappings.put(classOf[java.util.List[_]], Types.ARRAY)
  }

  override def registerSqlTypeNameToClassMappings(mappings: java.util.Map[String, Class[_]]): Unit = {
    super.registerSqlTypeNameToClassMappings(mappings)
    mappings.put("array", classOf[java.util.List[_]])
  }

  override def registerSqlTypeToSqlTypeNameOverrides(overrides: java.util.Map[Integer, String]): Unit = {
    super.registerSqlTypeToSqlTypeNameOverrides(overrides)
    overrides.put(Types.ARRAY, "ARRAY")
  }

  override def encodePostColumnCreateTable(att: AttributeDescriptor, sql: StringBuffer): Unit = {
    import PartitionedPostgisDialect.GeometryAttributeConversions
    att match {
      case gd: GeometryDescriptor =>
        val nullable = gd.getMinOccurs <= 0 || gd.isNillable
        val i = sql.lastIndexOf("geometry")
        // expect `geometry NOT NULL` or `geometry` depending on nullable flag
        if (i == -1 || (nullable && i != sql.length() - 8) || (!nullable && i != sql.length() - 17)) {
          logger.warn(s"Found geometry-type attribute but no geometry column binding: $sql")
        } else {
          val srid = gd.getSrid.getOrElse(-1)
          val geomType = PartitionedPostgisDialect.GeometryMappings.getOrElse(gd.getType.getBinding, "GEOMETRY")
          val geomTypeWithDims = gd.getCoordinateDimensions match {
            case None | Some(2) => geomType
            case Some(3) => s"${geomType}Z"
            case Some(4) => s"${geomType}ZM"
            case Some(d) =>
              throw new IllegalArgumentException(
                s"PostGIS only supports geometries with 2, 3 and 4 dimensions, but found: $d")
          }
          sql.insert(i + 8, s" ($geomTypeWithDims, $srid)")
        }

      case _ if att.isJson() =>
        // replace 'VARCHAR' with jsonb
        val i = sql.lastIndexOf(" VARCHAR")
        if (i == sql.length() - 8) {
          sql.replace(i + 1, i + 8, "JSONB")
        } else {
          logger.warn(s"Found JSON-type attribute but no CHARACTER VARYING column binding: $sql")
        }

      case _ if att.isList =>
        // go back and encode the array type in the CQL create statement
        val i = sql.lastIndexOf(" ARRAY")
        if (i == sql.length() - 6) {
          sql.insert(i, " " + getListTypeMapping(att.getListType()))
        } else {
          logger.warn(s"Found list-type attribute but no ARRAY column binding: $sql")
        }

      case _ => // no-op
    }
  }

  override def getMapping(columnMetaData: ResultSet, cx: Connection): Class[_] = {
    val mapping = super.getMapping(columnMetaData, cx)
    if (mapping != null && mapping.isArray) {
      classOf[java.util.List[_]]
    } else {
      mapping
    }
  }

  /**
   * Gets the array type for a list/array column
   *
   * @param binding list-type binding
   * @return
   */
  private def getListTypeMapping(binding: Class[_]): String = {
    val mappings = new java.util.HashMap[String, Class[_]]()
    registerSqlTypeNameToClassMappings(mappings)
    var mapping: String = null
    var partial: String = null
    val iter = mappings.asScala.iterator
    while (iter.hasNext && mapping == null) {
      val (name, clas) = iter.next
      if (clas == binding) {
        mapping = name
      } else if (partial == null && clas.isAssignableFrom(binding)) {
        partial = name
      }
    }
    if (mapping != null) {
      mapping
    } else if (partial != null) {
      partial
    } else {
      logger.warn(s"Could not find list-type column for type ${binding.getName}")
      "text"
    }
  }

  /**
   * Gets a list of indexed columns for the given type
   *
   * @param cx connection
   * @param typeName feature type name
   * @return
   */
  private def getIndexedColumns(cx: Connection, typeName: String): Seq[String] = {
    val attributesWithIndicesSql =
      s"""select distinct(att.attname) as indexed_attribute_name
         |from pg_class obj
         |join pg_index idx on idx.indrelid = obj.oid
         |join pg_attribute att on att.attrelid = obj.oid and att.attnum = any(idx.indkey)
         |join pg_views v on v.viewname = ?
         |where obj.relname = concat(?, ${PartitionedTableSuffix.quoted})
         |order by att.attname;""".stripMargin
    try {
      WithClose(cx.prepareStatement(attributesWithIndicesSql)) { statement =>
        statement.setString(1, typeName)
        statement.setString(2, typeName)
        WithClose(statement.executeQuery()) { rs =>
          Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
        }
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error loading attributes with indices for schema $typeName:", e)
        Seq.empty
    }
  }
}

object PartitionedPostgisDialect extends Conversions {

  private val IgnoredTables = Seq("pg_stat_statements", "pg_stat_statements_info")

  private val GeometryMappings = Map[Class[_], String](
    classOf[Geometry]           -> "GEOMETRY",
    classOf[Point]              -> "POINT",
    classOf[LineString]         -> "LINESTRING",
    classOf[Polygon]            -> "POLYGON",
    classOf[MultiPoint]         -> "MULTIPOINT",
    classOf[MultiLineString]    -> "MULTILINESTRING",
    classOf[MultiPolygon]       -> "MULTIPOLYGON",
    classOf[GeometryCollection] -> "GEOMETRYCOLLECTION",
    classOf[CircularString]     -> "CIRCULARSTRING",
    classOf[CircularRing]       -> "CIRCULARSTRING",
    classOf[MultiCurve]         -> "MULTICURVE",
    classOf[CompoundCurve]      -> "COMPOUNDCURVE",
    classOf[CompoundRing]       -> "COMPOUNDCURVE"
  )

  private val Commands: Seq[Sql] = Seq(
    SequenceTable,
    WriteAheadTable,
    WriteAheadTrigger,
    PartitionTables,
    MainView,
    InsertTrigger,
    UpdateTrigger,
    DeleteTrigger,
    PrimaryKeyTable,
    PartitionTablespacesTable,
    AnalyzeQueueTable,
    SortQueueTable,
    UserDataTable,
    TruncateToTenMinutes,
    TruncateToPartition,
    RollWriteAheadLog,
    PartitionWriteAheadLog,
    MergeWriteAheadPartitions,
    DropAgedOffPartitions,
    PartitionMaintenance,
    AnalyzePartitions,
    CompactPartitions,
    LogCleaner
  )

  /**
   * Feature type user data fields
   *
   * @param key key used to store the data
   * @param mutable whether the value can be changed after the schema has been created
   * @param default default value, if any
   * @param conversion conversion from user data (string) to typed value
   * @tparam T typed value
   */
  case class SftUserData[T](key: String, mutable: Boolean, default: T)(implicit conversion: Conversion[T]) {
    def get(sft: SimpleFeatureType): T = Option(sft.getUserData.get(key)).map(conversion.convert).getOrElse(default)
    def get(userData: Map[String, String]): T = userData.get(key).map(conversion.convert).getOrElse(default)
  }

  object SftUserData {
    // default date field
    val DtgField: SftUserData[Option[String]] = SftUserData(SimpleFeatureTypes.Configs.DefaultDtgField, mutable = false, None)
    // size of each partition - can be updated after schema is created, but requires
    // running PartitionedPostgisDialect.upgrade in order to be applied
    val IntervalHours: SftUserData[Int] = SftUserData("pg.partitions.interval.hours", mutable = true, 6)
    // pages_per_range on the BRIN index - can't be updated after schema is created
    val PagesPerRange: SftUserData[Int] = SftUserData("pg.partitions.pages-per-range", mutable = false, 128)
    // max partitions to keep, i.e. age-off - can be updated freely after schema is created
    val MaxPartitions: SftUserData[Option[Int]] = SftUserData("pg.partitions.max", mutable = true, None)
    // minute of each 10 minute block to execute the partition jobs - TODO can be updated after schema is created,
    // but requires running PartitionedPostgisDialect.upgrade in order to be applied
    val CronMinute: SftUserData[Option[Int]] = SftUserData("pg.partitions.cron.minute", mutable = false, None)
    // remove 'whole world' filters - can be updated freely after schema is created
    val FilterWholeWorld: SftUserData[Boolean] = SftUserData("pg.partitions.filter.world", mutable = true, default = true)
    // query interceptors
    val QueryInterceptors: SftUserData[Option[String]] = SftUserData(SimpleFeatureTypes.Configs.QueryInterceptors, mutable = true, None)
    // set postgres table wal logging
    val WalLogEnabled: SftUserData[Boolean] = SftUserData("pg.wal.enabled", mutable = false, default = true)

    // tablespace configurations - can be updated freely after the schema is created
    val WriteAheadTableSpace: SftUserData[Option[String]] = SftUserData("pg.partitions.tablespace.wa", mutable = true, None)
    val WriteAheadPartitionsTableSpace: SftUserData[Option[String]] = SftUserData("pg.partitions.tablespace.wa-partitions", mutable = true, None)
    val MainTableSpace: SftUserData[Option[String]] = SftUserData("pg.partitions.tablespace.main", mutable = true, None)
  }

  implicit private def optionConversion[T](implicit conversion: Conversion[T]): Conversion[Option[T]] =
    new OptionConversion[T](conversion)

  private class OptionConversion[T](delegate: Conversion[T]) extends Conversion[Option[T]] {
    override def convert(value: AnyRef): Option[T] = Option(value).map(delegate.convert)
  }

  implicit class GeometryAttributeConversions(val d: GeometryDescriptor) extends AnyVal {
    def getSrid: Option[Int] =
      Option(d.getUserData.get(JDBCDataStore.JDBC_NATIVE_SRID)).map(int)
        .orElse(
          Option(d.getCoordinateReferenceSystem)
            .flatMap(crs => Try(CRS.lookupEpsgCode(crs, true)).filter(_ != null).toOption.map(_.intValue())))
    def getCoordinateDimensions: Option[Int] =
      Option(d.getUserData.get(Hints.COORDINATE_DIMENSION)).map(int)
  }
}
