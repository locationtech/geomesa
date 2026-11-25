/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.codec.digest.MurmurHash3
import org.geotools.api.data.Query
import org.geotools.api.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.data.postgis.PostGISDialect
import org.geotools.geometry.jts._
import org.geotools.jdbc.JDBCDataStore
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.{SftUserData, getIndexedColumns}
import org.locationtech.geomesa.gt.partition.postgis.dialect.filter.SplitFilterVisitor
import org.locationtech.geomesa.gt.partition.postgis.dialect.functions.{LogCleaner, TruncateToPartition, TruncateToTenMinutes}
import org.locationtech.geomesa.gt.partition.postgis.dialect.procedures._
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables._
import org.locationtech.geomesa.gt.partition.postgis.dialect.triggers.{DeleteTrigger, InsertTrigger, UpdateTrigger, WriteAheadTrigger}
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.utils.geotools.PrimitiveConversions.{Conversion, ConvertToInt}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.locationtech.jts.geom._

import java.nio.charset.StandardCharsets
import java.sql.{Connection, DatabaseMetaData, ResultSet, Types}
import java.util.regex.Pattern
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Dialect
 *
 * @param store data store
 * @param grants roles that should be granted access to feature types created by this dialect
 */
class PartitionedPostgisDialect(store: JDBCDataStore, grants: Seq[RoleName] = Seq.empty)
    extends PostGISDialect(store) with StrictLogging {

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

  // note: schemas are not removed through the jdbc store, as it hard-codes a "DROP TABLE" on the main view,
  // and doesn't run everything in a single transaction - see `dropSchema`

  private val interceptors = {
    val factory = QueryInterceptorFactory(store)
    sys.addShutdownHook(CloseWithLogging(factory)) // we don't have any API hooks to dispose of things...
    factory
  }

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

  override def encodeTableName(raw: String, sql: StringBuffer): Unit = sql.append(escape(raw))

  override def encodePrimaryKey(column: String, sql: StringBuffer): Unit = {
    encodeColumnName(null, column, sql)
    // make our primary key a string instead of the default integer
    sql.append(" character varying NOT NULL")
  }

  override def encodePostCreateTable(tableName: String, sql: StringBuffer): Unit = {
    if (tableName.length > 63) {
      throw new IllegalArgumentException("Can't create schema: type name exceeds max supported Postgres identifier length of 63")
    }
  }

  override def postCreateTable(schemaName: String, original: SimpleFeatureType, cx: Connection): Unit = {

    // note: we skip the call to `super`, which creates a spatial index (that we don't want), and which
    // alters the geometry column types (which we handle in the create statement)

    val sft = SimpleFeatureTypes.copy(original)

    // create everything in a single transaction, so that a failure part way through doesn't leave behind a
    // partially created feature type - in particular, a main view and user data without any partition maintenance
    // note: the jdbc store creates the initial table before calling this method, outside of our transaction
    val autoCommit = cx.getAutoCommit
    if (autoCommit) {
      cx.setAutoCommit(false)
    }
    implicit val ex: ExecutionContext = new ExecutionContext(cx)
    try {
      typeLock(schemaName, sft.getTypeName).locked {
        val info = typeInfo(schemaName, sft, cx)
        // note: we have to check this before we create anything
        val isNew = !isView(cx, info.tables.view.name)
        create(info, isNew)
      }
    } catch {
      case NonFatal(e) =>
        // note: the execution context only rolls back if one of its statements fails
        try { cx.rollback() } catch {
          case NonFatal(suppressed) => e.addSuppressed(suppressed)
        }
        throw e
    } finally {
      try { ex.close() } finally {
        if (autoCommit) {
          cx.setAutoCommit(true)
        }
      }
    }
  }

  /**
   * Drops the initial table that the jdbc store creates for a feature type, if creating the feature type failed
   * before the table was converted into the write ahead table. Note that this will fail if the feature type
   * was successfully created, as the name will refer to the main view.
   *
   * @param schemaName database schema
   * @param typeName name of the feature type (i.e. main view) that failed to be created
   * @param cx connection
   */
  def dropInitialTable(schemaName: String, typeName: String, cx: Connection): Unit = {
    WithClose(new ExecutionContext(cx)) { ex =>
      ex.execute(s"DROP TABLE IF EXISTS ${TableIdentifier(schemaName, typeName).qualified};")
    }
  }

  /**
   * Generates a unique name that can be used for the view (and associated tables) backing a feature type, when
   * the feature type name itself can't be used
   *
   * @param schemaName database schema
   * @param typeName feature type name
   * @param cx connection
   * @return
   */
  def nextViewName(schemaName: String, typeName: String, cx: Connection): String = {
    WithClose(new ExecutionContext(cx)) { implicit ex =>
      Iterator.continually(nextIdentifier(schemaName, typeName)).dropWhile(n => exists(ex.cx, TableIdentifier(schemaName, n))).next()
    }
  }

  /**
   * Checks if a relation (table, view, etc) exists
   *
   * @param schemaName database schema
   * @param name name of the relation
   * @param cx connection
   * @return
   */
  def relationExists(schemaName: String, name: String, cx: Connection): Boolean =
    exists(cx, TableIdentifier(schemaName, name))

  /**
   * Checks if a view exists
   *
   * @param schemaName database schema
   * @param viewName name of the view
   * @param cx connection
   * @return
   */
  def viewExists(schemaName: String, viewName: String, cx: Connection): Boolean =
    isView(cx, TableIdentifier(schemaName, viewName))

  /**
   * Gets the names of the attributes that are indexed in the main partition table for a feature type
   *
   * @param schemaName database schema
   * @param sft feature type, as loaded from the jdbc store (i.e. named for its main view)
   * @param cx connection
   * @return
   */
  def indexedColumns(schemaName: String, sft: SimpleFeatureType, cx: Connection): Seq[String] =
    getIndexedColumns(cx, TypeInfo(schemaName, sft)).get

  /**
   * Sets the feature type name that is exposed for a view
   *
   * @param schemaName database schema
   * @param viewName name of the main view
   * @param typeName feature type name
   * @param cx connection
   */
  def setTypeName(schemaName: String, viewName: String, typeName: String, cx: Connection): Unit = {
    WithClose(new ExecutionContext(cx)) { implicit ex =>
      UserDataTable.write(schemaName, viewName, SftUserData.TypeName, typeName)
    }
  }

  /**
   * Runs an operation while holding advisory locks for changes to one or more feature type names, so that different
   * data store instances don't try to create, update or remove the same feature type concurrently.
   *
   * Note that the locks are held by the connection passed in to this method, which must not be used for anything
   * that commits or rolls back while the operation is running, and that the locks are distinct from the lock used
   * by `postCreateTable`, so that the operation can create things.
   *
   * @param schemaName database schema
   * @param typeNames feature type names
   * @param cx connection used to hold the locks
   * @param op operation to run
   */
  def withUpdateLock(schemaName: String, typeNames: Seq[String], cx: Connection)(op: => Unit): Unit = {
    WithClose(new ExecutionContext(cx)) { implicit ex =>
      // note: the locks are always acquired in the same order, so that concurrent callers can't deadlock
      val locks = typeNames.distinct.sorted.map(typeLock(schemaName, _, seed = 1))
      def locked(remaining: Seq[AdvisoryLock]): Unit =
        if (remaining.isEmpty) { op } else { remaining.head.locked(locked(remaining.tail)) }
      locked(locks)
    }
  }

  /**
   * Drops a feature type (or a generation of a feature type), optionally along with previous generations of it.
   * Everything is dropped in a single transaction, so if anything fails then all of the generations are left
   * intact - in particular, we don't leave behind a main view without any partition maintenance.
   *
   * The previous generations must be passed in explicitly - they are not dropped when the current generation is
   * dropped on its own, so that a new generation that failed to be created can be cleaned up without affecting the
   * existing ones.
   *
   * Note that this bypasses the jdbc store, so the caller is responsible for clearing anything that the store
   * has cached for the views that are dropped.
   *
   * @param schemaName database schema
   * @param sft feature type to drop, as loaded from the jdbc store (i.e. named for its main view)
   * @param previous names of the main views for any previous generations to drop, most recent first
   * @param cx connection
   */
  def dropSchema(schemaName: String, sft: SimpleFeatureType, previous: Seq[String], cx: Connection): Unit = {
    val info = TypeInfo(schemaName, sft)
    val autoCommit = cx.getAutoCommit
    if (autoCommit) {
      cx.setAutoCommit(false)
    }
    try {
      try {
        WithClose(new ExecutionContext(cx, commit = false)) { implicit ex =>
          // note: load the previous generations up front, before we've dropped anything
          val (existing, retired) = previous.partition(v => exists(cx, TableIdentifier(schemaName, v)))
          val generations = existing.map(view => TypeInfo(schemaName, store.getSchema(view)))
          // note: we use the drop statements directly, without the advisory locks that are used to synchronize creating
          // the shared tables - we only delete rows from them, and holding the locks until we commit could deadlock
          // with another feature type being created, which takes them in the opposite order
          PartitionedPostgisDialect.dropSql(info).foreach(ex.execute)
          // note: the previous generations have to be dropped after the current view, which depends on their views
          generations.flatMap(PartitionedPostgisDialect.dropSql(_)).foreach(ex.execute)
          // clean up any user data left behind for generations that no longer exist
          retired.foreach(UserDataTable.delete(schemaName, _))
        }
        // rename the sft so that configuration is applied to the write ahead table
        super.postDropTable(schemaName, SimpleFeatureTypes.renameSft(sft, info.tables.writeAhead.name.raw), cx)
        cx.commit()
      } catch {
        case NonFatal(e) =>
          try { cx.rollback() } catch {
            case NonFatal(suppressed) => e.addSuppressed(suppressed)
          }
          throw e
      }
    } finally {
      if (autoCommit) {
        cx.setAutoCommit(true)
      }
    }
  }

  /**
   * Gets the next unique identifier for a feature type
   *
   * @param schemaName database schema
   * @param typeName feature type name
   * @param ex execution context
   * @return
   */
  private def nextIdentifier(schemaName: String, typeName: String)(implicit ex: ExecutionContext): String = {
    // note: 'if not exists' still fails if the sequence is created concurrently, so we synchronize creating it
    if (!exists(ex.cx, TableIdentifier(schemaName, PartitionedPostgisDialect.SftSeqName))) {
      PartitionedPostgisDialect.SftSeqLock.locked {
        ex.execute(
          s"CREATE SEQUENCE IF NOT EXISTS ${escape(schemaName)}.${PartitionedPostgisDialect.SftSeqName} " +
            s"AS integer MINVALUE 0 MAXVALUE 65535")
      }
    }
    val sql = s"SELECT nextval(${literal(s"$schemaName.${PartitionedPostgisDialect.SftSeqName}")})"
    val nextVal = WithClose(ex.cx.prepareStatement(sql)) { st =>
      WithClose(st.executeQuery()) { rs =>
        rs.next()
        rs.getInt(1)
      }
    }
    if (nextVal > 0xFFFF) {
      throw new IllegalStateException(
        s"Sequence ${PartitionedPostgisDialect.SftSeqName} has exceeded maximum supported value of 65535 unique feature types")
    }
    typeName.take(26) + f"_$nextVal%04x" // 4-character hex-encoded padded string
  }

  /**
   * Gets an advisory lock for a feature type, so that multiple threads don't try to create things concurrently,
   * which can cause errors
   *
   * @param schemaName database schema
   * @param typeName feature type name
   * @return
   */
  private def typeLock(schemaName: String, typeName: String, seed: Int = 0): AdvisoryLock = {
    new AdvisoryLock {
      override protected val lockId: Long = {
        val bytes = s"$schemaName.$typeName".getBytes(StandardCharsets.UTF_8)
        MurmurHash3.hash128x64(bytes, 0, bytes.length, seed).head
      }
    }
  }

  /**
   * Gets the type info for a feature type, populating any identifiers that are derived from the database
   *
   * @param schemaName database schema
   * @param sft feature type, will be updated with derived identifiers
   * @param cx connection
   * @param ex execution context
   * @return
   */
  private def typeInfo(schemaName: String, sft: SimpleFeatureType, cx: Connection)(implicit ex: ExecutionContext): TypeInfo = {
    // if the sft name is longer than 31 characters, use an alias for delegate tables to avoid character limits
    // 31 is the max length, based on the current length of our sql identifiers (tables, etc)
    // note: for an existing type that is being upgraded, we need to re-use the alias that was assigned when it was created
    if (sft.getTypeName.length() >= 32 && !sft.getUserData.containsKey(SftUserData.IdentAlias.key)) {
      sft.getUserData.put(SftUserData.IdentAlias.key, nextIdentifier(schemaName, sft.getTypeName))
    }
    // get the first column as the fid col, which may or may not be called 'fid'
    WithClose(cx.getMetaData.getColumns(null, schemaName, sft.getTypeName, null)) { cols =>
      if (cols.next()) {
        val name = cols.getString("COLUMN_NAME")
        if (name != null) {
          sft.getUserData.put(SftUserData.FidColumn.key, name)
        }
      }
    }
    val info = TypeInfo(schemaName, sft)
    // load the previous generation of this type, if there is one and it hasn't been retired yet
    val previous = SftUserData.PreviousView.get(sft).filter(view => exists(cx, TableIdentifier(schemaName, view))).map { view =>
      TypeInfo(schemaName, store.getSchema(view))
    }
    info.copy(previous = previous)
  }

  /**
   * Creates (or re-creates) all the tables, procedures, etc for a feature type
   *
   * @param info type info
   * @param isNew whether the feature type (or generation) is being created for the first time
   * @param ex execution context
   */
  private def create(info: TypeInfo, isNew: Boolean)(implicit ex: ExecutionContext): Unit = {
    PartitionedPostgisDialect.Commands.foreach(_.create(info))
    if (grants.nonEmpty) {
      val roles = grants.map(_.quoted).mkString(", ")
      val tables =
        Seq(
          info.tables.view.name,
          info.tables.writeAhead.name,
          info.tables.writeAheadPartitions.name,
          info.tables.mainPartitions.name,
          info.tables.spillPartitions.name,
          TableIdentifier(info.schema.raw, PrimaryKeyTable.Name.raw),
          TableIdentifier(info.schema.raw, UserDataTable.Name.raw)
        )
      tables.foreach { table =>
        ex.execute(s"GRANT SELECT ON ${table.qualified} TO $roles;")
      }
    }
    info.previous.foreach { previous =>
      // carry over any privileges that were granted on the previous view
      // note: we only do this when the generation is created, so that we don't restore privileges
      // that are subsequently revoked from the current view
      if (isNew) {
        viewPrivileges(previous.tables.view.name, info.tables.view.name).foreach(ex.execute)
      }
      // age-off is configured per-generation, so apply the current configuration to all the previous generations
      // note: the retire procedure for each generation also keeps its previous generation in sync, in case the
      // configuration is changed directly in the user data table
      // note: we only write the rows that are out of sync, to avoid taking row locks if nothing has changed
      val table = s"${info.schema.quoted}.${UserDataTable.Name.quoted}"
      val generations =
        s"""WITH RECURSIVE generations(view) AS (
           |  SELECT ?::text
           |  UNION
           |  SELECT u.value FROM $table u JOIN generations g ON u.type_name = g.view WHERE u.key = ?
           |)""".stripMargin
      val key = SftUserData.MaxPartitions.key
      info.partitions.maxPartitions match {
        case Some(max) =>
          val value = String.valueOf(max)
          ex.executeUpdate(
            s"""$generations
               |INSERT INTO $table (type_name, key, value, mutable)
               |  SELECT g.view, ?, ?, true FROM generations g
               |    WHERE NOT EXISTS (SELECT FROM $table t WHERE t.type_name = g.view AND t.key = ? AND t.value = ?)
               |  ON CONFLICT (type_name, key) DO UPDATE SET value = EXCLUDED.value;""".stripMargin,
            Seq(previous.typeName, SftUserData.PreviousView.key, key, value, key, value))

        case None =>
          ex.executeUpdate(
            s"""$generations
               |DELETE FROM $table WHERE key = ? AND type_name IN (SELECT view FROM generations);""".stripMargin,
            Seq(previous.typeName, SftUserData.PreviousView.key, key))
      }
    }
  }

  /**
   * Gets grant statements that will restore any privileges on the main view, so that they can be re-applied if
   * the view is dropped and re-created
   *
   * @param view existing view
   * @param target view that the privileges will be applied to
   * @param ex execution context
   * @return
   */
  private def viewPrivileges(view: TableIdentifier, target: TableIdentifier)(implicit ex: ExecutionContext): Seq[String] = {
    val sql =
      s"""SELECT r.rolname, acl.privilege_type, acl.is_grantable
         |  FROM pg_class c
         |  CROSS JOIN LATERAL aclexplode(c.relacl) acl
         |  LEFT JOIN pg_roles r ON r.oid = acl.grantee
         |  WHERE c.oid = to_regclass(${literal(view.qualified)}) AND acl.grantee <> c.relowner;""".stripMargin
    WithClose(ex.cx.createStatement()) { st =>
      WithClose(st.executeQuery(sql)) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map { row =>
          val role = Option(row.getString(1)).map(escape).getOrElse("PUBLIC") // a null role indicates public
          val option = if (row.getBoolean(3)) { " WITH GRANT OPTION" } else { "" }
          s"GRANT ${row.getString(2)} ON ${target.qualified} TO $role$option;"
        }.toList
      }
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

    // the default geometry wasn't always recorded in the user data, and the jdbc store always uses the first geometry
    if (!sft.getUserData.containsKey(SftUserData.GeomField.key)) {
      legacyDefaultGeometry(sft, schemaName, cx).foreach(sft.getUserData.put(SftUserData.GeomField.key, _))
    }

    // populate flags on indexed attributes
    getIndexedColumns(cx, TypeInfo(schemaName, sft)) match {
      case Success(cols) =>
        cols.foreach { col =>
          val i = sft.indexOf(col)
          if (i == -1) {
            logger.debug(
              s"Found unexpected indexed column not in feature type: $col for ${sft.getTypeName}=${SimpleFeatureTypes.encodeType(sft)}")
          } else {
            sft.getDescriptor(i).getUserData.put(AttributeOptions.OptIndex, "true")
          }
        }

      case Failure(e) => logger.warn(s"Error loading indexed columns for feature type ${sft.getTypeName}:", e)
    }
  }

  /**
   * Gets the default geometry for a feature type that was created before the default geometry was recorded in the
   * user data. The default geometry is used to sort the data in each partition, so we determine it based on the
   * column that the existing procedures are sorting by.
   *
   * @param sft feature type, with user data populated
   * @param schemaName database schema
   * @param cx connection
   * @return the default geometry, if it could be determined and there is more than one candidate
   */
  private def legacyDefaultGeometry(sft: SimpleFeatureType, schemaName: String, cx: Connection): Option[String] = {
    val geoms = sft.getAttributeDescriptors.asScala.collect { case d: GeometryDescriptor => d.getLocalName }
    if (geoms.lengthCompare(2) < 0) { None } else {
      val identifier = SftUserData.IdentAlias.get(sft).getOrElse(sft.getTypeName)
      val sql = "SELECT p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ? AND p.proname = ?"
      try {
        val source = WithClose(cx.prepareStatement(sql)) { st =>
          st.setString(1, schemaName)
          st.setString(2, s"${identifier}_compact_partitions")
          WithClose(st.executeQuery())(rs => if (rs.next()) { Option(rs.getString(1)) } else { None })
        }
        source.flatMap { src =>
          val matcher = PartitionedPostgisDialect.SortColumn.matcher(src)
          if (matcher.find()) { Some(matcher.group(1).replace("\"\"", "\"")).filter(geoms.contains) } else { None }
        }
      } catch {
        case NonFatal(e) => logger.warn(s"Error loading default geometry for feature type ${sft.getTypeName}:", e); None
      }
    }
  }

  override def preDropTable(schemaName: String, sft: SimpleFeatureType, cx: Connection): Unit = {
    // the jdbc store hard-codes a "DROP TABLE" on the main view, which will fail, and doesn't drop things atomically
    throw new UnsupportedOperationException(
      "Schemas can't be removed through the jdbc data store - use PartitionedPostgisDataStore.removeSchema instead")
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
}

object PartitionedPostgisDialect extends StrictLogging {

  // name of the hidden physical column used to store per-row visibility labels
  val VisCol: String = "_vis"

  private val SftSeqName = "geomesa_sft_seq"

  // the (quoted) column that partitions are sorted by, in the sql for our procedures
  private val SortColumn = Pattern.compile("_st_sortablehash\\(\"((?:[^\"]|\"\")+)\"\\)")

  // synchronizes creating the sequence
  private object SftSeqLock extends AdvisoryLock {
    override protected val lockId: Long = 4183902216709473921L
  }

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

  /**
   * Gets the sql to drop everything for a feature type (or a generation of a feature type).
   *
   * The statements don't depend on the state of a connection, so they can be executed later (for example, as part
   * of a procedure). They don't take the advisory locks that are used to synchronize creating the shared tables,
   * as they only delete rows from them.
   *
   * @param info type info
   * @param writeAhead include the write ahead table, which otherwise has to be dropped separately
   * @return
   */
  private[dialect] def dropSql(info: TypeInfo, writeAhead: Boolean = true): Seq[String] = {
    val drop = Commands.reverse.filter(_ != WriteAheadTable).flatMap(_.dropSql(info)) ++ PartitionTablespacesTable.dropSql(info)
    if (!writeAhead) { drop } else {
      // note: cascade the drop to delete any write ahead partitions
      drop ++ Seq(s"DROP TABLE IF EXISTS ${info.tables.writeAhead.name.qualified} CASCADE;") ++ WriteAheadTable.dropSql(info)
    }
  }

  private[dialect] val Commands: Seq[Sql] = Seq(
    SequenceTable,
    WriteAheadTable,
    WriteAheadTrigger,
    PartitionTables,
    WriteAheadMigrationsTable,
    AgeOffQueueTable,
    WriteAheadRetirementsTable,
    MainView,
    InsertTrigger,
    UpdateTrigger,
    DeleteTrigger,
    PrimaryKeyTable,
    AnalyzeQueueTable,
    SortQueueTable,
    UserDataTable,
    TruncateToTenMinutes,
    TruncateToPartition,
    CleanWriteAheadRetirements,
    RollWriteAheadLog,
    PartitionWriteAheadLog,
    MergeWriteAheadPartitions,
    CleanWriteAheadMigrations,
    CleanAgedOffPartitions,
    DropAgedOffPartitions,
    PartitionMaintenance,
    AnalyzePartitions,
    CompactPartitions,
    LogCleaner,
    RetirePreviousGeneration,
  )

  /**
   * Feature type user data fields
   *
   * @param key key used to store the data
   * @param mutable whether the value can be changed directly in the user data table after the schema has been
   *                created, without having to re-create the partition procedures
   * @param default default value, if any
   * @param updatable whether the value can be changed through `updateSchema` after the schema has been created -
   *                  note that this is not the same thing as `mutable`
   * @param internal whether the value is managed internally, and can't be set by a user
   * @param conversion conversion from user data (string) to typed value
   * @tparam T typed value
   */
  case class SftUserData[T](key: String, mutable: Boolean, default: T, updatable: Boolean = true, internal: Boolean = false)
      (implicit conversion: Conversion[T]) {
    def get(sft: SimpleFeatureType): T = Option(sft.getUserData.get(key)).map(conversion.convert).getOrElse(default)
    def get(userData: Map[String, String]): T = userData.get(key).map(conversion.convert).getOrElse(default)
  }

  object SftUserData {
    // default date field
    val DtgField: SftUserData[Option[String]] = SftUserData(SimpleFeatureTypes.Configs.DefaultDtgField, mutable = false, None, updatable = false)
    // size of each partition - can be updated after schema is created, but requires
    // running PartitionedPostgisDialect.upgrade in order to be applied
    val IntervalHours: SftUserData[Int] = SftUserData("pg.partitions.interval.hours", mutable = true, 6)
    // pages_per_range on the BRIN index - can't be updated after schema is created
    val PagesPerRange: SftUserData[Int] = SftUserData("pg.partitions.pages-per-range", mutable = false, 128, updatable = false)
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
    val WalLogEnabled: SftUserData[Boolean] = SftUserData("pg.wal.enabled", mutable = false, default = true, updatable = false)
    // enable per-row visibility enforcement via a hidden '_vis' column
    val VisEnabled: SftUserData[Boolean] = SftUserData("pg.vis.enabled", mutable = false, default = false, updatable = false)
    // unique alias to use for identifiers so that we don't exceed the max postgres identifier length
    val IdentAlias: SftUserData[Option[String]] = SftUserData("pg.ident.alias", mutable = false, None, internal = true)
    // name of the feature id column
    val FidColumn: SftUserData[String] = SftUserData("pg.fid.col", mutable = false, "fid", internal = true)
    // name of the default geometry column - can't be updated after schema is created
    val GeomField: SftUserData[Option[String]] = SftUserData("pg.geom.col", mutable = false, None, internal = true)
    // toast_tuple_target for main and spill partitions, i.e. the stored row size above which postgres moves the
    // largest values out of line, leaving less to read when scanning - can be updated after schema is created,
    // but requires running PartitionedPostgisDialect.upgrade in order to be applied, and then only affects
    // partitions created afterwards
    val ToastTupleTarget: SftUserData[Option[Int]] = SftUserData("pg.partitions.toast-tuple-target", mutable = true, None)
    // include the primary geometry if it is a point in the dtg index, so that counts with time/bbox filters can
    // use index-only scans; visibility is always a trailing key - can't be updated after schema is created
    val CoveringDtgIndex: SftUserData[Boolean] =
      SftUserData("pg.partitions.dtg-index.covering", mutable = false, default = false, updatable = false)

    // tablespace configurations - can be updated freely after the schema is created
    val WriteAheadTableSpace: SftUserData[Option[String]] = SftUserData("pg.partitions.tablespace.wa", mutable = true, None)
    val WriteAheadPartitionsTableSpace: SftUserData[Option[String]] = SftUserData("pg.partitions.tablespace.wa-partitions", mutable = true, None)
    val MainTableSpace: SftUserData[Option[String]] = SftUserData("pg.partitions.tablespace.main", mutable = true, None)

    // the feature type name exposed for a view, if it's different from the name of the view itself
    val TypeName: SftUserData[Option[String]] = SftUserData("pg.type.name", mutable = false, None, internal = true)
    // the view for the previous generation of a feature type, which is included in the view for the current generation
    val PreviousView: SftUserData[Option[String]] = SftUserData("pg.view.previous", mutable = false, None, internal = true)

    // all the configs - note: there's a unit test that verifies that this list is complete
    val Values: Seq[SftUserData[_]] =
      Seq(
        DtgField,
        IntervalHours,
        PagesPerRange,
        MaxPartitions,
        CronMinute,
        FilterWholeWorld,
        QueryInterceptors,
        WalLogEnabled,
        VisEnabled,
        IdentAlias,
        FidColumn,
        GeomField,
        ToastTupleTarget,
        CoveringDtgIndex,
        WriteAheadTableSpace,
        WriteAheadPartitionsTableSpace,
        MainTableSpace,
        TypeName,
        PreviousView,
      )
  }

  implicit private def optionConversion[T](implicit conversion: Conversion[T]): Conversion[Option[T]] =
    new OptionConversion[T](conversion)

  private class OptionConversion[T](delegate: Conversion[T]) extends Conversion[Option[T]] {
    override def convert(value: AnyRef): Option[T] = Option(value).map(delegate.convert)
  }

  implicit class GeometryAttributeConversions(val d: GeometryDescriptor) extends AnyVal {
    def getSrid: Option[Int] =
      Option(d.getUserData.get(JDBCDataStore.JDBC_NATIVE_SRID)).map(ConvertToInt.convert)
        .orElse(
          Option(d.getCoordinateReferenceSystem)
            .flatMap(crs => Try(CRS.lookupEpsgCode(crs, true)).filter(_ != null).toOption.map(_.intValue())))
    def getCoordinateDimensions: Option[Int] =
      Option(d.getUserData.get(Hints.COORDINATE_DIMENSION)).map(ConvertToInt.convert)
  }

  /**
   * Get a list of indexed columns for the given SimpleFeatureType
   *
   * @param cx connection
   * @param info type info
   * @return a sequence of SimpleFeatureType attribute names which have an index
   */
  private def getIndexedColumns(cx: Connection, info: TypeInfo): Try[List[String]] = {
    // only key columns: indkey also lists the INCLUDE columns of a covering index, after the first indnkeyatts
    val sql =
      s"""select distinct(att.attname) as indexed_attribute_name
         |from pg_class obj
         |join pg_index idx on idx.indrelid = obj.oid
         |join pg_attribute att on att.attrelid = obj.oid
         |  and exists (select from generate_series(0, idx.indnkeyatts - 1) k where idx.indkey[k] = att.attnum)
         |where obj.relname = ${literal(info.tables.mainPartitions.name.raw)}
         |order by att.attname;""".stripMargin
    Try {
      WithClose(cx.createStatement()) { st =>
        WithClose(st.executeQuery(sql)) { rs =>
          Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1))
            .filter(c => c != info.cols.fid.raw && !info.cols.vis.exists(_.raw == c)).toList
        }
      }
    }
  }
}
