/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import com.github.benmanes.caffeine.cache.{AsyncCacheLoader, AsyncLoadingCache, CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore.{SchemaCompatibility, VersionKey}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.conf.SemanticVersion.MinorOrdering
import org.locationtech.geomesa.utils.conf.{GeoMesaProperties, IndexId, SemanticVersion}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeComparator.TypeComparison
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{AttributeOptions, Configs, InternalConfigs}
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.IOException
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Abstract base class for data store implementations on top of distributed databases
  *
  * @param config common datastore configuration options - subclasses can extend this
  * @tparam DS type of this data store
  */
abstract class GeoMesaDataStore[DS <: GeoMesaDataStore[DS]](val config: GeoMesaDataStoreConfig)
    extends MetadataBackedDataStore(config) with HasGeoMesaStats {

  this: DS =>

  import scala.collection.JavaConverters._

  val queryPlanner: QueryPlanner[DS] = new QueryPlanner(this)

  val manager: IndexManager = new IndexManager(this)

  // abstract methods to be implemented by subclasses

  def adapter: IndexAdapter[DS]

  /**
    * Returns all tables that may be created for the simple feature type. Note that some of these
    * tables may be shared with other simple feature types, and the tables may not all currently exist.
    *
    * @param typeName simple feature type name
    * @return
    */
  def getAllTableNames(typeName: String): Seq[String] = Seq(config.catalog) ++ getAllIndexTableNames(typeName)

  /**
    * Returns all index tables that may be created for the simple feature type. Note that some of these
    * tables may be shared with other simple feature types, and the tables may not all currently exist.
    *
    * @param typeName simple feature type name
    * @return
    */
  def getAllIndexTableNames(typeName: String): Seq[String] =
    Option(getSchema(typeName)).toSeq.flatMap(sft => manager.indices(sft).flatMap(_.getTableNames(None)))

  /**
    * Optimized method to delete everything (all tables) associated with this datastore
    * (index tables and catalog table)
    * NB: We are *not* currently deleting the query table and/or query information.
    */
  def delete(): Unit = {
    val types = getTypeNames
    val tables = if (types.length == 0) Array(config.catalog) else {
      types.flatMap(getAllTableNames).distinct
    }
    adapter.deleteTables(tables)
    metadata.resetCache()
  }

  // hooks to allow extended functionality

  /**
    * Gets iterator versions as a string. Subclasses with distributed classpaths should override and implement.
    *
    * @return iterator versions
    */
  protected def loadIteratorVersions: Set[String] = Set.empty

  /**
    * Update the local value for `sft.getIndices`. Only needed for legacy data stores with old index metadata
    * encoding
    *
    * @param sft simple feature type
    */
  protected def transitionIndices(sft: SimpleFeatureType): Unit =
    throw new NotImplementedError("This data store does not support legacy index formats - please create a new schema")

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaCreate(sft: SimpleFeatureType): Unit = {
    import Configs.{TableSplitterClass, TableSplitterOpts}
    import InternalConfigs.{PartitionSplitterClass, PartitionSplitterOpts}

    // check for old enabled indices and re-map them
    // noinspection ScalaDeprecation
    SimpleFeatureTypes.Configs.ENABLED_INDEX_OPTS.drop(1).find(sft.getUserData.containsKey).foreach { key =>
      sft.getUserData.put(SimpleFeatureTypes.Configs.EnabledIndices, sft.getUserData.remove(key))
    }

    // validate column groups
    adapter.groups.validate(sft)

    // disable table sharing, no longer supported
    // noinspection ScalaDeprecation
    if (sft.isTableSharing) {
      logger.warn("Table sharing is no longer supported - disabling table sharing")
      sft.getUserData.remove(Configs.TableSharing)
      sft.getUserData.remove(InternalConfigs.TableSharingPrefix)
    }

    // configure the indices to use
    if (sft.getIndices.isEmpty) {
      val indices = GeoMesaFeatureIndexFactory.indices(sft)
      if (indices.isEmpty) {
        throw new IllegalArgumentException("There are no available indices that support the schema " +
            SimpleFeatureTypes.encodeType(sft))
      }
      sft.setIndices(indices)
    }

    // try to create the indices up front to ensure they are valid for the sft
    GeoMesaFeatureIndexFactory.create(this, sft, sft.getIndices)

    // remove the enabled indices after configuration so we don't persist them
    sft.getUserData.remove(SimpleFeatureTypes.Configs.EnabledIndices)
    // remove any 'index' flags in the attribute metadata, they have already been captured in the indices above
    sft.getAttributeDescriptors.asScala.foreach(_.getUserData.remove(AttributeOptions.OptIndex))

    // for partitioned schemas, persist the table partitioning keys
    if (TablePartition.partitioned(sft)) {
      Seq((TableSplitterClass, PartitionSplitterClass), (TableSplitterOpts, PartitionSplitterOpts)).foreach {
        case (from, to) => Option(sft.getUserData.get(from)).foreach(sft.getUserData.put(to, _))
      }
    }

    // set stats enabled based on the data store config if not explicitly set
    if (!sft.getUserData.containsKey(SimpleFeatureTypes.Configs.StatsEnabled)) {
      sft.setStatsEnabled(config.generateStats)
    }

    sft.getFeatureExpiration // validate any configured age-off
  }

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaUpdate(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    updateSchemaUserData(sft, previous)
    // try to create the new indices to ensure they are valid for the sft
    try { GeoMesaFeatureIndexFactory.create(this, sft, sft.getIndices) } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Error configuring new feature index:", e)
    }
    // validate any configured age-off
    sft.getFeatureExpiration
  }

  // create the index tables (if not using partitioned tables)
  override protected def onSchemaCreated(sft: SimpleFeatureType): Unit = {
    val indices = manager.indices(sft)
    if (TablePartition.partitioned(sft)) {
      logger.debug(s"Delaying creation of partitioned indices ${indices.map(_.identifier).mkString(", ")}")
    } else {
      logger.debug(s"Creating indices ${indices.map(_.identifier).mkString(", ")}")
      indices.foreach(index => adapter.createTable(index, None, index.getSplits(None)))
    }
  }

  // create the new index tables (if not using partitioned tables)
  override protected def onSchemaUpdated(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    val partitioned = TablePartition.partitioned(sft)

    // check for column renaming
    val colMap = getColumnMap(previous, sft).map { case (k, v) => (v, k) }

    // rewrite the table name keys for the renamed cols
    if (colMap.nonEmpty) {
      sft.getIndices.foreach { i =>
        if (i.attributes.exists(colMap.contains)) {
          val prev = i.copy(attributes = i.attributes.map(a => colMap.getOrElse(a, a)))
          val old = manager.index(previous, GeoMesaFeatureIndex.identifier(prev))
          val index = GeoMesaFeatureIndexFactory.create(this, sft, Seq(i)).headOption.getOrElse {
            throw new IllegalArgumentException(
              s"Error configuring new feature index: ${GeoMesaFeatureIndex.identifier(i)}")
          }
          val partitions = if (!partitioned) { Seq(None) } else {
            // have to use the old table name key but the new sft name for looking up the partitions
            val tableNameKey = old.tableNameKey(Some(""))
            val offset = tableNameKey.length
            metadata.scan(sft.getTypeName, tableNameKey).map { case (k, _) => Some(k.substring(offset)) }
          }
          partitions.foreach { p =>
            metadata.read(sft.getTypeName, old.tableNameKey(p)).foreach { v =>
              metadata.insert(sft.getTypeName, index.tableNameKey(p), v)
            }
          }
        }
      }
    }

    // configure any new indices
    if (partitioned) {
      logger.debug("Delaying creation of partitioned indices")
    } else {
      logger.debug(s"Ensuring indices ${manager.indices(sft).map(_.identifier).mkString(", ")}")
      manager.indices(sft).foreach(index => adapter.createTable(index, None, index.getSplits(None)))
    }

    // update stats
    if (previous.statsEnabled) {
      if (!sft.statsEnabled) {
        stats.writer.clear(previous)
      } else if (sft.getTypeName != previous.getTypeName || colMap.nonEmpty) {
        stats.writer.rename(sft, previous)
      }
    }

    // rename tables to match the new sft name
    if (sft.getTypeName != previous.getTypeName || sft.getIndices != previous.getIndices) {
      if (FastConverter.convertOrElse[java.lang.Boolean](sft.getUserData.get(Configs.UpdateRenameTables), false)) {
        manager.indices(sft).foreach { index =>
          val partitions = if (partitioned) { index.getPartitions.map(Option.apply) } else { Seq(None) }
          partitions.foreach { partition =>
            val key = index.tableNameKey(partition)
            metadata.read(sft.getTypeName, key).foreach { table =>
              metadata.remove(sft.getTypeName, key)
              val renamed = index.configureTableName(partition, adapter.tableNameLimit)
              if (renamed != table) {
                logger.debug(s"Renaming table from '$table' to '$renamed'")
                adapter.renameTable(table, renamed)
              }
            }
          }
        }
      }
    }
  }

  // delete the index tables
  override protected def onSchemaDeleted(sft: SimpleFeatureType): Unit = {
    def deleteFull(index: GeoMesaFeatureIndex[_, _]): Unit =
      adapter.deleteTables(index.deleteTableNames(None))
    def deleteShared(index: GeoMesaFeatureIndex[_, _]): Unit = {
      if (index.keySpace.sharing.isEmpty) { deleteFull(index) } else {
        adapter.clearTables(index.deleteTableNames(None), Some(index.keySpace.sharing))
      }
    }

    val indices = manager.indices(sft).toList
    // noinspection ScalaDeprecation
    if (sft.isTableSharing && getTypeNames.exists(t => t != sft.getTypeName && getSchema(t).isTableSharing)) {
      indices.map(i => CachedThreadPool.submit(() => deleteShared(i))).foreach(_.get)
    } else {
      indices.map(i => CachedThreadPool.submit(() => deleteFull(i))).foreach(_.get)
    }
    if (sft.statsEnabled) {
      stats.writer.clear(sft)
    }
  }

  // methods from org.geotools.api.data.DataStore

  /**
   * @see org.geotools.api.data.DataStore#getSchema(java.lang.String)
   * @param typeName feature type name
   * @return feature type, or null if it does not exist
   */
  override def getSchema(typeName: String): SimpleFeatureType = {
    var sft = super.getSchema(typeName)
    if (sft != null) {
      // ensure index metadata is correct
      if (sft.getIndices.exists(i => i.attributes.isEmpty && i.name != IdIndex.name)) {
        sft = SimpleFeatureTypes.mutable(sft)
        // migrate index metadata to standardized versions and attributes
        transitionIndices(sft)
        sft = SimpleFeatureTypes.immutable(sft)
        // validate indices
        try { manager.indices(sft) } catch {
          case NonFatal(e) =>
            throw new IllegalStateException(s"The schema ${sft.getTypeName} was written with a older " +
                "version of GeoMesa that is no longer supported. You may continue to use an older client, or " +
                s"manually edit the metadata for '${InternalConfigs.IndexVersions}' to exclude the invalid indices.", e)
        }
      } else {
        // validate indices
        try { manager.indices(sft) } catch {
          case NonFatal(e) =>
            val versions = sft.getIndices.map(i => s"${i.name}:${i.version}").mkString(",")
            val available = GeoMesaFeatureIndexFactory.available(sft).map(i => s"${i._1}:${i._2}").mkString(",")
            logger.error(s"Trying to access schema ${sft.getTypeName} with invalid index versions '$versions' - " +
                s"available indices are '$available'", e)
            throw new IllegalStateException(s"The schema ${sft.getTypeName} was written with a newer " +
                "version of GeoMesa than this client can handle. Please ensure that you are using the " +
                "same GeoMesa jar versions across your entire workflow. For more information, see " +
                "http://www.geomesa.org/documentation/user/installation_and_configuration.html#upgrading")
        }
      }

      // check for sft-level stats flag and set it if not present
      if (!sft.getUserData.containsKey(SimpleFeatureTypes.Configs.StatsEnabled)) {
        val extra = Collections.singletonMap(SimpleFeatureTypes.Configs.StatsEnabled, config.generateStats.toString)
        sft = SimpleFeatureTypes.immutable(sft, extra)
      }

      // get the remote version if it's available, but don't wait for it
      GeoMesaDataStore.versions.get(VersionKey(this)).getNow(Right(None)) match {
        case Left(e) => throw e
        case Right(version) =>
          version.foreach { v =>
            val userData = Collections.singletonMap[AnyRef, AnyRef](InternalConfigs.RemoteVersion, v.toString)
            sft = SimpleFeatureTypes.immutable(sft, userData)
          }
      }
    }
    sft
  }

  /**
   * @see org.geotools.api.data.DataStore#getFeatureSource(java.lang.String)
   * @param typeName simple feature type name
   * @return featureStore, suitable for reading and writing
   */
  override def getFeatureSource(typeName: String): GeoMesaFeatureStore = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    new GeoMesaFeatureStore(this, sft)
  }

  override private[geomesa] def getFeatureReader(
      sft: SimpleFeatureType,
      transaction: Transaction,
      query: Query): GeoMesaFeatureReader = {
    if (transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    GeoMesaFeatureReader(sft, query, queryPlanner, config.audit)
  }

  /**
    * Internal method to get a feature writer without reloading the simple feature type. We don't expose this
    * widely as we want to ensure that the sft has been loaded from our catalog
    *
    * @param sft simple feature type
    * @param filter if defined, will do an updating write, otherwise will do an appending write
    * @return
    */
  override private[geomesa] def getFeatureWriter(
      sft: SimpleFeatureType,
      transaction: Transaction,
      filter: Option[Filter]): FlushableFeatureWriter = {
    val atomic = transaction == AtomicWriteTransaction.INSTANCE
    if (!atomic && transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    GeoMesaFeatureWriter(this, sft, manager.indices(sft, mode = IndexMode.Write), filter, atomic)
  }

  /**
    * Writes to the specified indices
    *
    * @param typeName feature type name
    * @param indices indices to write
    * @return
    */
  def getIndexWriterAppend(typeName: String, indices: Seq[GeoMesaFeatureIndex[_, _]]): FlushableFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    GeoMesaFeatureWriter(this, sft, indices, None, atomic = false)
  }

  /**
   * Cleanup any open connections, etc. Equivalent to java.io.Closeable.close()
   *
   * @see org.geotools.data.DataAccess#dispose()
   */
  override def dispose(): Unit = {
    Try(GeoMesaDataStore.liveStores.get(VersionKey(config.catalog, getClass)).remove(this))
    CloseWithLogging(stats)
    config.audit.foreach { case (writer, _, _) => CloseWithLogging(writer) }
    super.dispose()
  }

  // end methods from org.geotools.api.data.DataStore

  // other public methods

  /**
   * Gets the query plan for a given query. The query plan consists of the tables, ranges, iterators etc
   * required to run a query against the data store.
   *
   * @param query query to execute
   * @param index hint on the index to use to satisfy the query
   * @return query plans
   */
  def getQueryPlan(
      query: Query,
      index: Option[String] = None,
      explainer: Explainer = new ExplainLogging): Seq[QueryPlan[DS]] = {
    require(query.getTypeName != null, "Type name is required in the query")
    val sft = getSchema(query.getTypeName)
    if (sft == null) {
      throw new IOException(s"Schema '${query.getTypeName}' has not been initialized. Please call 'createSchema' first.")
    }
    queryPlanner.planQuery(sft, query, index, explainer)
  }

  /**
    * Gets the geomesa version
    *
    * @return client version
    */
  def getClientVersion: SemanticVersion = SemanticVersion(GeoMesaProperties.ProjectVersion, lenient = true)

  /**
    * Gets the geomesa version
    *
    * @return iterator version, if data store has iterators
    */
  def getDistributedVersion: Option[SemanticVersion] = {
    GeoMesaDataStore.versions.get(VersionKey(this)).get() match {
      case Right(v) => v
      case Left(e)  => throw e
    }
  }

  /**
   * Checks a simple feature type against an existing schema
   *
   * @param typeName type name
   * @param sft udpated simple feature type
   * @return
   */
  def checkSchemaCompatibility(typeName: String, sft: SimpleFeatureType): SchemaCompatibility = {
    val previous = getSchema(typeName)
    if (previous == null) {
      new SchemaCompatibility.DoesNotExist(this, sft)
    } else {
      validateSchemaUpdate(previous, sft) match {
        case Right(TypeComparison.Compatible(extension, renamed, _)) =>
          val copy = SimpleFeatureTypes.copy(sft)
          updateSchemaUserData(copy, previous)
          if (!extension && !renamed && copy.getUserData == previous.getUserData) {
            SchemaCompatibility.Unchanged
          } else {
            new SchemaCompatibility.Compatible(this, typeName, sft)
          }

        case Left(e) =>
          SchemaCompatibility.Incompatible(e)
      }
    }
  }

  // end public methods

  /**
   * Updates the user data for a schema update prior to persistence. Handles converting existing
   * user data based on new/updated attributes, copying existing user data over, etc. Feature
   * type will be updated in place
   *
   * @param sft schema update
   * @param previous existing schema
   * @return
   */
  private def updateSchemaUserData(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {

    // check for column renaming
    val colMap = getColumnMap(previous, sft)

    def remapCol(name: String): String = colMap.getOrElse(name, name)

    // check for attributes flagged 'index' and convert them to sft-level user data
    def indexed(d: AttributeDescriptor): Boolean = {
      d.getUserData.get(AttributeOptions.OptIndex) match {
        case i: String if Seq("true", "full").exists(_.equalsIgnoreCase(i)) => true
        case i if i == null || Seq("false", "none").exists(_.equalsIgnoreCase(i.toString)) => false
        case i => throw new IllegalArgumentException(s"Configured index coverage '$i' is not valid: expected 'true'")
      }
    }
    sft.getAttributeDescriptors.asScala.foreach { d =>
      if (indexed(d)) {
        val existing = {
          val explicit = sft.getIndices
          if (explicit.nonEmpty) { explicit } else { previous.getIndices }
        }
        if (!existing.exists(e => e.name == AttributeIndex.name && remapCol(e.attributes.head) == d.getLocalName)) {
          val fields = Seq(d.getLocalName) ++ Option(sft.getGeomField) ++ sft.getDtgField
          val id = IndexId(AttributeIndex.name, AttributeIndex.version, fields, IndexMode.ReadWrite)
          sft.setIndices(existing :+ id)
        }
      }
    }

    // check for new indices and 'enabled indices' changes
    val indices = {
      val enabled = if (!sft.getUserData.containsKey(Configs.EnabledIndices)) { Seq.empty } else {
        GeoMesaFeatureIndexFactory.indices(sft)
      }
      val remapped = (previous.getIndices ++ sft.getIndices).map(i => i.copy(attributes = i.attributes.map(remapCol)))
      (remapped ++ enabled).foldLeft(Seq.empty[IndexId]) { (sum, next) =>
        // note: ignore index version
        if (sum.exists(i => i.name == next.name && i.attributes == next.attributes)) { sum } else { sum :+ next }
      }
    }
    if (indices != previous.getIndices) {
      sft.setIndices(indices)
    }

    // preserve any existing user data but overwrite any keys we redefine
    val userData = new java.util.HashMap[AnyRef, AnyRef](previous.getUserData)
    userData.putAll(sft.getUserData)
    sft.getUserData.putAll(userData)
    // remove enabled indices as we don't need to persist it
    sft.getUserData.remove(Configs.EnabledIndices)
    // remove any null/empty keys as a way to delete existing user data
    sft.getUserData.asScala.collect { case (k, null | "") => k }.foreach(sft.getUserData.remove)
    // remove any 'index' flags in the attribute user data - we need them above for checking enabled indices
    sft.getAttributeDescriptors.asScala.foreach(_.getUserData.remove(AttributeOptions.OptIndex))
  }

  /**
   * Gets a map of column renames, for use during schema updates
   *
   * @param previous previous feature type
   * @param sft updated feature type
   * @return map of old name -> new name
   */
  private def getColumnMap(previous: SimpleFeatureType, sft: SimpleFeatureType): Map[String, String] = {
    previous.getAttributeDescriptors.asScala.zipWithIndex.toMap.flatMap { case (prev, i) =>
      val cur = sft.getDescriptor(i)
      if (prev.getLocalName != cur.getLocalName) {
        Map(prev.getLocalName -> cur.getLocalName)
      } else {
        Map.empty[String, String]
      }
    }
  }
}

object GeoMesaDataStore extends LazyLogging {

  import org.locationtech.geomesa.index.conf.SchemaProperties.{CheckDistributedVersion, ValidateDistributedClasspath}

  import scala.collection.JavaConverters._

  private val liveStores = new ConcurrentHashMap[VersionKey, java.util.Set[GeoMesaDataStore[_]]]()

  private val loader: AsyncCacheLoader[VersionKey, Either[Exception, Option[SemanticVersion]]] =
    new CacheLoader[VersionKey, Either[Exception, Option[SemanticVersion]]]() {
      override def load(key: VersionKey): Either[Exception, Option[SemanticVersion]] = {
        if (CheckDistributedVersion.toBoolean.contains(false)) { Right(None) } else {
          val ds = Option(liveStores.get(key)).flatMap(_.asScala.find(_.getTypeNames.nonEmpty)).orNull
          if (ds == null) {
            // short-circuit load - should try again next time cache is accessed
            throw new RuntimeException("Can't load remote versions if there are no feature types")
          }
          val clientVersion = ds.getClientVersion
          // use lenient parsing to account for versions like 1.3.5.1
          val iterVersions = ds.loadIteratorVersions.map(v => SemanticVersion(v, lenient = true))

          def message: String = "Classpath errors detected: configured server-side iterators do not match " +
              s"client version. Client version: $clientVersion, server versions: ${iterVersions.mkString(", ")}"

          // take the newest one if there are multiple - probably an update went partially awry, so it's
          // likely to match more tablet servers than the lower version
          val version = iterVersions.reduceLeftOption((left, right) => if (right > left) { right } else { left })

          // ensure matching versions
          // return an error if the user has enabled strict checking and it's not a patch/pre-release version mismatch
          // otherwise just log a warning
          if (iterVersions.forall(_ == clientVersion)) {
            Right(version)
          } else if (ValidateDistributedClasspath.toBoolean.contains(false) ||
              iterVersions.forall(MinorOrdering.compare(_, clientVersion) == 0)) {
            logger.warn(message)
            Right(version)
          } else {
            Left(new RuntimeException(s"$message. You may override this check by setting the system property " +
                s"'-D${ValidateDistributedClasspath.property}=false'"))
          }
        }
      }
    }

  private val versions: AsyncLoadingCache[VersionKey, Either[Exception, Option[SemanticVersion]]] =
    Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.DAYS).buildAsync(loader)

  /**
    * Kick off an asynchronous call to load remote iterator versions
    *
    * @param ds datastore
    */
  def initRemoteVersion(ds: GeoMesaDataStore[_]): Unit = {
    val key = VersionKey(ds)
    val loader = new java.util.function.Function[VersionKey, java.util.Set[GeoMesaDataStore[_]]]() {
      override def apply(t: VersionKey): java.util.Set[GeoMesaDataStore[_]] =
        Collections.newSetFromMap(new ConcurrentHashMap[GeoMesaDataStore[_], java.lang.Boolean])
    }
    liveStores.computeIfAbsent(key, loader).add(ds)
    // can't get remote version if there aren't any tables
    if (ds.getTypeNames.length > 0) {
      versions.get(key)
    }
  }

  sealed trait SchemaCompatibility {

    /**
     * Ensures that the schema matches the existing schema in the data store, or throws an error if
     * the schemas are incompatible
     */
    def apply(): Unit
  }

  object SchemaCompatibility {

    /**
     * Indicates that the schema is equal to the existing schema in the data store
     */
    case object Unchanged extends SchemaCompatibility {
      override def apply(): Unit = {}
    }

    /**
     * Indicates that the schema does not exist in the data store
     *
     * @param ds data store
     * @param sft schema
     */
    class DoesNotExist(ds: GeoMesaDataStore[_], val sft: SimpleFeatureType) extends SchemaCompatibility {
      override def apply(): Unit = ds.createSchema(sft)
    }

    object DoesNotExist {
      def unapply(arg: DoesNotExist): Option[SimpleFeatureType] = Some(arg.sft)
    }

    /**
     * Indicates that the schema is not equal to the existing schema, but is compatible
     *
     * @param ds data store
     * @param typeName type name
     * @param update the updated schema with all appropriate metadata
     */
    class Compatible(ds: GeoMesaDataStore[_], val typeName: String, val update: SimpleFeatureType)
        extends SchemaCompatibility {
      override def apply(): Unit = ds.updateSchema(typeName, update)
    }

    object Compatible {
      def unapply(arg: Compatible): Option[(String, SimpleFeatureType)] = Some(arg.typeName, arg.update)
    }

    /**
     * Indicates that the schema is not compatible with the existing schema in the data store
     *
     * @param error error message
     */
    case class Incompatible(error: Throwable) extends SchemaCompatibility {
      override def apply(): Unit = throw error
    }
  }

  /**
    * Cache key that for looking up remote versions
    */
  private case class VersionKey(catalog: String, clas: Class[_])

  private object VersionKey{
    def apply(ds: GeoMesaDataStore[_]): VersionKey = VersionKey(ds.config.catalog, ds.getClass)
  }
}
