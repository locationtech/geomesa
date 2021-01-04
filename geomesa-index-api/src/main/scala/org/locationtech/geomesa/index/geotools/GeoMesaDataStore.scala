/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.io.IOException
import java.util.Collections
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.api.{IndexManager, _}
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore.VersionKey
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata.ATTRIBUTES_KEY
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.utils.conf.SemanticVersion.MinorOrdering
import org.locationtech.geomesa.utils.conf.{GeoMesaProperties, IndexId, SemanticVersion}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{AttributeOptions, Configs, InternalConfigs}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

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

  val queryPlanner: QueryPlanner[DS] = new QueryPlanner[DS](this)

  val manager: IndexManager = new IndexManager(this)

  @deprecated
  protected def catalog: String = config.catalog

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
  def delete(): Unit = adapter.deleteTables(getTypeNames.flatMap(getAllTableNames).distinct)

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
    import Configs.{TABLE_SPLITTER, TABLE_SPLITTER_OPTS}
    import InternalConfigs.{PARTITION_SPLITTER, PARTITION_SPLITTER_OPTS}

    // check for old enabled indices and re-map them
    SimpleFeatureTypes.Configs.ENABLED_INDEX_OPTS.drop(1).find(sft.getUserData.containsKey).foreach { key =>
      sft.getUserData.put(SimpleFeatureTypes.Configs.ENABLED_INDICES, sft.getUserData.remove(key))
    }

    // validate column groups
    adapter.groups.validate(sft)

    // disable table sharing, no longer supported
    // noinspection ScalaDeprecation
    if (sft.isTableSharing) {
      logger.warn("Table sharing is no longer supported - disabling table sharing")
      sft.getUserData.remove(Configs.TABLE_SHARING_KEY)
      sft.getUserData.remove(InternalConfigs.SHARING_PREFIX_KEY)
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
    sft.getUserData.remove(SimpleFeatureTypes.Configs.ENABLED_INDICES)
    // remove any 'index' flags in the attribute metadata, they have already been captured in the indices above
    sft.getAttributeDescriptors.asScala.foreach(_.getUserData.remove(AttributeOptions.OPT_INDEX))

    // for partitioned schemas, persist the table partitioning keys
    if (TablePartition.partitioned(sft)) {
      Seq((TABLE_SPLITTER, PARTITION_SPLITTER), (TABLE_SPLITTER_OPTS, PARTITION_SPLITTER_OPTS)).foreach {
        case (from, to) => Option(sft.getUserData.get(from)).foreach(sft.getUserData.put(to, _))
      }
    }
  }

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaUpdate(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    // check for attributes flagged 'index' and convert them to sft-level user data
    sft.getAttributeDescriptors.asScala.foreach { d =>
      val index = d.getUserData.remove(AttributeOptions.OPT_INDEX).asInstanceOf[String]
      if (index == null || index.equalsIgnoreCase(IndexCoverage.NONE.toString) || index.equalsIgnoreCase("false")) {
        // no-op
      } else if (index.equalsIgnoreCase(IndexCoverage.FULL.toString) || java.lang.Boolean.valueOf(index)) {
        val fields = Seq(d.getLocalName) ++ Option(sft.getGeomField) ++ sft.getDtgField
        val attribute = IndexId(AttributeIndex.name, AttributeIndex.version, fields, IndexMode.ReadWrite)
        val existing = sft.getIndices.map(GeoMesaFeatureIndex.identifier)
        if (!existing.contains(GeoMesaFeatureIndex.identifier(attribute))) {
          sft.setIndices(sft.getIndices :+ attribute)
        }
      } else {
        throw new IllegalArgumentException(s"Configured index coverage '$index' is not valid: expected " +
            IndexCoverage.FULL.toString)
      }
    }

    // try to create the new indices to ensure they are valid for the sft
    val previousIndices = previous.getIndices.map(GeoMesaFeatureIndex.identifier)
    val newIndices = sft.getIndices.filterNot(i => previousIndices.contains(GeoMesaFeatureIndex.identifier(i)))
    if (newIndices.nonEmpty) {
      try { GeoMesaFeatureIndexFactory.create(this, sft, newIndices) } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Error configuring new feature index:", e)
      }
    }
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
    val old = previous.getIndices.map(GeoMesaFeatureIndex.identifier)
    val added = manager.indices(sft).filterNot(i => old.contains(i.identifier))
    if (TablePartition.partitioned(sft)) {
      logger.debug(s"Delaying creation of partitioned indices ${added.map(_.identifier).mkString(", ")}")
    } else {
      logger.debug(s"Creating indices ${added.map(_.identifier).mkString(", ")}")
      added.foreach(index => adapter.createTable(index, None, index.getSplits(None)))
    }
  }

  // delete the index tables
  override protected def onSchemaDeleted(sft: SimpleFeatureType): Unit = {
    // noinspection ScalaDeprecation
    if (sft.isTableSharing && getTypeNames.exists(t => t != sft.getTypeName && getSchema(t).isTableSharing)) {
      manager.indices(sft).par.foreach { index =>
        if (index.keySpace.sharing.isEmpty) {
          adapter.deleteTables(index.deleteTableNames(None))
        } else {
          adapter.clearTables(index.deleteTableNames(None), Some(index.keySpace.sharing))
        }
      }
    } else {
      manager.indices(sft).par.foreach(index => adapter.deleteTables(index.deleteTableNames(None)))
    }
    stats.clearStats(sft)
  }

  // methods from org.geotools.data.DataStore

  /**
   * @see org.geotools.data.DataStore#getSchema(java.lang.String)
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
                s"manually edit the metadata for '${InternalConfigs.INDEX_VERSIONS}' to exclude the invalid indices.", e)
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

      // get the remote version if it's available, but don't wait for it
      GeoMesaDataStore.versions.get(new VersionKey(this)).getNow(Right(None)) match {
        case Left(e) => throw e
        case Right(version) =>
          version.foreach { v =>
            val userData = Collections.singletonMap[AnyRef, AnyRef](InternalConfigs.REMOTE_VERSION, v.toString)
            sft = SimpleFeatureTypes.immutable(sft, userData)
          }
      }
    }
    sft
  }

  /**
   * @see org.geotools.data.DataStore#getFeatureSource(java.lang.String)
   * @param typeName simple feature type name
   * @return featureStore, suitable for reading and writing
   */
  override def getFeatureSource(typeName: String): GeoMesaFeatureStore = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    if (config.caching) {
      new GeoMesaFeatureStore(this, sft, queryPlanner) with GeoMesaFeatureSource.CachingFeatureSource
    } else {
      new GeoMesaFeatureStore(this, sft, queryPlanner)
    }
  }

  /**
   * @see org.geotools.data.DataStore#getFeatureReader(org.geotools.data.Query, org.geotools.data.Transaction)
   * @param query query to execute
   * @param transaction transaction to use (currently ignored)
   * @return feature reader
   */
  override def getFeatureReader(query: Query, transaction: Transaction): GeoMesaFeatureReader = {
    require(query.getTypeName != null, "Type name is required in the query")
    val sft = getSchema(query.getTypeName)
    if (sft == null) {
      throw new IOException(s"Schema '${query.getTypeName}' has not been initialized. Please call 'createSchema' first.")
    }
    GeoMesaFeatureReader(sft, query, queryPlanner, config.queryTimeout, config.audit)
  }

  /**
   * Create a general purpose writer that is capable of updates and deletes.
   * Does <b>not</b> allow inserts.
   *
   * @see org.geotools.data.DataStore#getFeatureWriter(java.lang.String, org.opengis.filter.Filter,
   *        org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param filter cql filter to select features for update/delete
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): FlushableFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    if (transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    GeoMesaFeatureWriter(this, sft, manager.indices(sft, mode = IndexMode.Write), Some(filter))
  }

  /**
   * Creates a feature writer only for writing - does not allow updates or deletes.
   *
   * @see org.geotools.data.DataStore#getFeatureWriterAppend(java.lang.String, org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): FlushableFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    if (transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    GeoMesaFeatureWriter(this, sft, manager.indices(sft, mode = IndexMode.Write), None)
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
    GeoMesaFeatureWriter(this, sft, indices, None)
  }

  /**
   * Cleanup any open connections, etc. Equivalent to java.io.Closeable.close()
   *
   * @see org.geotools.data.DataAccess#dispose()
   */
  override def dispose(): Unit = {
    CloseWithLogging(stats)
    config.audit.foreach { case (writer, _, _) => CloseWithLogging(writer) }
    super.dispose()
  }

  // end methods from org.geotools.data.DataStore

  // other public methods

  /**
   * Gets the query plan for a given query. The query plan consists of the tables, ranges, iterators etc
   * required to run a query against the data store.
   *
   * @param query query to execute
   * @param index hint on the index to use to satisfy the query
   * @return query plans
   */
  def getQueryPlan(query: Query,
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
    GeoMesaDataStore.versions.get(new VersionKey(this)).get() match {
      case Right(v) => v
      case Left(e)  => throw e
    }
  }

  @deprecated("use getDistributedVersion")
  def getDistributeVersion: Option[SemanticVersion] = getDistributedVersion

  /**
    * Gets the geomesa version
    *
    * @return (client version, iterator version)
    */
  @deprecated("use getClientVersion and getDistributedVersion")
  def getVersion: (String, Set[String]) = (GeoMesaProperties.ProjectVersion, loadIteratorVersions)

  // end public methods
}

object GeoMesaDataStore extends LazyLogging {

  import org.locationtech.geomesa.index.conf.SchemaProperties.{CheckDistributedVersion, ValidateDistributedClasspath}

  private val loader = new CacheLoader[VersionKey, Either[Exception, Option[SemanticVersion]]]() {
    override def load(key: VersionKey): Either[Exception, Option[SemanticVersion]] = {
      if (key.ds.getTypeNames.length == 0) {
        // short-circuit load - should try again next time cache is accessed
        throw new RuntimeException("Can't load remote versions if there are no feature types")
      }
      if (CheckDistributedVersion.toBoolean.contains(false)) { Right(None) } else {
        val clientVersion = key.ds.getClientVersion
        // use lenient parsing to account for versions like 1.3.5.1
        val iterVersions = key.ds.loadIteratorVersions.map(v => SemanticVersion(v, lenient = true))

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

  private val versions = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.DAYS)
      .buildAsync[VersionKey, Either[Exception, Option[SemanticVersion]]](loader)

  /**
    * Kick off an asynchronous call to load remote iterator versions
    *
    * @param ds datastore
    */
  def initRemoteVersion(ds: GeoMesaDataStore[_]): Unit = {
    // can't get remote version if there aren't any tables
    if (ds.getTypeNames.length > 0) {
      versions.get(new VersionKey(ds))
    }
  }

  /**
    * Cache key that bases equality on data store class and catalog, but allows for loading remote version
    * from datastore
    *
    * @param ds data store
    */
  private class VersionKey(val ds: GeoMesaDataStore[_]) {

    override def equals(other: Any): Boolean = other match {
      case that: VersionKey => ds.config.catalog == that.ds.config.catalog && ds.getClass == that.ds.getClass
      case _ => false
    }

    override def hashCode(): Int =
      Seq(ds.config.catalog, ds.getClass).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
