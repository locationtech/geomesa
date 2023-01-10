/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.accumulo.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.security.UserGroupInformation
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.accumulo.audit.AccumuloAuditService
import org.locationtech.geomesa.accumulo.data.AccumuloBackedMetadata.SingleRowAccumuloMetadata
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.AccumuloDataStoreConfig
import org.locationtech.geomesa.accumulo.data.stats._
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.{AgeOffIterator, DtgAgeOffIterator, ProjectVersionIterator, VisibilityIterator}
import org.locationtech.geomesa.accumulo.util.TableUtils
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.conf.FeatureExpiration.{FeatureTimeExpiration, IngestTimeExpiration}
import org.locationtech.geomesa.utils.conf.{FeatureExpiration, IndexId}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.OverrideDtgJoin
import org.locationtech.geomesa.utils.hadoop.HadoopUtils
import org.locationtech.geomesa.utils.index.{GeoMesaSchemaValidator, IndexMode, VisibilityLevel}
<<<<<<< HEAD
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, HadoopUtils, WithClose}
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
import org.locationtech.geomesa.utils.stats.{IndexCoverage, Stat}
import org.locationtech.geomesa.utils.zk.ZookeeperLocking

import java.util.Locale
import scala.util.control.NonFatal

/**
 * This class handles DataStores which are stored in Accumulo Tables. To be clear, one table may
 * contain multiple features addressed by their featureName.
 *
 * @param connector Accumulo connector
 * @param config configuration values
 */
class AccumuloDataStore(val connector: AccumuloClient, override val config: AccumuloDataStoreConfig)
    extends GeoMesaDataStore[AccumuloDataStore](config) with ZookeeperLocking {

  import scala.collection.JavaConverters._

  override val metadata = new AccumuloBackedMetadata(connector, config.catalog, MetadataStringSerializer)

  private val oldMetadata = new SingleRowAccumuloMetadata(metadata)

  override val adapter: AccumuloIndexAdapter = new AccumuloIndexAdapter(this)

  override val stats: AccumuloGeoMesaStats = AccumuloGeoMesaStats(this)

  override protected def zookeepers: String =
    connector.properties().getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey)

  // If on a secured cluster, create a thread to periodically renew Kerberos tgt
  private val kerberosTgtRenewer = {
    val enabled = try { UserGroupInformation.isSecurityEnabled } catch {
      case e: Throwable => logger.error("Error checking for hadoop security", e); false
    }
    if (enabled) { Some(HadoopUtils.kerberosTicketRenewer()) } else { None }
  }

  // some convenience operations

  /**
    * Gets the authorizations for the current user. This may change, so the results shouldn't be cached
    *
    * @return
    */
  def auths: Authorizations = new Authorizations(config.authProvider.getAuthorizations.asScala.toSeq: _*)

  override def delete(): Unit = {
    // note: don't delete the query audit table
    val all = getTypeNames.toSeq.flatMap(getAllTableNames).distinct
    val toDelete = config.audit match {
      case Some((a: AccumuloAuditService, _, _)) => all.filter(_ != a.table)
      case _ => all
    }
    adapter.deleteTables(toDelete)
  }

  override def getAllTableNames(typeName: String): Seq[String] = {
    val others = Seq(stats.metadata.table) ++ config.audit.map(_._1.asInstanceOf[AccumuloAuditService].table).toSeq
    super.getAllTableNames(typeName) ++ others
  }

  // data store hooks

  override protected def transitionIndices(sft: SimpleFeatureType): Unit = {
    // note: versions already correspond to accumulo index versions
    val dtg = sft.getDtgField.toSeq
    val geom = Option(sft.getGeomField).toSeq
    val indices = sft.getIndices.flatMap {
      case id if id.name == IdIndex.name  => Seq(id) // no update needed
      case id if id.name == "records"     => Seq(id.copy(name = IdIndex.name))
      case id if id.name == Z3Index.name  => Seq(id.copy(attributes = geom ++ dtg))
      case id if id.name == XZ3Index.name => Seq(id.copy(attributes = geom ++ dtg))
      case id if id.name == Z2Index.name  => Seq(id.copy(attributes = geom))
      case id if id.name == XZ2Index.name => Seq(id.copy(attributes = geom))
      case id if id.name == AttributeIndex.name =>
        lazy val fields = if (id.version < 4) { dtg } else { geom ++ dtg }
        sft.getAttributeDescriptors.asScala.flatMap { d =>
          val index = d.getUserData.remove(AttributeOptions.OptIndex).asInstanceOf[String]
          if (index == null || index.equalsIgnoreCase(IndexCoverage.NONE.toString) || index.equalsIgnoreCase("false")) {
            Seq.empty
          } else if (index.equalsIgnoreCase(IndexCoverage.FULL.toString)) {
            Seq(id.copy(name = AttributeIndex.name, attributes = Seq(d.getLocalName) ++ fields))
          } else if (index.equalsIgnoreCase(IndexCoverage.JOIN.toString) || java.lang.Boolean.valueOf(index)) {
            Seq(id.copy(name = JoinIndex.name, attributes = Seq(d.getLocalName) ++ fields))
          } else {
            throw new IllegalStateException(s"Expected an index coverage or boolean but got: $index")
          }
        }
    }
    sft.setIndices(indices)
  }

  override protected def loadIteratorVersions: Set[String] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    // just check the first table available
    val versions = getTypeNames.iterator.flatMap { typeName =>
      getAllIndexTableNames(typeName).iterator.flatMap { table =>
        try {
          if (connector.tableOperations().exists(table)) {
            WithClose(connector.createScanner(table, new Authorizations())) { scanner =>
              ProjectVersionIterator.scanProjectVersion(scanner).iterator
            }
          } else {
            Iterator.empty
          }
        } catch {
          case NonFatal(_) => Iterator.empty
        }
      }
    }
    versions.headOption.toSet
  }

  override protected def preSchemaCreate(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.index.conf.SchemaProperties.ValidateDistributedClasspath

    // validate that the accumulo runtime is available
    val namespace = config.catalog.indexOf('.') match {
      case -1 => ""
      case i  => config.catalog.substring(0, i)
    }
    TableUtils.createNamespaceIfNeeded(connector, namespace)
    val canLoad = connector.namespaceOperations().testClassLoad(namespace,
      classOf[ProjectVersionIterator].getName, classOf[SortedKeyValueIterator[_, _]].getName)

    if (!canLoad) {
      val msg = s"Could not load GeoMesa distributed code from the Accumulo classpath for table '${config.catalog}'"
      logger.error(msg)
      if (ValidateDistributedClasspath.toBoolean.contains(true)) {
        val nsMsg = if (namespace.isEmpty) { "" } else { s" for the namespace '$namespace'" }
        throw new RuntimeException(s"$msg. You may override this check by setting the system property " +
            s"'${ValidateDistributedClasspath.property}=false'. Otherwise, please verify that the appropriate " +
            s"JARs are installed$nsMsg - see http://www.geomesa.org/documentation/user/accumulo/install.html" +
            "#installing-the-accumulo-distributed-runtime-library")
      }
    }

    if (sft.getVisibilityLevel == VisibilityLevel.Attribute && sft.getAttributeCount > 255) {
      throw new IllegalArgumentException("Attribute level visibility only supports up to 255 attributes")
    }

    super.preSchemaCreate(sft)

    // note: dtg should be set appropriately before calling this method
    sft.getDtgField.foreach { dtg =>
      if (sft.getIndices.exists(i => i.name == JoinIndex.name && i.attributes.headOption.contains(dtg))) {
        if (!GeoMesaSchemaValidator.declared(sft, OverrideDtgJoin)) {
          throw new IllegalArgumentException("Trying to create a schema with a partial (join) attribute index " +
              s"on the default date field '$dtg'. This may cause whole-world queries with time bounds to be much " +
              "slower. If this is intentional, you may override this check by putting Boolean.TRUE into the " +
              s"SimpleFeatureType user data under the key '$OverrideDtgJoin' before calling createSchema, or by " +
              s"setting the system property '$OverrideDtgJoin' to 'true'. Otherwise, please either specify a " +
              "full attribute index or remove it entirely.")
        }
      }
    }
  }

  override protected def onSchemaCreated(sft: SimpleFeatureType): Unit = {
    super.onSchemaCreated(sft)
    if (sft.statsEnabled) {
      // configure the stats combining iterator on the table for this sft
      stats.configureStatCombiner(connector, sft)
    }
    sft.getFeatureExpiration.foreach {
      case IngestTimeExpiration(ttl) =>
        val tableOps = connector.tableOperations()
        getAllIndexTableNames(sft.getTypeName).filter(tableOps.exists).foreach { table =>
          AgeOffIterator.set(tableOps, table, sft, ttl)
        }

      case FeatureTimeExpiration(dtg, _, ttl) =>
        val tableOps = connector.tableOperations()
        manager.indices(sft).foreach { index =>
          val indexSft = index match {
            case joinIndex: AttributeJoinIndex => joinIndex.indexSft
            case _ => sft
          }
          DtgAgeOffIterator.set(tableOps, indexSft, index, ttl, dtg)
        }

      case e =>
        throw new IllegalArgumentException(s"Unexpected feature expiration: $e")
    }
  }

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaUpdate(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    if (sft.getVisibilityLevel == VisibilityLevel.Attribute && sft.getAttributeCount > 255) {
      throw new IllegalArgumentException("Attribute level visibility only supports up to 255 attributes")
    }

    // check for attributes flagged 'index=join' and convert them to sft-level user data
    sft.getAttributeDescriptors.asScala.foreach { d =>
      val index = d.getUserData.get(AttributeOptions.OptIndex).asInstanceOf[String]
      if (index != null && index.equalsIgnoreCase(IndexCoverage.JOIN.toString)) {
        d.getUserData.remove(AttributeOptions.OptIndex) // remove it so it's not processed again
        val fields = Seq(d.getLocalName) ++ Option(sft.getGeomField) ++ sft.getDtgField.filter(_ != d.getLocalName)
        val attribute = IndexId(JoinIndex.name, JoinIndex.version, fields, IndexMode.ReadWrite)
        val existing = sft.getIndices.map(GeoMesaFeatureIndex.identifier)
        if (!existing.contains(GeoMesaFeatureIndex.identifier(attribute))) {
          sft.setIndices(sft.getIndices :+ attribute)
        }
      }
    }

    // check any previous age-off - previously age-off wasn't tied to the sft metadata
    if (!sft.isFeatureExpirationEnabled && !previous.isFeatureExpirationEnabled) {
      // explicitly set age-off in the feature type if found
      val tableOps = connector.tableOperations()
      val tables = getAllIndexTableNames(previous.getTypeName).filter(tableOps.exists)
      val ageOff = tables.foldLeft[Option[FeatureExpiration]](None) { (res, table) =>
        res.orElse(AgeOffIterator.expiry(tableOps, table))
      }
      val configured = ageOff.orElse {
        tables.foldLeft[Option[FeatureExpiration]](None) { (res, table) =>
          res.orElse(DtgAgeOffIterator.expiry(tableOps, previous, table))
        }
      }
      configured.foreach(sft.setFeatureExpiration)
    }

    super.preSchemaUpdate(sft, previous)
  }

  override protected def onSchemaUpdated(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    super.onSchemaUpdated(sft, previous)

    if (previous.statsEnabled) {
      stats.removeStatCombiner(connector, previous)
    }
    if (sft.statsEnabled) {
      stats.configureStatCombiner(connector, sft)
    }

    val tableOps = connector.tableOperations()
    val previousTables = getAllIndexTableNames(previous.getTypeName).filter(tableOps.exists)
    val tables = getAllIndexTableNames(sft.getTypeName).filter(tableOps.exists)

    if (previous.isVisibilityRequired != sft.isVisibilityRequired) {
      previousTables.foreach(VisibilityIterator.clear(tableOps, _))
      if (sft.isVisibilityRequired) {
        tables.foreach(VisibilityIterator.set(tableOps, _))
      }
    }

    previousTables.foreach { table =>
      AgeOffIterator.clear(tableOps, table)
      DtgAgeOffIterator.clear(tableOps, table)
    }

    sft.getFeatureExpiration.foreach {
      case IngestTimeExpiration(ttl) =>
        tables.foreach(AgeOffIterator.set(tableOps, _, sft, ttl))

      case FeatureTimeExpiration(dtg, _, ttl) =>
        manager.indices(sft).foreach { index =>
          val indexSft = index match {
            case joinIndex: AttributeJoinIndex => joinIndex.indexSft
            case _ => sft
          }
          DtgAgeOffIterator.set(tableOps, indexSft, index, ttl, dtg)
        }

      case e => throw new IllegalArgumentException(s"Unexpected feature expiration: $e")
    }
  }

  override def getQueryPlan(query: Query, index: Option[String], explainer: Explainer): Seq[AccumuloQueryPlan] =
    super.getQueryPlan(query, index, explainer).asInstanceOf[Seq[AccumuloQueryPlan]]

  // extensions and back-compatibility checks for core data store methods

  override def getTypeNames: Array[String] = super.getTypeNames ++ oldMetadata.getFeatureTypes

  // noinspection ScalaDeprecation
  override def getSchema(typeName: String): SimpleFeatureType = {
    var sft = super.getSchema(typeName)
    if (sft == null) {
      // check for old-style metadata and re-write it if necessary
      if (oldMetadata.getFeatureTypes.contains(typeName)) {
        val lock = acquireCatalogLock()
        try {
          if (oldMetadata.getFeatureTypes.contains(typeName)) {
            oldMetadata.migrate(typeName)
            new SingleRowAccumuloMetadata[Stat](stats.metadata).migrate(typeName)
          }
        } finally {
          lock.release()
        }
        sft = super.getSchema(typeName)
      }
    }
    if (sft != null) {
      // back compatible check for index versions
      if (sft.getIndices.isEmpty) {
        // make the sft temporarily mutable so we can update the keys
        sft = SimpleFeatureTypes.mutable(sft)
        // back compatible check if user data wasn't encoded with the sft
        if (!sft.getUserData.containsKey(AccumuloDataStore.DeprecatedSchemaVersionKey)) {
          metadata.read(typeName, "dtgfield").foreach(sft.setDtgField)
          sft.getUserData.put(AccumuloDataStore.DeprecatedSchemaVersionKey,
            metadata.readRequired(typeName, GeoMesaMetadata.VersionKey))

          // if no data is written, we default to 'false' in order to support old tables
          if (metadata.read(typeName, "tables.sharing").exists(_.toBoolean)) {
            sft.getUserData.put(SimpleFeatureTypes.Configs.TableSharing, "true")
            // use schema id if available or fall back to old type name for backwards compatibility
            val prefix = metadata.read(typeName, "id").getOrElse(s"${sft.getTypeName}~")
            sft.getUserData.put(SimpleFeatureTypes.InternalConfigs.TableSharingPrefix, prefix)
          }
          // noinspection deprecation
          SimpleFeatureTypes.Configs.ENABLED_INDEX_OPTS.foreach { i =>
            metadata.read(typeName, i).foreach(e => sft.getUserData.put(SimpleFeatureTypes.Configs.EnabledIndices, e))
          }
        }

        // set the enabled indices
        sft.setIndices(AccumuloDataStore.translateSchemaVersion(sft))

        // store the metadata and reload the sft again to validate indices
        val encoded = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
        metadata.insert(typeName, GeoMesaMetadata.AttributesKey, encoded)
        sft = super.getSchema(typeName)
      }

      // back compatibility check for stat configuration
      if (sft.statsEnabled && metadata.read(typeName, GeoMesaMetadata.StatsGenerationKey).isEmpty) {
        // configure the stats combining iterator - we only use this key for older data stores
        val configuredKey = "stats-configured"
        if (!metadata.read(typeName, configuredKey).contains("true")) {
          val lock = acquireCatalogLock()
          try {
            if (!metadata.read(typeName, configuredKey, cache = false).contains("true")) {
              stats.configureStatCombiner(connector, sft)
              metadata.insert(typeName, configuredKey, "true")
            }
          } finally {
            lock.release()
          }
        }
        // kick off asynchronous stats run for the existing data - this will set the stat date
        // this may get triggered more than once, but should only run one time
        val statsRunner = new StatsRunner(this)
        statsRunner.submit(sft)
        statsRunner.close()
      }
    }

    sft
  }

  override def dispose(): Unit = {
    try {
      super.dispose()
    } finally {
      CloseWithLogging(kerberosTgtRenewer.toSeq ++ Seq(connector))
    }
  }
}

object AccumuloDataStore extends LazyLogging {

  import scala.collection.JavaConverters._

  private val DeprecatedSchemaVersionKey = "geomesa.version"

  /**
    * Converts the old 'index schema' into the appropriate index identifiers
    *
    * @param sft simple feature type
    * @return
    */
  private def translateSchemaVersion(sft: SimpleFeatureType): Seq[IndexId] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    lazy val docs =
      "http://www.geomesa.org/documentation/user/jobs.html#updating-existing-data-to-the-latest-index-format"

    val geom = Option(sft.getGeomField)
    val dtg = sft.getDtgField

    val id = IndexId(IdIndex.name, -1, Seq.empty, IndexMode.ReadWrite)
    val z3 = for { g <- geom; d <- dtg } yield { IndexId(Z3Index.name, -1, Seq(g, d), IndexMode.ReadWrite) }
    val xz3 = for { g <- geom; d <- dtg } yield { IndexId(XZ3Index.name, 1, Seq(g, d), IndexMode.ReadWrite) }
    val z2 = geom.map(g => IndexId(Z2Index.name, -1, Seq(g), IndexMode.ReadWrite))
    val xz2 = geom.map(g => IndexId(XZ2Index.name, 1, Seq(g), IndexMode.ReadWrite))
    val attributes = sft.getAttributeDescriptors.asScala.flatMap { d =>
      val index = d.getUserData.remove(AttributeOptions.OptIndex).asInstanceOf[String]
      if (index == null || index.equalsIgnoreCase(IndexCoverage.NONE.toString) || index.equalsIgnoreCase("false")) {
        Seq.empty
      } else if (index.equalsIgnoreCase(IndexCoverage.FULL.toString)) {
        Seq(IndexId(AttributeIndex.name, -1, Seq(d.getLocalName) ++ dtg, IndexMode.ReadWrite))
      } else if (index.equalsIgnoreCase(IndexCoverage.JOIN.toString) || java.lang.Boolean.valueOf(index)) {
        Seq(IndexId(JoinIndex.name, -1, Seq(d.getLocalName) ++ dtg, IndexMode.ReadWrite))
      } else {
        throw new IllegalStateException(s"Expected an index coverage or boolean but got: $index")
      }
    }

    // note: 10 was the last valid value for CURRENT_SCHEMA_VERSION, which is no longer used except
    // to transition old schemas from the 1.2.5 era
    val version = {
      // noinspection ScalaDeprecation
      val string = sft.getUserData.remove(DeprecatedSchemaVersionKey).asInstanceOf[String]
      if (string != null) { string.toInt } else { 10 }
    }
    val indices: Seq[IndexId] = if (version > 8) {
      // note: version 9 was never in a release
      val zs = if (sft.isPoints) { z3.map(_.copy(version = 3)) ++ z2.map(_.copy(version = 2)) } else { xz3 ++ xz2 }
      zs.toSeq ++ Seq(id.copy(version = 2)) ++ attributes.map(_.copy(version = 3))
    } else if (version == 8) {
      z3.map(_.copy(version = 2)).toSeq ++ z2.map(_.copy(version = 1)) ++
          Seq(id.copy(version = 1)) ++ attributes.map(_.copy(version = 2))
    } else if (version > 5) {
      logger.warn("The GeoHash index is no longer supported. Some queries may take longer than normal. To " +
          s"update your data to a newer format, see $docs")
      val z = if (version == 7) { z3.map(_.copy(version = 2)) } else { z3.map(_.copy(version = 1)) }
      z.toSeq ++ Seq(id.copy(version = 1)) ++ attributes.map(_.copy(version = 2))
    } else {
      throw new NotImplementedError("This schema format is no longer supported. Please use " +
          s"GeoMesa 1.2.6 to update you data to a newer format. For more information, see $docs")
    }

    // noinspection deprecation
    SimpleFeatureTypes.Configs.ENABLED_INDEX_OPTS.map(sft.getUserData.get).find(_ != null) match {
      case None => indices
      case Some(enabled) =>
        // check for old index names
        val e = enabled.toString.toLowerCase(Locale.US).split(",").map(_.trim).filterNot(_.isEmpty).map {
          case "attr_idx" => AttributeIndex.name
          case "records" => IdIndex.name
          case i => i
        }
        indices.filter(i => e.contains(i.name))
    }
  }
}
