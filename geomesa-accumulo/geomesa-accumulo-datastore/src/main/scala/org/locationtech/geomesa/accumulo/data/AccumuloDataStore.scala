/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/


package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client._
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.security.UserGroupInformation
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.AccumuloDataStoreConfig
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.EmptyPlan
import org.locationtech.geomesa.accumulo.data.stats._
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.{AgeOffIterator, DtgAgeOffIterator, ProjectVersionIterator, VisibilityIterator}
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryStrategy}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.metadata.MetadataStringSerializer
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.zk.ZookeeperLocking
import org.locationtech.geomesa.utils.conf.FeatureExpiration.{FeatureTimeExpiration, IngestTimeExpiration}
import org.locationtech.geomesa.utils.conf.{FeatureExpiration, IndexId}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.OverrideDtgJoin
import org.locationtech.geomesa.utils.hadoop.HadoopUtils
import org.locationtech.geomesa.utils.index.{GeoMesaSchemaValidator, IndexCoverage, IndexMode, VisibilityLevel}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import scala.util.control.NonFatal

/**
 * This class handles DataStores which are stored in Accumulo Tables. To be clear, one table may
 * contain multiple features addressed by their featureName.
 *
 * @param client Accumulo client
 * @param config configuration values
 */
class AccumuloDataStore(val client: AccumuloClient, override val config: AccumuloDataStoreConfig)
    extends GeoMesaDataStore[AccumuloDataStore](config) with ZookeeperLocking {

  import scala.collection.JavaConverters._

  override val metadata = new AccumuloBackedMetadata(client, config.catalog, MetadataStringSerializer, config.queries.consistency)

  override val adapter: AccumuloIndexAdapter = new AccumuloIndexAdapter(this)

  override val stats: AccumuloGeoMesaStats = AccumuloGeoMesaStats(this)

  override protected def zookeepers: String =
    client.properties().getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey)

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

  @deprecated("Use `client`")
  def connector: AccumuloClient = client

  override def delete(): Unit = {
    // note: don't delete the query audit table
    val toDelete = getTypeNames.toSeq.flatMap(getAllTableNames).distinct.filter(_ != config.auditWriter.table)
    adapter.deleteTables(toDelete)
  }

  override def getAllTableNames(typeName: String): Seq[String] = {
    val others = Seq(stats.metadata.table) :+ config.auditWriter.table
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
    // just check the first table available
    val versions = getTypeNames.iterator.flatMap { typeName =>
      getAllIndexTableNames(typeName).iterator.flatMap { table =>
        try {
          if (client.tableOperations().exists(table)) {
            WithClose(client.createScanner(table, new Authorizations())) { scanner =>
              config.queries.consistency.foreach(scanner.setConsistencyLevel)
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
    versions.find(_ != null).toSet
  }

  override protected def preSchemaCreate(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.index.conf.SchemaProperties.ValidateDistributedClasspath

    // call super first so that user data keys are updated
    super.preSchemaCreate(sft)

    def getNamespace(prefix: String): String = prefix.indexOf('.') match {
      case -1 => ""
      case i  => prefix.substring(0, i)
    }

    val prefixes = Seq(config.catalog) ++ sft.getIndices.flatMap(i => sft.getTablePrefix(i.name))
    prefixes.map(getNamespace).distinct.foreach { namespace =>
      if (namespace.nonEmpty) {
        adapter.ensureNamespaceExists(namespace)
      }
      // validate that the accumulo runtime is available
      val canLoad = client.namespaceOperations().testClassLoad(namespace,
        classOf[ProjectVersionIterator].getName, classOf[SortedKeyValueIterator[_, _]].getName)

      if (!canLoad) {
        val msg = s"Could not load GeoMesa distributed code from the Accumulo classpath"
        logger.error(s"$msg for catalog ${config.catalog}")
        if (ValidateDistributedClasspath.toBoolean.contains(true)) {
          val nsMsg = if (namespace.isEmpty) { "" } else { s" for the namespace '$namespace'" }
          throw new RuntimeException(s"$msg. You may override this check by setting the system property " +
            s"'${ValidateDistributedClasspath.property}=false'. Otherwise, please verify that the appropriate " +
            s"JARs are installed$nsMsg - see https://www.geomesa.org/documentation/stable/user/accumulo/install.html" +
            "#installing-the-accumulo-distributed-runtime-library")
        }
      }
    }

    if (sft.getVisibilityLevel == VisibilityLevel.Attribute && sft.getAttributeCount > 255) {
      throw new IllegalArgumentException("Attribute level visibility only supports up to 255 attributes")
    }

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
      adapter.ensureTableExists(stats.metadata.table)
      stats.configureStatCombiner(client, sft)
    }
    sft.getFeatureExpiration.foreach {
      case IngestTimeExpiration(ttl) =>
        val tableOps = client.tableOperations()
        getAllIndexTableNames(sft.getTypeName).filter(tableOps.exists).foreach { table =>
          AgeOffIterator.set(tableOps, table, sft, ttl)
        }

      case FeatureTimeExpiration(dtg, _, ttl) =>
        val tableOps = client.tableOperations()
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
      val tableOps = client.tableOperations()
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
      stats.removeStatCombiner(client, previous)
    }
    if (sft.statsEnabled) {
      adapter.ensureTableExists(stats.metadata.table)
      stats.configureStatCombiner(client, sft)
    }

    val tableOps = client.tableOperations()
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

  // methods specific to accumulo

  /**
   * Gets a query plan that can be satisfied via AccumuloInputFormat - e.g. only 1 table and configuration.
   *
   * @param query query
   * @return
   */
  def getSingleQueryPlan(query: Query): AccumuloQueryPlan = {
    // disable join plans as those have multiple tables
    JoinIndex.AllowJoinPlans.set(false)

    try {
      lazy val fallbackIndex = {
        val schema = getSchema(query.getTypeName)
        manager.indices(schema, IndexMode.Read).headOption.getOrElse {
          throw new IllegalStateException(s"Schema '${schema.getTypeName}' does not have any readable indices")
        }
      }

      val queryPlans = getQueryPlan(query)

      if (queryPlans.isEmpty) {
        val filter =
          FilterStrategy(fallbackIndex, None, Some(Filter.EXCLUDE), temporal = false, Float.PositiveInfinity, query.getHints)
        EmptyPlan(QueryStrategy(filter, Seq.empty, Seq.empty, Seq.empty, filter.filter, None))
      } else {
        val qps =
          if (queryPlans.lengthCompare(1) == 0) { queryPlans } else {
            // this query requires multiple scans, which we can't execute from some input formats
            // instead, fall back to a full table scan
            logger.warn("Desired query plan requires multiple scans - falling back to full table scan")
            getQueryPlan(query, Some(fallbackIndex.identifier))
          }
        if (qps.lengthCompare(1) > 0 || qps.exists(_.tables.lengthCompare(1) > 0)) {
          logger.error("The query being executed requires multiple scans, which is not currently " +
            "supported by GeoMesa. Your result set will be partially incomplete. " +
            s"Query: ${FilterHelper.toString(query.getFilter)}")
        }
        qps.head
      }
    } finally {
      // make sure we reset the thread locals
      JoinIndex.AllowJoinPlans.remove()
    }
  }

  override def dispose(): Unit = {
    try {
      super.dispose()
    } finally {
      CloseWithLogging(kerberosTgtRenewer.toSeq ++ Seq(client))
    }
  }
}
