/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.identity.FeatureId
import org.geotools.data.simple._
import org.geotools.data.store.DecoratingDataStore
import org.geotools.data.{DataUtilities, DefaultTransaction}
import org.geotools.feature.FeatureCollection
import org.geotools.feature.collection.DecoratingSimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.jdbc.{JDBCDataStore, SQLDialect}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.{SftUserData, VisCol}
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose

import java.awt.RenderingHints
import java.sql.Connection
import java.util.concurrent.CompletionException
import javax.sql.DataSource

/**
 * Data store wrapper that manages a hidden per-row visibility column (`_vis`).
 *
 * The physical schema carries `_vis` as an ordinary attribute so that it flows through the sealed
 * `JDBCDataStore` insert/read machinery for free. This wrapper hides that column from callers:
 *
 *  - on `createSchema`, if the `pg.vis.enabled` user-data flag is explicitly `true`, a nullable
 *    `_vis` attribute is appended to the type before it's created
 *  - on read, features are re-typed to drop `_vis`; the MainView already filters rows by the caller's
 *    authorizations, and the visibility expression is surfaced into the feature user data
 *  - on write, the feature's visibility (from user data) is copied into `_vis`
 *
 * For any type that has no physical `_vis` column (pre-existing schemas, or vis explicitly disabled)
 * the wrapper is fully transparent
 *
 * @param delegate wrapped jdbc data store
 */
class PartitionedPostgisDataStore(delegate: JDBCDataStore) extends DecoratingDataStore(delegate) {

  import PartitionedPostgisDataStore._
  import PartitionedPostgisDialect.VisCol

  // cache of schemas with _vis attribute removed
  private val schemas =
    Caffeine.newBuilder().expireAfterWrite(TableBasedMetadata.Expiry.toJavaDuration.get)
      .build(new CacheLoader[String, SchemaType]() {
        override def load(typeName: String): SchemaType = {
          val underlying = delegate.getSchema(typeName)
          if (underlying == null) {
            null
          } else {
            val visCol = underlying.indexOf(VisCol)
            if (visCol == -1) {
              SchemaWithoutVis(underlying)
            } else {
              // verify vis is the last attribute, logic in other places depends on that
              require(visCol == underlying.getAttributeCount - 1, s"Expected $VisCol to be last attribute, but it was not")
              SchemaWithVis(removeVisFromSchema(underlying), underlying)
            }
          }
        }
      })

  // pass-through methods for jdbc ops
  def dialect: SQLDialect = delegate.dialect
  def getDatabaseSchema: String = delegate.getDatabaseSchema
  def getConnection(t: Transaction): Connection = delegate.getConnection(t)
  def getDataSource: DataSource = delegate.getDataSource

  /**
   * Re-create the PLPG/SQL procedures associated with a feature type. This can be used
   * to 'upgrade in place' if the code is changed.
   *
   * This can be used to alter mutable user data settings, but *cannot* be used to modify the attributes
   * of the feature type
   *
   * @param sft updated feature type
   */
  def upgrade(sft: SimpleFeatureType): Unit = {
    val existing = loadSchema(sft.getTypeName)
    if (existing == null) {
      throw new IllegalArgumentException(s"Schema does not exist: ${sft.getTypeName}")
    }
    val dialect = this.dialect match {
      case d: PartitionedPostgisDialect => d
      case d: PartitionedPostgisPsDialect => d.delegate
    }
    val upgrade = existing match {
      case _: SchemaWithoutVis => sft
      case s: SchemaWithVis =>
        val copy = SimpleFeatureTypes.copy(s.underling)
        copy.getUserData.putAll(sft.getUserData)
        copy
    }
    WithClose(new DefaultTransaction()) { tx =>
      WithClose(getConnection(tx)) { cx =>
        dialect.postCreateTable(getDatabaseSchema, upgrade, cx)
        tx.commit()
      }
    }
  }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    val sft = if (!SftUserData.VisEnabled.get(featureType)) { featureType } else { addVisToSchema(featureType) }
    delegate.createSchema(sft)
  }

  override def getSchema(typeName: String): SimpleFeatureType =
    Option(loadSchema(typeName)).fold[SimpleFeatureType](null) {
      case s: SchemaWithoutVis => s.sft
      case s: SchemaWithVis => s.userFacing
    }

  override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)

  override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit =
    updateSchema(typeName.getLocalPart, featureType)

  override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit = {
    super.updateSchema(typeName, featureType)
    schemas.invalidate(typeName)
  }

  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  override def removeSchema(typeName: String): Unit = {
    delegate.removeSchema(typeName)
    schemas.invalidate(typeName)
  }

  override def getFeatureSource(typeName: String): SimpleFeatureSource = {
    val source = delegate.getFeatureSource(typeName)
    loadSchema(typeName) match {
      case _: SchemaWithoutVis => source
      case s: SchemaWithVis =>
        source match {
          case store: SimpleFeatureStore => new VisSimpleFeatureStore(store, s.userFacing)
          case _ => new VisSimpleFeatureSource(source, s.userFacing)
        }
    }
  }

  override def getFeatureSource(typeName: Name): SimpleFeatureSource = getFeatureSource(typeName.getLocalPart)

  override def getFeatureReader(query: Query, tx: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    loadSchema(query.getTypeName) match {
      case _: SchemaWithoutVis => delegate.getFeatureReader(query, tx)
      case _: SchemaWithVis => new VisFeatureReader(delegate.getFeatureReader(addVisToTransform(query), tx))
    }
  }

  override def getFeatureWriter(typeName: String, filter: Filter, tx: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    val writer = delegate.getFeatureWriter(typeName, filter, tx)
    loadSchema(typeName) match {
      case _: SchemaWithoutVis => writer
      case s: SchemaWithVis => new VisFeatureWriter(writer, s.userFacing)
    }
  }

  override def getFeatureWriter(typeName: String, tx: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    val writer = delegate.getFeatureWriter(typeName, tx)
    loadSchema(typeName) match {
      case _: SchemaWithoutVis => writer
      case s: SchemaWithVis => new VisFeatureWriter(writer, s.userFacing)
    }
  }

  override def getFeatureWriterAppend(typeName: String, tx: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    val writer = delegate.getFeatureWriterAppend(typeName, tx)
    loadSchema(typeName) match {
      case _: SchemaWithoutVis => writer
      case s: SchemaWithVis => new VisFeatureWriter(writer, s.userFacing)
    }
  }

  /**
   * Helper to re-route cache completion exceptions to the underlying cause
   *
   * @param typeName feature type name
   * @return
   */
  private def loadSchema(typeName: String): SchemaType = {
    try { schemas.get(typeName) } catch {
      case e: CompletionException => throw e.getCause
    }
  }
}

object PartitionedPostgisDataStore {

  /**
   * Remove visibility from a schema, for user-facing types
   *
   * @param sft underlying feature type
   * @return
   */
  private def removeVisFromSchema(sft: SimpleFeatureType): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    builder.remove(PartitionedPostgisDialect.VisCol)
    val result = builder.buildFeatureType()
    result.getUserData.putAll(sft.getUserData)
    result
  }

  /**
   * Adds visibility to a schema, for underlying types
   *
   * @param sft user-facing feature type
   * @return
   */
  private def addVisToSchema(sft: SimpleFeatureType): SimpleFeatureType = {
    require(sft.indexOf(VisCol) == -1, s"'$VisCol' is a reserved attribute name")
    // add the _vis column into the underlying table
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    builder.nillable(true)
    builder.add(PartitionedPostgisDialect.VisCol, classOf[String])
    val result = builder.buildFeatureType()
    result.getUserData.putAll(sft.getUserData)
    result
  }

  /**
   * Add the _vis col to any transforms so that it is always retrieved
   *
   * @param query query
   * @return same query, with updated property transform
   */
  private def addVisToTransform(query: Query): Query = {
    if (query.retrieveAllProperties()) {
      query
    } else {
      val updated = new Query(query)
      updated.setPropertyNames(query.getPropertyNames :+ PartitionedPostgisDialect.VisCol: _*)
      updated
    }
  }

  /**
   * Project a user-facing feature onto the underlying schema, mapping the user-data visibility into `_vis`.
   * Other attributes are copied by name.
   *
   * @param from user feature
   * @param underlyingType physical schema (with `_vis` column)
   * @return physical feature with `_vis` populated
   */
  private def toUnderlying(from: SimpleFeature, underlyingType: SimpleFeatureType): SimpleFeature = {
    val to = new ScalaSimpleFeature(underlyingType, from.getID)
    var i = 0
    while (i < from.getAttributeCount) {
      to.setAttributeNoConvert(i, from.getAttribute(i))
      i += 1
    }
    to.setAttributeNoConvert(i, SecurityUtils.getVisibility(from))
    to.getUserData.putAll(from.getUserData)
    to
  }

  /**
   * Project a physical feature onto the user schema, dropping the `_vis` column but surfacing its
   * value into the feature's user data as the visibility expression (if non-null/non-empty).
   *
   * @param from physical feature (with `_vis`)
   * @param userFacingType user schema (without `_vis`)
   * @return user feature with visibility populated in user data
   */
  private def toUserFacing(from: SimpleFeature, userFacingType: SimpleFeatureType): SimpleFeature = {
    val to = new ScalaSimpleFeature(userFacingType, from.getID)
    var i = 0
    while (i < userFacingType.getAttributeCount) {
      to.setAttributeNoConvert(i, from.getAttribute(i))
      i += 1
    }
    SecurityUtils.setFeatureVisibility(to, from.getAttribute(i).asInstanceOf[String])
    to.getUserData.putAll(from.getUserData)
    to
  }

  /**
   * Types for cached schemas
   */
  private sealed trait SchemaType

  private case class SchemaWithVis(userFacing: SimpleFeatureType, underling: SimpleFeatureType) extends SchemaType
  private case class SchemaWithoutVis(sft: SimpleFeatureType) extends SchemaType

  /**
   * Feature reader that re-types features to the user schema, dropping `_vis` (but surfacing its
   * value into the feature's user data as the visibility)
   */
  private class VisFeatureReader(delegate: FeatureReader[SimpleFeatureType, SimpleFeature])
      extends FeatureReader[SimpleFeatureType, SimpleFeature] {
    private lazy val userFacingType = removeVisFromSchema(delegate.getFeatureType)
    override def getFeatureType: SimpleFeatureType = userFacingType
    override def hasNext: Boolean = delegate.hasNext
    override def next(): SimpleFeature = toUserFacing(delegate.next(), userFacingType)
    override def close(): Unit = delegate.close()
  }

  /**
   * Wraps a feature collection so its features are re-typed to the user schema, dropping `_vis`
   */
  private class VisFeatureCollection(delegate: SimpleFeatureCollection) extends DecoratingSimpleFeatureCollection(delegate) {

    private lazy val userFacingType = removeVisFromSchema(delegate.getSchema)

    override def getSchema: SimpleFeatureType = userFacingType

    override def features(): SimpleFeatureIterator = new SimpleFeatureIterator {
      private val iter = delegate.features()
      override def hasNext: Boolean = iter.hasNext
      override def next(): SimpleFeature = toUserFacing(iter.next(), userFacingType)
      override def close(): Unit = iter.close()
    }
  }

  /**
   * Feature writer that exposes the user schema but maps visibility into `_vis` on write. The
   * delegate feature returned by `next()` is retained so that `write()` can copy the caller's
   * attributes (including visibility) back into it before persisting.
   */
  private class VisFeatureWriter(delegate: FeatureWriter[SimpleFeatureType, SimpleFeature], userFacingType: SimpleFeatureType)
      extends FeatureWriter[SimpleFeatureType, SimpleFeature] {

    private var delegateFeature: SimpleFeature = _
    private var userFeature: SimpleFeature = _

    override def getFeatureType: SimpleFeatureType = userFacingType

    override def hasNext: Boolean = delegate.hasNext

    override def next(): SimpleFeature = {
      delegateFeature = delegate.next()
      // present a user-schema view (without '_vis') for the caller to populate
      userFeature = toUserFacing(delegateFeature, userFacingType)
      userFeature
    }

    override def write(): Unit = {
      var i = 0
      while (i < userFacingType.getAttributeCount) {
        delegateFeature.setAttribute(i, userFeature.getAttribute(i))
        i += 1
      }
      delegateFeature.setAttribute(i, SecurityUtils.getVisibility(userFeature))
      delegateFeature.getUserData.putAll(userFeature.getUserData)
      // propagate a caller-provided fid - the jdbc writer reads the id off the delegate feature itself
      var providedFid = userFeature.getUserData.get(Hints.PROVIDED_FID)
      if (providedFid == null && java.lang.Boolean.TRUE == userFeature.getUserData.get(Hints.USE_PROVIDED_FID)) {
        providedFid = userFeature.getID
      }
      if (providedFid != null) {
        delegateFeature.getIdentifier match {
          case id: FeatureIdImpl => id.setID(providedFid.toString)
          case _ => // no-op
        }
      }
      delegate.write()
    }

    override def remove(): Unit = delegate.remove()

    override def close(): Unit = delegate.close()
  }

  /**
   * Read-only feature source wrapper
   */
  private class VisSimpleFeatureSource(source: SimpleFeatureSource, userFacingType: SimpleFeatureType)
      extends VisFeatureSourceMethods(source, userFacingType) {
    override def getDataStore: DataAccess[SimpleFeatureType, SimpleFeature] = source.getDataStore
  }

  /**
   * Feature store wrapper - maps visibility into `_vis` on write, since the store's own writer path
   * bypasses the data store's `getFeatureWriter*` methods.
   */
  private class VisSimpleFeatureStore(source: SimpleFeatureStore, userFacingType: SimpleFeatureType)
      extends VisFeatureSourceMethods(source, userFacingType) with SimpleFeatureStore {

    private val underlying = source.getSchema

    override def getDataStore: DataAccess[SimpleFeatureType, SimpleFeature] = source.getDataStore

    override def addFeatures(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]): java.util.List[FeatureId] = {
      val mapped = new DecoratingSimpleFeatureCollection(DataUtilities.simple(collection)) {
        override def getSchema: SimpleFeatureType = underlying
        override def features(): SimpleFeatureIterator = new SimpleFeatureIterator {
          private val iter = collection.features()
          override def hasNext: Boolean = iter.hasNext
          override def next(): SimpleFeature = toUnderlying(iter.next(), underlying)
          override def close(): Unit = iter.close()
        }
      }
      source.addFeatures(mapped)
    }

    override def setFeatures(reader: FeatureReader[SimpleFeatureType, SimpleFeature]): Unit = {
      val mapped = new FeatureReader[SimpleFeatureType, SimpleFeature] {
        override def getFeatureType: SimpleFeatureType = underlying
        override def hasNext: Boolean = reader.hasNext
        override def next(): SimpleFeature = toUnderlying(reader.next(), underlying)
        override def close(): Unit = reader.close()
      }
      source.setFeatures(mapped)
    }

    override def removeFeatures(filter: Filter): Unit = source.removeFeatures(filter)
    override def modifyFeatures(attributeName: Name, attributeValue: AnyRef, filter: Filter): Unit =
      source.modifyFeatures(attributeName, attributeValue, filter)
    override def modifyFeatures(attributeNames: Array[Name], attributeValues: Array[AnyRef], filter: Filter): Unit =
      source.modifyFeatures(attributeNames, attributeValues, filter)
    override def modifyFeatures(name: String, attributeValue: AnyRef, filter: Filter): Unit =
      source.modifyFeatures(name, attributeValue, filter)
    override def modifyFeatures(names: Array[String], attributeValues: Array[AnyRef], filter: Filter): Unit =
      source.modifyFeatures(names, attributeValues, filter)
    override def setTransaction(tx: Transaction): Unit = source.setTransaction(tx)
    override def getTransaction: Transaction = source.getTransaction
  }

  /**
   * Read-side overrides shared by the source and store wrappers. Hides `_vis` from the exposed schema
   * and from returned features; everything else delegates.
   */
  private abstract class VisFeatureSourceMethods(source: SimpleFeatureSource, userFacingType: SimpleFeatureType)
      extends SimpleFeatureSource {
    override def getSchema: SimpleFeatureType = userFacingType
    override def getName: Name = userFacingType.getName
    override def getFeatures: SimpleFeatureCollection = new VisFeatureCollection(source.getFeatures)
    override def getFeatures(filter: Filter): SimpleFeatureCollection = new VisFeatureCollection(source.getFeatures(filter))
    override def getFeatures(query: Query): SimpleFeatureCollection = new VisFeatureCollection(source.getFeatures(addVisToTransform(query)))
    override def getInfo: ResourceInfo = source.getInfo
    override def getQueryCapabilities: QueryCapabilities = source.getQueryCapabilities
    override def getSupportedHints: java.util.Set[RenderingHints.Key] = source.getSupportedHints
    override def getBounds: ReferencedEnvelope = source.getBounds
    override def getBounds(query: Query): ReferencedEnvelope = source.getBounds(query)
    override def getCount(query: Query): Int = source.getCount(query)
    override def addFeatureListener(listener: FeatureListener): Unit = source.addFeatureListener(listener)
    override def removeFeatureListener(listener: FeatureListener): Unit = source.removeFeatureListener(listener)
  }
}
