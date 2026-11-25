/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature._
import org.geotools.api.feature.`type`._
import org.geotools.api.referencing.crs.CoordinateReferenceSystem
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.identity.FeatureId
import org.geotools.api.filter.expression.PropertyName
import org.geotools.api.filter.Filter
import org.geotools.data.simple._
import org.geotools.data.store.{ContentDataStore, ContentEntry, DecoratingDataStore}
import org.geotools.data.{DataUtilities, DefaultTransaction}
import org.geotools.feature.{DecoratingFeature, FeatureCollection, FeatureTypes, NameImpl}
import org.geotools.feature.collection.DecoratingSimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.jdbc.{JDBCDataStore, JDBCDataStoreFactory, JDBCFeatureSource, SQLDialect}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.{SftUserData, VisCol}
import org.locationtech.geomesa.gt.partition.postgis.dialect.auths.SessionDataSource
import org.locationtech.geomesa.gt.partition.postgis.dialect.functions.PgVis
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.UserDataTable
import org.locationtech.geomesa.gt.partition.postgis.dialect.{Columns, PartitionedPostgisDialect, PartitionedPostgisPsDialect}
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.security.{AuthorizationsProvider, HasAuthorizationsProvider, SecurityUtils}
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.json.JsonPathPropertyNameResolver

import java.awt.RenderingHints
import java.io.IOException
import java.sql.{Connection, SQLException}
import java.util.concurrent.CompletionException
import java.util.concurrent.locks.ReentrantLock
import javax.sql.DataSource
import scala.util.control.NonFatal

/**
 * Data store wrapper for partitioned postgis feature types. This wrapper handles:
 *
 * <b>Visibilities</b>
 *
 * The physical schema carries a hidden per-row visibility column (`_vis`) as an ordinary attribute so that it flows
 * through the sealed `JDBCDataStore` insert/read machinery for free. This wrapper hides that column from callers:
 *
 *  - on `createSchema`, if the `pg.vis.enabled` user-data flag is explicitly `true`, a nullable
 *    `_vis` attribute is appended to the type before it's created
 *  - on read, features are re-typed to drop `_vis`; the MainView already filters rows by the caller's
 *    authorizations, and the visibility expression is surfaced into the feature user data
 *  - on write, the feature's visibility (from user data) is copied into `_vis`
 *
 * <b>Feature type names</b>
 *
 * The wrapped `JDBCDataStore` uses the name of the main view as the feature type name. Normally they are the same,
 * but they will be different if a feature type has been renamed, or if attributes have been added or removed,
 * which creates a new generation of the feature type backed by a new view. This wrapper maps feature type names
 * to the current view, and hides the views for any previous generations.
 *
 * <b>Feature ids</b>
 *
 * The wrapped `JDBCDataStore` prefixes feature ids with the name of the main view. This wrapper removes the prefix,
 * so that ids are stable across generations. Note that an id which itself starts with the name of the view is not
 * supported, and may not be matched correctly.
 *
 * <b>Default geometries</b>
 *
 * The wrapped `JDBCDataStore` always uses the first geometry attribute as the default geometry. The default geometry
 * that a feature type was created with is stored in the `pg.geom.col` user data, and this wrapper restores it in
 * the feature types and features that it returns, and in any filters that reference the default geometry.
 *
 * For any type that has no physical `_vis` column (pre-existing schemas, or vis explicitly disabled), whose
 * name matches its view, and whose default geometry is the first geometry attribute, features are passed through
 * without being re-typed
 *
 * <b>Multiple data store instances</b>
 *
 * The mapping of feature type names to views is cached by each data store instance, for the duration configured
 * by the `geomesa.metadata.expiry` system property. After `updateSchema` creates a new generation of a feature
 * type, other data store instances (including ones in other processes) will keep reading from and writing to the
 * view for the previous generation until their cache expires. The previous view does not expose the new generation,
 * so those instances will not see anything written through the new view, although anything that they write will be
 * visible through both views. Any feature source, reader or writer is bound to the view that was current when it
 * was created, for as long as it is used, and will start to fail once that generation has been dropped.
 *
 * The mapping used to list feature type names is cached in the same way, but is re-loaded whenever the views in the
 * database change - so a feature type that is renamed through a different data store instance will be listed
 * under its old name until the cache expires.
 *
 * @param delegate wrapped jdbc data store
 * @param maxConnections max size of the connection pool, if known and limited
 */
class PartitionedPostgisDataStore(delegate: JDBCDataStore, maxConnections: Option[Int] = None)
    extends DecoratingDataStore(delegate) with HasAuthorizationsProvider {

  import PartitionedPostgisDataStore._
  import PartitionedPostgisDialect.VisCol
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  // cache of schemas, keyed by (user-facing) feature type name
  // note: this means that we may keep using the view for a previous generation of a feature type for a period
  // of time, if the feature type is updated through a different data store instance - see the class docs
  private val schemas =
    Caffeine.newBuilder().expireAfterWrite(TableBasedMetadata.Expiry.toJavaDuration.get)
      .build(new CacheLoader[String, SchemaType]() {
        override def load(typeName: String): SchemaType = {
          val view = generations().view(typeName).getOrElse {
            throw new IOException(s"Schema '$typeName' does not exist")
          }
          loadSchemaType(typeName, view)
        }
      })

  // cached mapping of feature type names to views, used for listing feature types
  @volatile
  private var listing: CachedGenerations = _
  private val listingExpiry = TableBasedMetadata.Expiry.toJavaDuration.get.toMillis

  // only allow one schema operation at a time, as each one requires two connections from the connection pool -
  // otherwise concurrent operations could each take one connection, and then starve each other of the second one
  private val updateLock = new ReentrantLock()

  override val authProvider: AuthorizationsProvider = delegate.getDataSource.asInstanceOf[SessionDataSource].provider

  // remove the view name from the id of any features that are read
  delegate.setFeatureFactory(new FidFeatureFactory(delegate.getFeatureFactory))

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
   * of the feature type, to add indices, or to rename it - use `updateSchema` instead. Any attributes in the
   * feature type are ignored, including any index flags - indices are never added to the existing tables, as that
   * would block writes while the index is built.
   *
   * Only the current generation of the feature type is upgraded. Any previous generations (created by adding or
   * removing attributes through `updateSchema`) keep the procedures and triggers that they were created with,
   * until they are dropped - only changes to the age-off configuration are applied to them.
   *
   * An upgrade requires two connections from the connection pool at the same time - one to hold a lock on the
   * feature type name, and one to make the changes.
   *
   * @param sft updated feature type
   */
  def upgrade(sft: SimpleFeatureType): Unit = {
    val typeName = sft.getTypeName
    // prevent the feature type from being updated or removed while we're upgrading it, including from other
    // data store instances - otherwise we could end up upgrading a generation that has been superseded
    withUpdateLock(typeName) { cx =>
      val viewExists = this.viewExists(cx) _
      // now that we have the lock, re-load the schema in case it has been updated since we cached it
      // note: we always start from the stored type, as the dialect depends on the internal user data that is loaded
      // with it (identifier alias, previous generation, etc), which may not be present in the type that's passed in
      val existing = reloadSchema(typeName, generations(cx), viewExists)
      val updated = SimpleFeatureTypes.copy(sft)
      InternalKeys.foreach(updated.getUserData.remove)
      validateUserData(existing, updated)
      warnIgnoredIndices(existing, updated)
      try { updateConfig(typeName, existing, updated, viewExists) } finally {
        invalidate(typeName)
      }
    }
  }

  override def getTypeNames: Array[String] = {
    val views = delegate.getTypeNames
    val generations = this.generations(views)
    views.flatMap(generations.typeName)
  }

  override def getNames: java.util.List[Name] = {
    val views = delegate.getNames.asScala
    val generations = this.generations(views.map(_.getLocalPart).toSeq)
    val names = views.flatMap { name =>
      generations.typeName(name.getLocalPart).map(n => new NameImpl(name.getNamespaceURI, n): Name)
    }
    new java.util.ArrayList[Name](names.asJava)
  }

  /**
   * Creates a new schema.
   *
   * Creating a schema requires two connections from the connection pool at the same time - one to hold a lock on the
   * feature type name, and one to create things.
   *
   * @param featureType feature type
   */
  override def createSchema(featureType: SimpleFeatureType): Unit = {
    val typeName = featureType.getTypeName
    val sft = SimpleFeatureTypes.copy(featureType)
    InternalKeys.foreach(sft.getUserData.remove)
    // note: the dialect only locks on the name of the view, which may not be the same as the type name, so we lock on
    // the type name in order to prevent the name from being used concurrently - either by the same feature type being
    // created under a different view, or by an existing feature type being renamed
    withUpdateLock(typeName) { cx =>
      try {
        val viewExists = this.viewExists(cx) _
        val generations = this.generations(cx)
        if (generations.view(typeName).exists(viewExists)) {
          throw new IllegalArgumentException(s"Schema '$typeName' already exists")
        }
        val named = if (!viewExists(typeName)) { sft } else {
          // the name is being used by a view for a different feature type, so we have to use a different name for our view
          val view = WithClose(getDataSource.getConnection)(partitionedDialect.nextViewName(getDatabaseSchema, typeName, _))
          val renamed = SimpleFeatureTypes.renameSft(sft, view)
          renamed.getUserData.put(SftUserData.TypeName.key, typeName)
          renamed
        }
        val view = named.getTypeName
        // check if there's already a (non-view) table with our name, as the jdbc store will re-use it, in which case
        // we don't want to drop it if we fail
        val tableExists = partitionedDialect.relationExists(getDatabaseSchema, view, cx)
        // the view may have been dropped outside of the jdbc store (for example, a previous generation that was
        // retired), in which case the jdbc store will still have it cached, and won't let us re-use the name
        if (!tableExists) {
          evict(delegate, view)
        }
        try {
          delegate.createSchema(if (SftUserData.VisEnabled.get(named)) { addVisToSchema(named) } else { named })
        } catch {
          case NonFatal(e) =>
            // clean up the initial table created by the jdbc store, so that it doesn't get re-used by a later attempt
            // note: the rest of the feature type is created in a single transaction, so nothing else should be left behind
            try {
              if (!tableExists && !viewExists(view)) {
                WithClose(getDataSource.getConnection)(partitionedDialect.dropInitialTable(getDatabaseSchema, view, _))
              }
            } catch {
              case NonFatal(suppressed) => e.addSuppressed(suppressed)
            }
            throw e
        }
      } finally {
        invalidate(typeName)
      }
    }
  }

  /**
   * Loads the previous generation of a feature type, if there is one, so that it's cached by the delegate store.
   *
   * The dialect needs the previous generation in order to re-create things - if it isn't cached, then it has to be
   * loaded with an additional connection, while we're already holding the connection being used to re-create things.
   *
   * @param underlying underlying feature type
   * @param viewExists checks if a view currently exists
   */
  private def loadPreviousGeneration(underlying: SimpleFeatureType, viewExists: String => Boolean): Unit =
    SftUserData.PreviousView.get(underlying).filter(viewExists).foreach(delegate.getSchema)

  /**
   * Checks if a view exists, which may be the view for a previous generation of a feature type.
   *
   * Note that this is a single query, as opposed to listing the type names from the delegate store, which
   * has to check every table and view in the schema.
   *
   * @param cx connection
   * @param view name of the view
   * @return
   */
  private def viewExists(cx: Connection)(view: String): Boolean =
    partitionedDialect.viewExists(getDatabaseSchema, view, cx)

  /**
   * Re-loads a schema from the database, clearing anything that we (or the delegate store) have cached for it
   *
   * @param typeName name of the feature type
   * @param generations current mapping of feature type names to views
   * @param viewExists checks if a view currently exists
   * @return the underlying feature type
   */
  private def reloadSchema(typeName: String, generations: Generations, viewExists: String => Boolean): SimpleFeatureType = {
    invalidate(typeName)
    // note: the delegate store may still have the feature type cached, if it was removed through a different
    // data store instance, so we have to verify that the view still exists
    val view = generations.view(typeName).filter(viewExists).getOrElse {
      throw new IOException(s"Schema '$typeName' does not exist")
    }
    flush(view)
    // note: we already have the current view, so we bypass the cache loader, which would use another connection
    // to look it up again, while we're already holding the connection for the update lock
    schemas.get(typeName, (_: String) => loadSchemaType(typeName, view)) match {
      case null => throw new IOException(s"Schema '$typeName' does not exist")
      case s: Passthrough => s.sft
      case s: Mapped => s.mapping.underlying
    }
  }

  override def getSchema(typeName: String): SimpleFeatureType =
    Option(loadSchema(typeName)).fold[SimpleFeatureType](null) {
      case s: Passthrough => s.sft
      case s: Mapped => s.mapping.userFacing
    }

  override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)

  override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit =
    updateSchema(typeName.getLocalPart, featureType)

  /**
   * Updates an existing schema. The following changes are supported:
   *
   *  - adding new attributes at the end of the feature type - new attributes must be nillable, and will be null for
   *    any existing features
   *  - removing existing attributes, other than the default date and default geometry
   *  - adding indices to existing attributes - only features written after the update will be indexed
   *  - renaming the feature type
   *  - updating user data configuration, for any configuration that isn't fixed when the schema is created - any
   *    configuration that is not specified in the updated feature type will be left unchanged
   *
   * Re-ordering or renaming attributes, changing attribute types, or changing the default date or geometry are
   * not supported.
   *
   * Adding or removing attributes, or adding an index, will create a new generation of the feature type, with a new set of tables and a
   * new main view which also exposes the data from the existing tables. This does not require any locks on the
   * existing view or tables, so it will not block (or be blocked by) any queries, writes or partition maintenance.
   * The previous generation will be dropped automatically once all of its data has aged off. The new generation
   * is created with the indices specified in the updated feature type - the tables for the previous generation
   * keep the indices that they were created with.
   *
   * Renaming a feature type only changes the name that is exposed by this data store - the underlying view,
   * tables, etc will keep their existing names.
   *
   * Updating user data configuration will re-create the PLPG/SQL procedures associated with the feature type, same
   * as with `upgrade`.
   *
   * Other data store instances that have already loaded the feature type will keep using the previous generation
   * until their cached schema expires, and any feature sources, readers or writers that have already been created
   * will keep using it for as long as they are used - see the class docs for details.
   *
   * An update requires two connections from the connection pool at the same time - one to hold a lock on the
   * feature type name (and the new name, if the feature type is being renamed), and one to make the changes.
   *
   * @param typeName name of the feature type to update
   * @param featureType updated feature type
   */
  override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit = {
    require(featureType.indexOf(VisCol) == -1, s"'$VisCol' is a reserved attribute name")
    // prevent concurrent updates to the same feature type, including from other data store instances - otherwise
    // each update could create its own generation, based off the same previous generation
    // note: if the feature type is being renamed, we also lock on the new name, so that a feature type with that
    // name can't be created at the same time
    withUpdateLock(typeName, featureType.getTypeName) { cx =>
      updateSchemaWithLock(typeName, featureType, this.generations(cx), viewExists(cx))
    }
  }

  /**
   * Updates a schema - the caller must be holding the update lock for the feature type name, and for the new name
   * if the feature type is being renamed
   *
   * @param typeName name of the feature type to update
   * @param featureType updated feature type
   * @param generations current mapping of feature type names to views
   * @param viewExists checks if a view currently exists
   */
  private def updateSchemaWithLock(
      typeName: String,
      featureType: SimpleFeatureType,
      generations: Generations,
      viewExists: String => Boolean): Unit = {
    // now that we have the lock, re-load the schema in case it has been updated since we cached it
    val existing = reloadSchema(typeName, generations, viewExists)
    val updated = SimpleFeatureTypes.copy(featureType)
    InternalKeys.foreach(updated.getUserData.remove)
    validateUpdate(existing, updated)

    val name = updated.getTypeName
    if (name != typeName && generations.view(name).exists(viewExists)) {
      throw new IllegalArgumentException(s"Schema '$name' already exists")
    }

    val attributes = existing.getAttributeDescriptors.asScala.map(_.getLocalName).filter(_ != VisCol)
    // an index can't be added to the existing tables without blocking writes, so it's added through a new generation
    // note: the index flags are missing from the existing type if they couldn't be loaded, so we verify them against
    // the database (which will fail if they still can't be loaded), to avoid creating a generation that isn't needed
    lazy val addsIndex = {
      val added = addedIndices(existing, updated)
      added.nonEmpty && {
        val indexed =
          WithClose(getDataSource.getConnection)(partitionedDialect.indexedColumns(getDatabaseSchema, existing, _))
        added.exists(!indexed.contains(_))
      }
    }
    if (attributes != updated.getAttributeDescriptors.asScala.map(_.getLocalName) || addsIndex) {
      // create a new generation with the updated attributes and indices, which will include the existing data
      // through its view
      val view = WithClose(getDataSource.getConnection)(partitionedDialect.nextViewName(getDatabaseSchema, name, _))
      val generation = SimpleFeatureTypes.renameSft(updated, view)
      generation.getAttributeDescriptors.asScala.foreach { d =>
        if (d.isJson() || d.isList) {
          // these types get special handling when creating a table, which only works based off the attribute binding
          d.getUserData.remove(JDBCDataStore.JDBC_NATIVE_TYPENAME)
          d.getUserData.remove(JDBCDataStore.JDBC_NATIVE_TYPE)
        }
      }
      // retain any existing user data that isn't being modified
      existing.getUserData.asScala.foreach { case (k, v) =>
        if (!InternalKeys.contains(k)) { generation.getUserData.putIfAbsent(k, v) }
      }
      generation.getUserData.put(SftUserData.TypeName.key, name)
      generation.getUserData.put(SftUserData.PreviousView.key, existing.getTypeName)
      // the view name is unused, but the jdbc store may still have it cached, if it was dropped outside of the store
      evict(delegate, view)
      try {
        delegate.createSchema(if (existing.indexOf(VisCol) == -1) { generation } else { addVisToSchema(generation) })
      } catch {
        case NonFatal(e) =>
          // clean up anything that was partially created, so that it doesn't interfere with the existing generation
          // note: the generation is created in a single transaction, so normally the only thing left behind will
          // be the initial table created by the jdbc store - we hold the lock and the view name was unused, so
          // anything with that name is ours
          try {
            if (viewExists(view)) {
              dropGenerations(view, Seq.empty)
            } else {
              WithClose(getDataSource.getConnection)(partitionedDialect.dropInitialTable(getDatabaseSchema, view, _))
            }
          } catch {
            case NonFatal(suppressed) => e.addSuppressed(suppressed)
          }
          throw e
      }
    } else {
      updateConfig(typeName, existing, updated, viewExists)
    }
    invalidate(typeName, name)
  }

  /**
   * Updates the user data configuration and/or the name of a schema, without changing its attributes. Unless the
   * schema is only being renamed, this re-creates the PLPG/SQL procedures associated with it.
   *
   * The caller is responsible for validating the update, and for invalidating our cached schema afterwards.
   *
   * @param typeName name of the feature type to update
   * @param existing existing (underlying) feature type, as currently stored in the database
   * @param updated updated (user-facing) feature type, without any internal user data - only the name and
   *                user data are used
   * @param viewExists checks if a view currently exists
   */
  private def updateConfig(
      typeName: String,
      existing: SimpleFeatureType,
      updated: SimpleFeatureType,
      viewExists: String => Boolean): Unit = {
    val name = updated.getTypeName
    val changed = updated.getUserData.asScala.exists { case (k, v) =>
      String.valueOf(v) != String.valueOf(existing.getUserData.get(k))
    }
    loadPreviousGeneration(existing, viewExists)
    WithClose(new DefaultTransaction()) { tx =>
      WithClose(getConnection(tx)) { cx =>
        if (changed || name == typeName) {
          val underlying = SimpleFeatureTypes.copy(existing)
          underlying.getUserData.putAll(updated.getUserData)
          if (name != typeName) {
            // note: the name is persisted along with the rest of the user data, in the same transaction - otherwise
            // a failure could leave the configuration updated, but the feature type not renamed
            underlying.getUserData.put(SftUserData.TypeName.key, name)
          }
          partitionedDialect.postCreateTable(getDatabaseSchema, underlying, cx)
        } else {
          // only the name is changing
          partitionedDialect.setTypeName(getDatabaseSchema, existing.getTypeName, name, cx)
        }
        tx.commit()
      }
    }
    // clear the cached feature type, so that it gets re-loaded from the database
    flush(existing.getTypeName)
  }

  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  /**
   * Removes a schema, along with any previous generations of it.
   *
   * Removing a schema requires two connections from the connection pool at the same time - one to hold a lock on
   * the feature type name, and one to drop things.
   *
   * @param typeName name of the feature type to remove
   */
  override def removeSchema(typeName: String): Unit = {
    // prevent the feature type from being updated while we're removing it, including from other data store instances -
    // otherwise an update could create a new generation based off a generation that we're dropping
    withUpdateLock(typeName) { cx =>
      removeSchemaWithLock(typeName, this.generations(cx), viewExists(cx))
    }
  }

  /**
   * Removes a schema - the caller must be holding the update lock for the feature type name
   *
   * @param typeName name of the feature type to remove
   * @param generations current mapping of feature type names to views
   * @param viewExists checks if a view currently exists
   */
  private def removeSchemaWithLock(typeName: String, generations: Generations, viewExists: String => Boolean): Unit = {
    // note: there won't be a view if the name is being used by the view for a different feature type, or for a
    // previous generation of a feature type - those must not be removed through their view names
    // note: the delegate store may still have the feature type cached, if it was removed through a different
    // data store instance, so we have to verify that the view still exists
    val view = generations.view(typeName).filter(viewExists).getOrElse {
      throw new IllegalArgumentException(s"Schema '$typeName' does not exist")
    }
    // also remove any previous generations, which are no longer accessible - they're dropped in the same
    // transaction as the current generation, so if anything fails then the feature type is left intact
    val previous = generations.previous(view)
    try {
      // load the previous generations, so that they're cached by the delegate store - otherwise the dialect has to
      // load them with an additional connection, while we're already holding the connection being used to drop things
      previous.filter(viewExists).foreach(delegate.getSchema)
      dropGenerations(view, previous)
    } finally {
      invalidate(typeName)
    }
  }

  /**
   * Drops a generation of a feature type, along with any previous generations, in a single transaction.
   *
   * Note that we don't go through the delegate store - it hard-codes a "DROP TABLE" on the main view, and doesn't
   * drop things atomically.
   *
   * @param view name of the main view for the generation to drop
   * @param previous names of the main views for any previous generations to drop, most recent first
   */
  private def dropGenerations(view: String, previous: Seq[String]): Unit = {
    val underlying = delegate.getSchema(view)
    WithClose(getDataSource.getConnection)(partitionedDialect.dropSchema(getDatabaseSchema, underlying, previous, _))
    // the views weren't removed through the delegate store, so it still has them cached
    (Seq(view) ++ previous).foreach(evict(delegate, _))
  }

  override def getFeatureSource(typeName: Name): SimpleFeatureStore = getFeatureSource(typeName.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureStore = {
    loadSchema(typeName) match {
      case _: Passthrough => jdbcFeatureStore(typeName, Transaction.AUTO_COMMIT)
      case s: Mapped => new MappedFeatureStore(jdbcFeatureStore(s.mapping.view, Transaction.AUTO_COMMIT), s.mapping)
    }
  }

  override def getFeatureReader(query: Query, tx: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    loadSchema(query.getTypeName) match {
      case _: Passthrough =>
        val source = jdbcFeatureStore(query.getTypeName, tx)
        source.setTransaction(tx)
        source.getReader(query)

      case s: Mapped =>
        val source = jdbcFeatureStore(s.mapping.view, tx)
        source.setTransaction(tx)
        new MappedFeatureReader(source.getReader(s.mapping.toUnderlying(query)), s.mapping)
    }
  }

  private def jdbcFeatureStore(view: String, tx: Transaction): JsonPathJdbcFeatureStore =
    new JsonPathJdbcFeatureStore(delegate, delegate.getFeatureSource(view, tx).getEntry, null)

  override def getFeatureWriter(typeName: String, filter: Filter, tx: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    loadSchema(typeName) match {
      case _: Passthrough => new FidFeatureWriter(delegate.getFeatureWriter(typeName, filter, tx), typeName)
      case s: Mapped =>
        new MappedFeatureWriter(delegate.getFeatureWriter(s.mapping.view, s.mapping.toUnderlying(filter), tx), s.mapping)
    }
  }

  override def getFeatureWriter(typeName: String, tx: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    loadSchema(typeName) match {
      case _: Passthrough => new FidFeatureWriter(delegate.getFeatureWriter(typeName, tx), typeName)
      case s: Mapped => new MappedFeatureWriter(delegate.getFeatureWriter(s.mapping.view, tx), s.mapping)
    }
  }

  override def getFeatureWriterAppend(typeName: String, tx: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    loadSchema(typeName) match {
      case _: Passthrough => new FidFeatureWriter(delegate.getFeatureWriterAppend(typeName, tx), typeName)
      case s: Mapped => new MappedFeatureWriter(delegate.getFeatureWriterAppend(s.mapping.view, tx), s.mapping)
    }
  }

  /**
   * Clears the feature type cached by the delegate store, so that it gets re-loaded from the database
   *
   * @param view name of the view
   */
  private def flush(view: String): Unit =
    delegate.getFeatureSource(view, Transaction.AUTO_COMMIT).getEntry.getState(Transaction.AUTO_COMMIT).flush()

  private def partitionedDialect: PartitionedPostgisDialect = this.dialect match {
    case d: PartitionedPostgisDialect => d
    case d: PartitionedPostgisPsDialect => d.delegate
  }

  /**
   * Runs an operation while holding the update locks for one or more feature type names, which prevents the names
   * from being created, updated or removed concurrently, including by other data store instances.
   *
   * The locks are held by a dedicated connection, which is passed to the operation so that it can be used to read
   * the current state without requiring another connection - it must not be used for anything else.
   *
   * Operations are run one at a time for this data store instance, so that concurrent operations can't exhaust
   * the connection pool while each waiting for the additional connection that they need to make their changes.
   *
   * @param typeNames feature type names
   * @param op operation to run
   */
  private def withUpdateLock(typeNames: String*)(op: Connection => Unit): Unit = {
    // fail fast instead of hanging while waiting for a second connection
    maxConnections.filter(_ < 2).foreach { max =>
      throw new IllegalStateException(
        s"Creating, updating, upgrading and removing schemas each require two connections at the same time, but " +
            s"the connection pool is limited to $max - set '${JDBCDataStoreFactory.MAXCONN.key}' to at least 2")
    }
    updateLock.lock()
    try {
      WithClose(getDataSource.getConnection) { cx =>
        partitionedDialect.withUpdateLock(getDatabaseSchema, typeNames, cx)(op(cx))
      }
    } finally {
      updateLock.unlock()
    }
  }

  /**
   * Clears any cached state for feature types that have been modified
   *
   * @param typeNames feature type names
   */
  private def invalidate(typeNames: String*): Unit = {
    typeNames.foreach(schemas.invalidate)
    listing = null
  }

  /**
   * Gets the mapping of feature type names to views, for listing feature types.
   *
   * The mapping is cached, to avoid having to query for it every time the feature types are listed. Most changes
   * made through other data store instances will add or remove a view, so we re-load the mapping whenever the views
   * are different from when it was loaded. This is only used for listing feature types - anything that loads or
   * modifies a feature type always uses the current mapping.
   *
   * @param views names of all the views that currently exist, including previous generations
   * @return
   */
  private def generations(views: Seq[String]): Generations = {
    val current = views.toSet
    val cached = listing
    if (cached != null && cached.views == current && cached.expires > System.currentTimeMillis()) {
      cached.generations
    } else {
      val generations = this.generations()
      listing = CachedGenerations(generations, current, System.currentTimeMillis() + listingExpiry)
      generations
    }
  }

  /**
   * Loads the mapping of feature type names to views
   *
   * @return
   */
  private def generations(): Generations = WithClose(getDataSource.getConnection)(generations)

  /**
   * Loads the mapping of feature type names to views, using an existing connection
   *
   * @param cx connection
   * @return
   */
  private def generations(cx: Connection): Generations = {
    val userData =
      try { UserDataTable.read(cx, getDatabaseSchema, Seq(SftUserData.TypeName, SftUserData.PreviousView)) } catch {
        case e: SQLException => throw new IOException("Error loading feature types:", e)
      }
    val names = userData.collect { case (view, SftUserData.TypeName.key, name) => view -> name }
    val previous = userData.collect { case (view, SftUserData.PreviousView.key, prev) => view -> prev }
    Generations(names.toMap, previous.toMap)
  }

  /**
   * Loads the schema for a feature type from the delegate store
   *
   * @param typeName feature type name
   * @param view name of the current view for the feature type
   * @return the schema, or null if it doesn't exist
   */
  private def loadSchemaType(typeName: String, view: String): SchemaType = {
    val underlying = delegate.getSchema(view)
    if (underlying == null) {
      null
    } else {
      // the default geometry, if it's not the one that the jdbc store uses (i.e. the first geometry)
      val geom = SftUserData.GeomField.get(underlying).filter { g =>
        Option(underlying.getGeometryDescriptor).forall(_.getLocalName != g)
      }
      if (view == typeName && underlying.indexOf(VisCol) == -1 && geom.isEmpty) {
        Passthrough(underlying)
      } else {
        // note: the vis column may not be the last attribute
        Mapped(new Mapping(toUserFacing(underlying, typeName, geom), underlying))
      }
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

object PartitionedPostgisDataStore extends LazyLogging {

  import PartitionedPostgisDialect.GeometryAttributeConversions
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  // user data that is managed internally
  private val InternalKeys = SftUserData.Values.filter(_.internal).map(_.key)

  // the jdbc store is final, and doesn't expose any way to remove a cached feature type other than dropping it
  private lazy val RemoveEntry = {
    val method = classOf[ContentDataStore].getDeclaredMethod("removeEntry", classOf[Name])
    method.setAccessible(true)
    method
  }

  /**
   * Removes a view that was dropped outside of the jdbc store from the jdbc store's cache - otherwise the store
   * won't allow a new feature type to be created with the same name.
   *
   * Note that this relies on reflection, so a failure is only logged - there's a unit test that verifies that
   * it works, in order to catch any changes to the jdbc store.
   *
   * @param store jdbc store
   * @param view name of the view
   */
  private def evict(store: JDBCDataStore, view: String): Unit = {
    try { RemoveEntry.invoke(store, new NameImpl(store.getNamespaceURI, view)) } catch {
      case NonFatal(e) => logger.warn(s"Error clearing cached feature type '$view':", e)
    }
  }

  /**
   * Converts an underlying schema into the user-facing type, by removing visibility, re-naming it, and
   * restoring the default geometry
   *
   * @param sft underlying feature type
   * @param typeName user-facing feature type name
   * @param geom name of the default geometry, if it's different from the underlying type
   * @return
   */
  private def toUserFacing(sft: SimpleFeatureType, typeName: String, geom: Option[String]): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    if (sft.indexOf(VisCol) != -1) {
      builder.remove(VisCol)
    }
    builder.setName(typeName)
    // note: the type may be a transform that doesn't include the default geometry
    geom.filter(sft.getDescriptor(_).isInstanceOf[GeometryDescriptor]).foreach(builder.setDefaultGeometry)
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
   * Verifies that the changes to a schema are supported by `updateSchema`
   *
   * @param existing existing (underlying) feature type
   * @param updated updated (user-facing) feature type
   */
  private def validateUpdate(existing: SimpleFeatureType, updated: SimpleFeatureType): Unit = {
    def unsupported(msg: String): Nothing = throw new UnsupportedOperationException(msg)

    val names = updated.getAttributeDescriptors.asScala.map(_.getLocalName).toList
    val (retained, added) = names.span(existing.getDescriptor(_) != null)
    if (added.exists(existing.getDescriptor(_) != null)) {
      unsupported(s"New attributes must be added after any existing attributes: ${added.mkString(", ")}")
    } else if (retained != existing.getAttributeDescriptors.asScala.map(_.getLocalName).filter(retained.contains)) {
      unsupported(s"Re-ordering attributes is not supported: ${retained.mkString(", ")}")
    }
    retained.foreach { name =>
      // note: bindings loaded from the database may be sub-types of the 'standard' bindings, e.g. java.sql.Timestamp
      val from = existing.getDescriptor(name)
      val to = updated.getDescriptor(name)
      def changed(what: String, fromValue: Any, toValue: Any): Nothing =
        unsupported(s"Changing attribute types is not supported: $name $what $fromValue -> $toValue")
      val binding = to.getType.getBinding
      if (!binding.isAssignableFrom(from.getType.getBinding) || (from.isInstanceOf[GeometryDescriptor] && binding != from.getType.getBinding)) {
        changed("type", from.getType.getBinding.getSimpleName, binding.getSimpleName)
      }
      // the binding doesn't capture everything about the column type, so check the rest explicitly
      (from, to) match {
        case (f: GeometryDescriptor, t: GeometryDescriptor) =>
          // note: an unknown srid may be -1 or 0
          val srids = Seq(f, t).map(_.getSrid.filter(_ > 0))
          if (srids.head != srids.last) {
            changed("srid", srids.head.getOrElse("none"), srids.last.getOrElse("none"))
          }
          val dims = Seq(f, t).map(_.getCoordinateDimensions.filter(_ > 0).getOrElse(2))
          if (dims.head != dims.last) {
            changed("coordinate dimensions", dims.head, dims.last)
          }

        case _ if from.isJson() != to.isJson() =>
          changed("json", from.isJson(), to.isJson())

        case _ if from.isList =>
          // note: if we couldn't determine the existing list type, then we can't check it
          val (f, t) = (from.getListType(), to.getListType())
          if (f != null && (t == null || !t.isAssignableFrom(f))) {
            changed("list type", f.getSimpleName, Option(t).map(_.getSimpleName).getOrElse("none"))
          }

        case _ if classOf[String].isAssignableFrom(binding) =>
          // note: an unlimited length may be -1, 0 or max int
          val lengths = Seq(from, to).map(d => Some(FeatureTypes.getFieldLength(d)).filter(n => n > 0 && n < Int.MaxValue))
          if (lengths.head != lengths.last) {
            changed("length", lengths.head.getOrElse("unlimited"), lengths.last.getOrElse("unlimited"))
          }

        case _ => // no-op
      }
    }
    added.map(updated.getDescriptor).foreach { d =>
      if (d.getMinOccurs > 0 && !d.isNillable) {
        unsupported(s"Attributes added to an existing schema must be nillable: ${d.getLocalName}")
      }
    }
    // note: when a feature type is loaded from the database, the default geometry will be the first geometry column,
    // so we use the default geometry that was recorded when the feature type was created, if available
    val geom = SftUserData.GeomField.get(existing).orElse(Option(existing.getGeometryDescriptor).map(_.getLocalName))
    geom.foreach { geom =>
      if (Option(updated.getGeometryDescriptor).forall(_.getLocalName != geom)) {
        unsupported(s"Removing or changing the default geometry attribute is not supported: $geom")
      }
    }
    SftUserData.DtgField.get(existing).foreach { dtg =>
      if (updated.getDescriptor(dtg) == null) {
        unsupported(s"Removing the default date attribute is not supported: $dtg")
      }
    }
    validateUserData(existing, updated)
  }

  /**
   * Gets any existing attributes that are flagged for an index in the updated feature type, but not in the existing
   * feature type
   *
   * @param existing existing (underlying) feature type
   * @param updated updated (user-facing) feature type - attributes that aren't in the existing type are ignored
   * @return
   */
  private def addedIndices(existing: SimpleFeatureType, updated: SimpleFeatureType): Seq[String] = {
    // note: the default date and geometries are always indexed, and aren't controlled by the index flag
    updated.getAttributeDescriptors.asScala.toSeq.collect {
      case to if !to.isInstanceOf[GeometryDescriptor] && Columns.indexed(to) &&
          !SftUserData.DtgField.get(existing).contains(to.getLocalName) &&
          Option(existing.getDescriptor(to.getLocalName)).exists(!Columns.indexed(_)) => to.getLocalName
    }
  }

  /**
   * Logs a warning if a feature type flags an attribute for an index that it doesn't have, when the feature type
   * is being used to update the existing tables - we don't add indices to existing tables, as that would block
   * writes while the index is built
   *
   * @param existing existing (underlying) feature type
   * @param updated updated (user-facing) feature type
   */
  private def warnIgnoredIndices(existing: SimpleFeatureType, updated: SimpleFeatureType): Unit = {
    val ignored = addedIndices(existing, updated)
    if (ignored.nonEmpty) {
      logger.warn(
        s"Ignoring index flags for existing attributes of '${updated.getTypeName}': ${ignored.mkString(", ")} - " +
            "indices can be added to existing attributes through updateSchema")
    }
  }

  /**
   * Verifies that an update doesn't change any user data configuration that is fixed when a schema is created
   *
   * @param existing existing (underlying) feature type
   * @param updated updated (user-facing) feature type
   */
  private def validateUserData(existing: SimpleFeatureType, updated: SimpleFeatureType): Unit = {
    SftUserData.Values.filterNot(_.updatable).foreach { config =>
      if (updated.getUserData.containsKey(config.key) && config.get(updated) != config.get(existing)) {
        throw new UnsupportedOperationException(
          s"Updating user data key '${config.key}' is not supported: " +
            s"${existing.getUserData.get(config.key)} -> ${updated.getUserData.get(config.key)}")
      }
    }
  }

  /**
   * Gets the visibility from a user-facing feature, ensuring it is not null or empty
   *
   * @param sf feature
   * @return
   */
  private def getVisibility(sf: SimpleFeature): String = {
    val visibility = SecurityUtils.getVisibility(sf)
    if (visibility == null || visibility.isBlank) {
      throw new IllegalArgumentException(
        s"Feature ${sf.getID} does not contain a visibility label under the user data key ${SecurityUtils.FEATURE_VISIBILITY}")
    }
    try {
      PgVis.toPostgisVisibility(visibility)
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Feature ${sf.getID} does not contain a valid visibility label: $visibility", e)
    }
  }

  /**
   * Types for cached schemas
   */
  private sealed trait SchemaType

  // the underlying type can be used directly
  private case class Passthrough(sft: SimpleFeatureType) extends SchemaType
  // the underlying type has visibilities and/or a different name than the user-facing type
  private case class Mapped(mapping: Mapping) extends SchemaType

  /**
   * The views backing each feature type
   *
   * @param names feature type name for each view, if it's different from the name of the view
   * @param generations previous generation for each view
   */
  private case class Generations(names: Map[String, String], generations: Map[String, String]) {

    // views that have been replaced with a newer generation
    private val superseded = generations.values.toSet

    /**
     * Gets the name of the view backing a feature type. Note that this does not verify that the view exists.
     *
     * @param typeName feature type name
     * @return
     */
    def view(typeName: String): Option[String] = {
      names.collectFirst { case (view, name) if name == typeName && !superseded.contains(view) => view }.orElse {
        // otherwise the view is the same as the type name, unless a view with that name is being used for something else
        if (names.contains(typeName) || superseded.contains(typeName)) { None } else { Some(typeName) }
      }
    }

    /**
     * Gets the feature type name for a view, if it's the current view for a feature type
     *
     * @param view view name
     * @return
     */
    def typeName(view: String): Option[String] =
      if (superseded.contains(view)) { None } else { Some(names.getOrElse(view, view)) }

    /**
     * Gets all the previous generations of a view, most recent first
     *
     * @param view view name
     * @return
     */
    def previous(view: String): List[String] = generations.get(view).toList.flatMap(p => p :: previous(p))
  }

  /**
   * A cached mapping of feature type names to views
   *
   * @param generations mapping
   * @param views names of all the views that existed when the mapping was loaded
   * @param expires time that the mapping expires, in millis since the epoch
   */
  private case class CachedGenerations(generations: Generations, views: Set[String], expires: Long)

  /**
   * Maps between a user-facing feature type and the underlying type, which may have a different name and/or
   * a visibility column
   *
   * @param userFacing user-facing feature type
   * @param underlying underlying feature type
   */
  private class Mapping(val userFacing: SimpleFeatureType, val underlying: SimpleFeatureType) {

    /**
     * Name of the underlying view
     */
    val view: String = underlying.getTypeName

    /**
     * Index of the visibility column in the underlying type, or -1
     */
    val vis: Int = underlying.indexOf(VisCol)

    // the default geometry, if it's different from the underlying type (which always uses the first geometry)
    private val geom = Option(userFacing.getGeometryDescriptor).map(_.getLocalName).filter { g =>
      Option(underlying.getGeometryDescriptor).forall(_.getLocalName != g)
    }

    // the last feature type that was checked against the user-facing type, and whether its attributes line up -
    // this avoids having to check the attributes for every feature
    @volatile
    private var aligned: (SimpleFeatureType, Boolean) = (userFacing, true)

    /**
     * Converts the result type of a query against the underlying type
     *
     * @param sft underlying result type, which may be a transform
     * @return
     */
    def toUserFacing(sft: SimpleFeatureType): SimpleFeatureType =
      PartitionedPostgisDataStore.toUserFacing(sft, userFacing.getTypeName, geom)

    /**
     * Project an underlying feature onto the user schema, dropping the `_vis` column but surfacing its
     * value into the feature's user data as the visibility expression (if non-null/non-empty).
     *
     * @param from underlying feature, as read by the jdbc store (i.e. the view name has already been removed from the id)
     * @param userFacingType user-facing type corresponding to the feature, which may be a transform
     * @param vis index of the vis column in the feature, or -1
     * @return user feature with visibility populated in user data
     */
    def toUserFacing(from: SimpleFeature, userFacingType: SimpleFeatureType, vis: Int): SimpleFeature =
      toUserFacing(from, new ScalaSimpleFeature(userFacingType, from.getID), vis)

    /**
     * Project an underlying feature onto the user schema, dropping the `_vis` column but surfacing its
     * value into the feature's user data as the visibility expression (if non-null/non-empty).
     *
     * @param from underlying feature
     * @param to user-facing feature to populate
     * @param vis index of the vis column in the underlying feature, or -1
     * @return user feature with visibility populated in user data
     */
    def toUserFacing[T <: ScalaSimpleFeature](from: SimpleFeature, to: T, vis: Int): T = {
      var i = 0
      var j = 0
      while (i < from.getAttributeCount) {
        if (i != vis) {
          to.setAttributeNoConvert(j, from.getAttribute(i))
          j += 1
        }
        i += 1
      }
      if (vis != -1) {
        val pgVis = from.getAttribute(vis).asInstanceOf[String]
        if (pgVis != null) {
          val visibility =
            try { PgVis.fromPostgisVisibility(pgVis) } catch {
              case NonFatal(e) =>
                logger.warn(s"Error converting visibilities back to standard syntax - may be pre-release data: $pgVis", e)
                pgVis
            }
          SecurityUtils.setFeatureVisibility(to, visibility)
        }
      }
      to.getUserData.putAll(from.getUserData)
      to
    }

    /**
     * Copies the attributes of a user-facing feature into an underlying feature, mapping the user-data
     * visibility into `_vis`.
     *
     * Attributes are copied by position, unless the feature doesn't line up with the user-facing type (for
     * example, if it was created before the feature type was updated), in which case they are matched by name.
     *
     * @param feature user-facing feature
     * @param to underlying feature
     * @param set function to set an attribute in the underlying feature
     */
    def toUnderlying(feature: SimpleFeature, to: SimpleFeature)(set: (Int, AnyRef) => Unit): Unit = {
      val from = align(feature)
      var i = 0
      var j = 0
      while (i < to.getAttributeCount) {
        if (i == vis) {
          set(i, getVisibility(from))
        } else {
          set(i, from.getAttribute(j))
          j += 1
        }
        i += 1
      }
      to.getUserData.putAll(from.getUserData)
    }

    /**
     * Converts a feature to the user-facing type, if its attributes don't line up with it. Attributes are
     * matched by name - any that don't exist in the user-facing type are dropped, and any that are missing are
     * left as null. This only happens rarely, for example, if a feature was created before the feature type was
     * updated
     *
     * @param from feature
     * @return
     */
    private def align(from: SimpleFeature): SimpleFeature = {
      val sft = from.getFeatureType
      val (checked, result) = aligned
      val matches = if (checked eq sft) { result } else {
        val names = sft.getAttributeDescriptors.asScala.map(_.getLocalName)
        val result = names == userFacing.getAttributeDescriptors.asScala.map(_.getLocalName)
        aligned = (sft, result)
        result
      }
      if (matches) { from } else {
        val to = new ScalaSimpleFeature(userFacing, from.getID)
        var i = 0
        while (i < userFacing.getAttributeCount) {
          val j = sft.indexOf(userFacing.getDescriptor(i).getLocalName)
          if (j != -1) {
            to.setAttribute(i, from.getAttribute(j))
          }
          i += 1
        }
        to.getUserData.putAll(from.getUserData)
        to
      }
    }

    /**
     * Converts a filter against the user-facing type, to account for references to the default geometry
     *
     * @param filter filter
     * @return
     */
    def toUnderlying(filter: Filter): Filter = {
      geom match {
        case Some(g) if filter != null => filter.accept(new UnderlyingFilterVisitor(g), null).asInstanceOf[Filter]
        case _ => filter
      }
    }

    /**
     * Converts a query against the user-facing type
     *
     * @param query query
     * @param includeVis ensure that the `_vis` col is always retrieved
     * @return
     */
    def toUnderlying(query: Query, includeVis: Boolean = true): Query = {
      val updated = new Query(query)
      updated.setTypeName(view)
      updated.setFilter(toUnderlying(query.getFilter))
      if (includeVis && vis != -1 && !query.retrieveAllProperties()) {
        updated.setPropertyNames(query.getPropertyNames :+ VisCol: _*)
      }
      updated
    }
  }

  /**
   * Converts a filter against a user-facing type into a filter against the underlying type
   *
   * @param geom name of the default geometry, used to replace any references to the default geometry (which
   *             would otherwise be resolved against the default geometry of the underlying type)
   */
  private class UnderlyingFilterVisitor(geom: String) extends DuplicatingFilterVisitor {

    override def visit(expression: PropertyName, extraData: AnyRef): AnyRef = {
      val name = expression.getPropertyName
      // note: an empty property name refers to the default geometry
      if (name == null || name.isEmpty) {
        getFactory(extraData).property(geom, expression.getNamespaceContext)
      } else {
        super.visit(expression, extraData)
      }
    }
  }

  private def removeFidPrefix(id: String, prefix: String): String =
    if (id.startsWith(prefix)) { id.substring(prefix.length) } else { id }

  /**
   * Feature factory that removes the view name from feature ids, for features that are read by the jdbc store
   *
   * @param delegate delegate factory
   */
  private class FidFeatureFactory(delegate: FeatureFactory) extends FeatureFactory {

    override def createSimpleFeature(array: Array[AnyRef], sft: SimpleFeatureType, id: String): SimpleFeature = {
      // note: the type name will be the name of the view, even if the type is a transform
      val prefix = sft.getTypeName
      val fid =
        if (id != null && id.length > prefix.length && id.charAt(prefix.length) == '.' && id.startsWith(prefix)) {
          id.substring(prefix.length + 1)
        } else {
          id
        }
      delegate.createSimpleFeature(array, sft, fid)
    }

    override def createSimpleFeautre(array: Array[AnyRef], descriptor: AttributeDescriptor, id: String): SimpleFeature =
      delegate.createSimpleFeautre(array, descriptor, id)
    override def createAssociation(value: Attribute, descriptor: AssociationDescriptor): Association =
      delegate.createAssociation(value, descriptor)
    override def createAttribute(value: AnyRef, descriptor: AttributeDescriptor, id: String): Attribute =
      delegate.createAttribute(value, descriptor, id)
    override def createGeometryAttribute(
        geometry: AnyRef, descriptor: GeometryDescriptor, id: String, crs: CoordinateReferenceSystem): GeometryAttribute =
      delegate.createGeometryAttribute(geometry, descriptor, id, crs)
    override def createComplexAttribute(
        value: java.util.Collection[Property], descriptor: AttributeDescriptor, id: String): ComplexAttribute =
      delegate.createComplexAttribute(value, descriptor, id)
    override def createComplexAttribute(value: java.util.Collection[Property], t: ComplexType, id: String): ComplexAttribute =
      delegate.createComplexAttribute(value, t, id)
    override def createFeature(value: java.util.Collection[Property], descriptor: AttributeDescriptor, id: String): Feature =
      delegate.createFeature(value, descriptor, id)
    override def createFeature(value: java.util.Collection[Property], t: FeatureType, id: String): Feature =
      delegate.createFeature(value, t, id)
  }

  /**
   * Feature writer that removes the view name from feature ids.
   *
   * Existing features are returned by the jdbc writer with the view name in their id. New features don't have
   * the view name initially, but the jdbc writer re-sets their id (with the view name) once they have been
   * inserted. The jdbc writer relies on the ids of its features (for example, to remove a feature), so we don't
   * modify them - instead, we wrap each feature, and remove the view name whenever the id is accessed.
   *
   * The jdbc writer re-uses its features (one for updates, plus one per buffered insert), so we re-use our
   * wrappers in the same way, instead of creating a new one for every feature that's written.
   *
   * @param delegate jdbc feature writer
   * @param view name of the view
   */
  private class FidFeatureWriter(delegate: FeatureWriter[SimpleFeatureType, SimpleFeature], view: String)
      extends FeatureWriter[SimpleFeatureType, SimpleFeature] {

    private val prefix = s"$view."
    // wrappers for the features re-used by the jdbc writer, keyed by identity
    private val wrappers = new java.util.IdentityHashMap[SimpleFeature, FidFeature]()

    override def getFeatureType: SimpleFeatureType = delegate.getFeatureType
    override def hasNext: Boolean = delegate.hasNext
    override def next(): SimpleFeature = {
      val feature = delegate.next()
      var wrapper = wrappers.get(feature)
      if (wrapper == null) {
        wrapper = new FidFeature(feature, prefix)
        wrappers.put(feature, wrapper)
      }
      wrapper
    }
    override def write(): Unit = delegate.write()
    override def remove(): Unit = delegate.remove()
    override def close(): Unit = delegate.close()
  }

  /**
   * Wrapper for a feature returned by a jdbc feature writer, that removes the view name from the feature id.
   *
   * Note that the jdbc writer re-uses its features, but gives them a new id each time, so the id is always
   * resolved from the jdbc feature, and is only wrapped if it's accessed.
   *
   * @param feature jdbc feature
   * @param prefix view name prefix
   */
  private class FidFeature(feature: SimpleFeature, prefix: String) extends DecoratingFeature(feature) {
    private var fid: WriterFeatureId = _
    override def getIdentifier: FeatureId = {
      val id = feature.getIdentifier
      if (fid == null || (fid.delegate ne id)) {
        fid = new WriterFeatureId(id, prefix)
      }
      fid
    }
    override def getID: String = removeFidPrefix(feature.getID, prefix)
  }

  /**
   * Feature id for a feature returned by a jdbc feature writer, that removes the view name whenever the id is
   * accessed. Setting the id will update the id of the jdbc feature, which is how the jdbc writer picks up
   * provided feature ids.
   *
   * Note that this references the id of the jdbc feature, and not the feature itself. The jdbc writer updates the id
   * once a new feature has been inserted, but it re-uses the feature itself (with a new id) for subsequent writes.
   *
   * @param delegate id of the jdbc feature
   * @param prefix view name prefix
   */
  private class WriterFeatureId(val delegate: FeatureId, prefix: String) extends FeatureIdImpl("") {
    override def getID: String = removeFidPrefix(delegate.getID, prefix)
    override def setID(id: String): Unit = {
      delegate match {
        case d: FeatureIdImpl => d.setID(id)
        case _ => throw new UnsupportedOperationException(s"Can't set feature id on ${delegate.getClass.getName}")
      }
    }
    override def toString: String = getID
    override def equals(obj: Any): Boolean = obj match {
      case fid: FeatureId => getID == fid.getID
      case _ => false
    }
    override def hashCode: Int = getID.hashCode
    override def equalsExact(fid: FeatureId): Boolean = {
      fid != null && {
        val id = getID
        id == fid.getID && id == fid.getRid && fid.getPreviousRid == null && fid.getFeatureVersion == null
      }
    }
  }

  /**
   * Feature reader that re-types features to the user schema, dropping `_vis` (but surfacing its
   * value into the feature's user data as the visibility)
   */
  private class MappedFeatureReader(delegate: FeatureReader[SimpleFeatureType, SimpleFeature], mapping: Mapping)
      extends FeatureReader[SimpleFeatureType, SimpleFeature] {
    private lazy val userFacingType = mapping.toUserFacing(delegate.getFeatureType)
    private lazy val vis = delegate.getFeatureType.indexOf(VisCol)
    override def getFeatureType: SimpleFeatureType = userFacingType
    override def hasNext: Boolean = delegate.hasNext
    override def next(): SimpleFeature = mapping.toUserFacing(delegate.next(), userFacingType, vis)
    override def close(): Unit = delegate.close()
  }

  /**
   * Wraps a feature collection so its features are re-typed to the user schema, dropping `_vis`
   */
  private class MappedFeatureCollection(delegate: SimpleFeatureCollection, mapping: Mapping)
      extends DecoratingSimpleFeatureCollection(delegate) {

    private lazy val userFacingType = mapping.toUserFacing(delegate.getSchema)
    private lazy val vis = delegate.getSchema.indexOf(VisCol)

    override def getSchema: SimpleFeatureType = userFacingType

    override def features(): SimpleFeatureIterator = new SimpleFeatureIterator {
      private val iter = delegate.features()
      override def hasNext: Boolean = iter.hasNext
      override def next(): SimpleFeature = mapping.toUserFacing(iter.next(), userFacingType, vis)
      override def close(): Unit = iter.close()
    }
  }

  /**
   * Feature writer that exposes the user schema but maps visibility into `_vis` on write. The
   * delegate feature returned by `next()` is retained so that `write()` can copy the caller's
   * attributes (including visibility) back into it before persisting.
   */
  private class MappedFeatureWriter(delegate: FeatureWriter[SimpleFeatureType, SimpleFeature], mapping: Mapping)
      extends FeatureWriter[SimpleFeatureType, SimpleFeature] {

    private var delegateFeature: SimpleFeature = _
    private var userFeature: ModifiedAttributesFeature = _
    private val prefix = s"${mapping.view}."

    override def getFeatureType: SimpleFeatureType = mapping.userFacing

    override def hasNext: Boolean = delegate.hasNext

    override def next(): SimpleFeature = {
      delegateFeature = delegate.next()
      // present a user-schema view (without '_vis') for the caller to populate
      // note: the id references the delegate feature, so that it reflects the id that is persisted on insert
      val id = new WriterFeatureId(delegateFeature.getIdentifier, prefix)
      userFeature = mapping.toUserFacing(delegateFeature, new ModifiedAttributesFeature(mapping.userFacing, id), mapping.vis)
      userFeature.clearModified()
      userFeature
    }

    override def write(): Unit = {
      // note: we only copy back attributes that were set by the caller - when updating an existing feature, the values
      // in the delegate feature may be raw jdbc types (e.g. arrays), which will be corrupted if we set them back as-is
      mapping.toUnderlying(userFeature, delegateFeature) { (i, value) =>
        val j = if (mapping.vis == -1 || i < mapping.vis) { i } else { i - 1 }
        if (i == mapping.vis || userFeature.isModified(j)) {
          delegateFeature.setAttribute(i, value)
        }
      }
      // propagate a caller-provided fid - the jdbc writer reads the id off the delegate feature itself
      // note: an id that is set directly on the user feature has already been set on the delegate feature
      val provided = userFeature.getUserData.get(Hints.PROVIDED_FID).asInstanceOf[String]
      if (provided != null && provided != userFeature.getID) {
        userFeature.setId(provided)
      }
      delegate.write()
    }

    override def remove(): Unit = delegate.remove()

    override def close(): Unit = delegate.close()
  }

  /**
   * Feature that tracks which attributes have been set, same as the features returned by the jdbc feature writers.
   * This catches attributes that are modified in place and then set back (like adding an element to a list), which
   * can't be detected by comparing values.
   *
   * @param sft simple feature type
   * @param fid feature id, referencing the id of the feature returned by the jdbc feature writer
   */
  private class ModifiedAttributesFeature(sft: SimpleFeatureType, fid: WriterFeatureId)
      extends ScalaSimpleFeature(sft, fid.getID) {

    private val modified = Array.ofDim[Boolean](sft.getAttributeCount)

    def isModified(index: Int): Boolean = modified(index)

    def clearModified(): Unit = java.util.Arrays.fill(modified, false)

    override def getID: String = fid.getID
    override def getIdentifier: FeatureId = fid
    override def setId(id: String): Unit = fid.setID(id)

    // note: all the various `set` methods delegate to this one
    override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = {
      super.setAttributeNoConvert(index, value)
      modified(index) = true
    }
  }

  /**
   * Feature store wrapper - maps visibility into `_vis` on write, since the store's own writer path
   * bypasses the data store's `getFeatureWriter*` methods.
   */
  private class MappedFeatureStore(source: SimpleFeatureStore, mapping: Mapping) extends SimpleFeatureStore {

    import mapping.{underlying, userFacing}

    override def getDataStore: DataAccess[SimpleFeatureType, SimpleFeature] = source.getDataStore

    override def addFeatures(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]): java.util.List[FeatureId] = {
      val mapped = new DecoratingSimpleFeatureCollection(DataUtilities.simple(collection)) {
        override def getSchema: SimpleFeatureType = underlying
        override def features(): SimpleFeatureIterator = new SimpleFeatureIterator {
          private val iter = collection.features()
          override def hasNext: Boolean = iter.hasNext
          override def next(): SimpleFeature = toUnderlying(iter.next())
          override def close(): Unit = iter.close()
        }
      }
      // note: the view name has already been removed from the ids
      source.addFeatures(mapped)
    }

    override def setFeatures(reader: FeatureReader[SimpleFeatureType, SimpleFeature]): Unit = {
      val mapped = new FeatureReader[SimpleFeatureType, SimpleFeature] {
        override def getFeatureType: SimpleFeatureType = underlying
        override def hasNext: Boolean = reader.hasNext
        override def next(): SimpleFeature = toUnderlying(reader.next())
        override def close(): Unit = reader.close()
      }
      source.setFeatures(mapped)
    }

    // read-side overrides - hides `_vis` from the exposed schema and from returned features
    override def getSchema: SimpleFeatureType = userFacing
    override def getName: Name = userFacing.getName
    override def getFeatures: SimpleFeatureCollection = new MappedFeatureCollection(source.getFeatures, mapping)
    override def getFeatures(filter: Filter): SimpleFeatureCollection =
      new MappedFeatureCollection(source.getFeatures(mapping.toUnderlying(filter)), mapping)
    override def getFeatures(query: Query): SimpleFeatureCollection =
      new MappedFeatureCollection(source.getFeatures(mapping.toUnderlying(query)), mapping)
    override def getInfo: ResourceInfo = source.getInfo
    override def getQueryCapabilities: QueryCapabilities = source.getQueryCapabilities
    override def getSupportedHints: java.util.Set[RenderingHints.Key] = source.getSupportedHints
    override def getBounds: ReferencedEnvelope = source.getBounds
    override def getBounds(query: Query): ReferencedEnvelope = source.getBounds(mapping.toUnderlying(query, includeVis = false))
    override def getCount(query: Query): Int = source.getCount(mapping.toUnderlying(query, includeVis = false))
    override def addFeatureListener(listener: FeatureListener): Unit = source.addFeatureListener(listener)
    override def removeFeatureListener(listener: FeatureListener): Unit = source.removeFeatureListener(listener)

    // write-side overrides - these don't need to explicitly deal with _vis
    override def removeFeatures(filter: Filter): Unit = source.removeFeatures(mapping.toUnderlying(filter))
    override def modifyFeatures(attributeName: Name, attributeValue: AnyRef, filter: Filter): Unit =
      source.modifyFeatures(attributeName, attributeValue, mapping.toUnderlying(filter))
    override def modifyFeatures(attributeNames: Array[Name], attributeValues: Array[AnyRef], filter: Filter): Unit =
      source.modifyFeatures(attributeNames, attributeValues, mapping.toUnderlying(filter))
    override def modifyFeatures(name: String, attributeValue: AnyRef, filter: Filter): Unit =
      source.modifyFeatures(name, attributeValue, mapping.toUnderlying(filter))
    override def modifyFeatures(names: Array[String], attributeValues: Array[AnyRef], filter: Filter): Unit =
      source.modifyFeatures(names, attributeValues, mapping.toUnderlying(filter))
    override def setTransaction(tx: Transaction): Unit = source.setTransaction(tx)
    override def getTransaction: Transaction = source.getTransaction

    /**
     * Project a user-facing feature onto the underlying schema, mapping the user-data visibility into `_vis`.
     * Other attributes are copied by position, or by name if the feature doesn't line up with the user-facing type.
     *
     * @param from user feature
     * @return physical feature with `_vis` populated
     */
    private def toUnderlying(from: SimpleFeature): SimpleFeature = {
      val fid = Option(from.getUserData.get(Hints.PROVIDED_FID).asInstanceOf[String]).getOrElse(from.getID)
      val to = new ScalaSimpleFeature(underlying, fid)
      mapping.toUnderlying(from, to)(to.setAttributeNoConvert)
      to
    }
  }

  /**
   * Replaces the JDBCFeatureSource with overrides to correctly resolve json path attributes. The method comes
   * from ContentFeatureSource, so we can't override it in our delegating vis store/source (above). Since JDBCFeatureStore is
   * a final class, write methods are re-routed back to the data store - this means that doing updates based on json-path
   * expressions will not work.
   *
   * @param ds data store
   * @param entry content entry
   * @param query query
   */
  private class JsonPathJdbcFeatureStore(ds: JDBCDataStore, entry: ContentEntry, query: Query)
      extends JDBCFeatureSource(entry, query) with SimpleFeatureStore {

    /**
     * Resolves filter property names against the schema, but preserves JSON paths.
     *
     * The base class runs `DataUtilities.resolvePropertyNames`, whose `PropertyNameResolvingVisitor`
     * evaluates each property name against the feature type and rewrites it to the resolved attribute's
     * local name. A JSON path like `$.props.name` evaluates (via GeoMesa's JSON property accessor) to
     * the `props` descriptor, so the default would collapse the whole path to `props` - losing the nested
     * field that IcebergFilterConverter needs to push down. We keep `$`-prefixed names verbatim and
     * resolve everything else as usual.
     *
     * @param query the query being planned
     * @return the query with non-JSON-path property names resolved
     */
    override protected def resolvePropertyNames(query: Query): Query = {
      val filter = query.getFilter
      if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE) { query } else {
        val resolved = filter.accept(new JsonPathPropertyNameResolver(getSchema), null).asInstanceOf[Filter]
        if (resolved == filter) { query } else {
          val newQuery = new Query(query)
          newQuery.setFilter(resolved)
          newQuery
        }
      }
    }

    override def setFeatures(reader: FeatureReader[SimpleFeatureType, SimpleFeature]): Unit = {
      WithClose(ds.getFeatureWriter(entry.getTypeName, Filter.INCLUDE, getTransaction)) { writer =>
        while (writer.hasNext) {
          writer.next()
          writer.remove()
        }
      }
      WithClose(ds.getFeatureWriterAppend(entry.getTypeName, getTransaction)) { writer =>
        while (reader.hasNext) {
          FeatureUtils.write(writer, reader.next())
        }
      }
    }

    override def addFeatures(features: FeatureCollection[SimpleFeatureType, SimpleFeature]): java.util.List[FeatureId] = {
      val written = new java.util.ArrayList[FeatureId]()
      WithClose(ds.getFeatureWriterAppend(entry.getTypeName, getTransaction)) { writer =>
        WithClose(features.features()) { iter =>
          while (iter.hasNext) {
            written.add(FeatureUtils.write(writer, iter.next()).getIdentifier)
          }
        }
      }
      // note: features are inserted in batches, and the jdbc writer doesn't set their actual ids until they have been
      // inserted - so we have to wait until the writer is closed (which inserts any remaining features) to read them
      val prefix = s"${entry.getTypeName}."
      val ids = new java.util.ArrayList[FeatureId](written.size())
      written.forEach(id => ids.add(new FeatureIdImpl(removeFidPrefix(id.getID, prefix))))
      ids
    }

    override def removeFeatures(filter: Filter): Unit = {
      WithClose(ds.getFeatureWriter(entry.getTypeName, filter, getTransaction)) { writer =>
        while (writer.hasNext) {
          writer.next()
          writer.remove()
        }
      }
    }

    override def modifyFeatures(attributeName: String, attributeValue: AnyRef, filter: Filter): Unit =
      modifyFeatures(Array(attributeName), Array(attributeValue), filter)

    override def modifyFeatures(attributeNames: Array[Name], attributeValues: Array[AnyRef], filter: Filter): Unit =
      modifyFeatures(attributeNames.map(_.getLocalPart), attributeValues, filter)

    override def modifyFeatures(attributeName: Name, attributeValue: AnyRef, filter: Filter): Unit =
      modifyFeatures(Array(attributeName.getLocalPart), Array(attributeValue), filter)

    override def modifyFeatures(attributeNames: Array[String], attributeValues: Array[AnyRef], filter: Filter): Unit = {
      if(attributeNames.length != attributeValues.length) {
        throw new IllegalArgumentException(
          s"Attribute names and values do not align: ${attributeNames.length} names (${attributeNames.mkString(", ")}) vs " +
            s"${attributeValues.length} values (${attributeValues.mkString(", ")})")
      }
      WithClose(ds.getFeatureWriter(entry.getTypeName, filter, getTransaction)) { writer =>
        while (writer.hasNext) {
          val sf = writer.next()
          var i = 0
          while (i < attributeNames.length) {
            sf.setAttribute(attributeNames(i), attributeValues(i))
            i += 1
          }
          writer.write()
        }
      }
    }

  }
}
