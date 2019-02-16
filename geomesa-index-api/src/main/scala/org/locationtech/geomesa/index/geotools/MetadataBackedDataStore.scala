/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.time.{Instant, ZoneOffset}
import java.util.{List => jList}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureWriter}
import org.geotools.factory.Hints
import org.geotools.feature.{FeatureTypes, NameImpl}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceConfig
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata._
import org.locationtech.geomesa.index.metadata.HasGeoMesaMetadata
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.utils.{DistributedLocking, Releasable}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.{DEFAULT_DATE_KEY, ST_INDEX_SCHEMA_KEY, TABLE_SHARING_KEY}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs.SHARING_PREFIX_KEY
import org.locationtech.geomesa.utils.geotools.{GeoToolsDateFormat, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
  * Abstract base class for data store implementations using metadata to track schemas
  */
abstract class MetadataBackedDataStore(config: NamespaceConfig) extends DataStore
    with HasGeoMesaMetadata[String] with DistributedLocking with LazyLogging {

  // TODO: GEOMESA-2360 - Remove global axis order hint from MetadataBackedDataStore
  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  protected [geomesa] val interceptors = QueryInterceptorFactory(this)

  // hooks to allow extended functionality

  /**
    * Called just before persisting schema metadata. Allows for validation or configuration of user data
    *
    * @param sft simple feature type
    * @throws java.lang.IllegalArgumentException if schema is invalid and shouldn't be written
    */
  @throws(classOf[IllegalArgumentException])
  protected def preSchemaCreate(sft: SimpleFeatureType): Unit

  /**
    * Called just before updating schema metadata. Allows for validation or configuration of user data
    *
    * @param sft simple feature type
    * @param previous previous feature type before changes
    * @throws java.lang.IllegalArgumentException if schema is invalid and shouldn't be updated
    */
  @throws(classOf[IllegalArgumentException])
  protected def preSchemaUpdate(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit

  /**
    * Called after schema metadata has been persisted. Allows for creating tables, etc
    *
    * @param sft simple feature type
    */
  protected def onSchemaCreated(sft: SimpleFeatureType): Unit

  /**
    * Called after schema metadata has been persisted. Allows for creating tables, etc
    *
    * @param sft simple feature type
    * @param previous previous feature type before changes
    */
  protected def onSchemaUpdated(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit

  /**
    * Called after deleting schema metadata. Allows for deleting tables, etc
    *
    * @param sft simple feature type
    */
  protected def onSchemaDeleted(sft: SimpleFeatureType): Unit

  // methods from org.geotools.data.DataStore

  /**
    * @see org.geotools.data.DataStore#getTypeNames()
    * @return existing simple feature type names
    */
  override def getTypeNames: Array[String] = metadata.getFeatureTypes

  /**
    * @see org.geotools.data.DataAccess#getNames()
    * @return existing simple feature type names
    */
  override def getNames: jList[Name] = {
    val names = new java.util.ArrayList[Name]
    config.namespace match {
      case None     => getTypeNames.foreach(name => names.add(new NameImpl(name)))
      case Some(ns) => getTypeNames.foreach(name => names.add(new NameImpl(ns, name)))
    }
    names
  }

  /**
    * Validates the schema and writes metadata to catalog.If the schema already exists,
    * continue without error.
    *
    * This method uses distributed locking to ensure a schema is only created once.
    *
    * @see org.geotools.data.DataAccess#createSchema(org.opengis.feature.type.FeatureType)
    * @param schema type to create
    */
  override def createSchema(schema: SimpleFeatureType): Unit = {
    if (getSchema(schema.getTypeName) == null) {
      val lock = acquireCatalogLock()
      try {
        // check a second time now that we have the lock
        if (getSchema(schema.getTypeName) == null) {
          // ensure that we have a mutable type so we can set user data
          val sft = SimpleFeatureTypes.mutable(schema)
          // inspect and update the simple feature type for various components
          // do this before anything else so that any modifications will be in place
          GeoMesaSchemaValidator.validate(sft)

          // set the enabled indices
          preSchemaCreate(sft)

          try {
            // write out the metadata to the catalog table
            // compute the metadata values - IMPORTANT: encode type has to be called after all user data is set
            val metadataMap = Map(
              ATTRIBUTES_KEY       -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
              STATS_GENERATION_KEY -> GeoToolsDateFormat.format(Instant.now().atOffset(ZoneOffset.UTC))
            )
            metadata.insert(sft.getTypeName, metadataMap)

            // reload the sft so that we have any default metadata,
            // then copy over any additional keys that were in the original sft.
            // avoid calling getSchema directly, as that may trigger a remote version
            // check for indices that haven't been created yet
            val attributes = metadata.readRequired(sft.getTypeName, ATTRIBUTES_KEY)
            val reloadedSft = SimpleFeatureTypes.createType(sft.getTypeName, attributes)
            (sft.getUserData.keySet -- reloadedSft.getUserData.keySet).foreach { k =>
              reloadedSft.getUserData.put(k, sft.getUserData.get(k))
            }

            // create the tables
            onSchemaCreated(reloadedSft)
          } catch {
            case NonFatal(e) =>
              // If there was an error creating a schema, clean up.
              try {
                metadata.delete(sft.getTypeName)
              } catch {
                case NonFatal(e2) => e.addSuppressed(e2)
              }
              throw e
          }
        }
      } finally {
        lock.release()
      }
    }
  }

  /**
    * @see org.geotools.data.DataAccess#getSchema(org.opengis.feature.type.Name)
    * @param name feature type name
    * @return feature type, or null if it does not exist
    */
  override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)

  /**
   * @see org.geotools.data.DataStore#getSchema(java.lang.String)
   * @param typeName feature type name
   * @return feature type, or null if it does not exist
   */
  override def getSchema(typeName: String): SimpleFeatureType = {
    metadata.read(typeName, ATTRIBUTES_KEY) match {
      case None => null
      case Some(spec) => SimpleFeatureTypes.createImmutableType(config.namespace.orNull, typeName, spec)
    }
  }

  /**
    * Allows the following modifications to the schema:
    *   modifying keywords through user-data
    *   enabling/disabling indices through RichSimpleFeatureType.setIndexVersion (SimpleFeatureTypes.INDEX_VERSIONS)
    *   appending of new attributes
    *
    * Other modifications are not supported.
    *
    * @see org.geotools.data.DataStore#updateSchema(java.lang.String, org.opengis.feature.simple.SimpleFeatureType)
    * @param typeName simple feature type name
    * @param sft new simple feature type
    */
  override def updateSchema(typeName: String, sft: SimpleFeatureType): Unit =
    updateSchema(new NameImpl(typeName), sft)

  /**
    * Allows the following modifications to the schema:
    *   modifying keywords through user-data
    *   enabling/disabling indices through RichSimpleFeatureType.setIndexVersion (SimpleFeatureTypes.INDEX_VERSIONS)
    *   appending of new attributes
    *
    * Other modifications are not supported.
    *
    * @see org.geotools.data.DataAccess#updateSchema(org.opengis.feature.type.Name, org.opengis.feature.type.FeatureType)
    * @param typeName simple feature type name
    * @param schema new simple feature type
    */
  override def updateSchema(typeName: Name, schema: SimpleFeatureType): Unit = {
    // validate type name has not changed
    if (typeName.getLocalPart != schema.getTypeName) {
      val msg = s"Updating the name of a schema is not allowed: '$typeName' changed to '${schema.getTypeName}'"
      throw new UnsupportedOperationException(msg)
    }

    val lock = acquireCatalogLock()
    try {
      // Get previous schema and user data
      val previousSft = getSchema(typeName)

      if (previousSft == null) {
        throw new IllegalArgumentException(s"Schema '$typeName' does not exist")
      }

      // validate that default geometry has not changed
      if (schema.getGeomField != previousSft.getGeomField) {
        throw new UnsupportedOperationException("Changing the default geometry is not supported")
      }

      // Check that unmodifiable user data has not changed
      MetadataBackedDataStore.unmodifiableUserdataKeys.foreach { key =>
        if (schema.userData[Any](key) != previousSft.userData[Any](key)) {
          throw new UnsupportedOperationException(s"Updating '$key' is not supported")
        }
      }

      // Check that the rest of the schema has not changed (columns, types, etc)
      val previousColumns = previousSft.getAttributeDescriptors
      val currentColumns = schema.getAttributeDescriptors
      if (previousColumns.toSeq != currentColumns.take(previousColumns.length)) {
        throw new UnsupportedOperationException("Updating schema columns is not allowed")
      }

      val sft = SimpleFeatureTypes.mutable(schema)

      preSchemaUpdate(sft, previousSft)

      // If all is well, update the metadata
      val attributesValue = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
      metadata.insert(sft.getTypeName, ATTRIBUTES_KEY, attributesValue)

      onSchemaUpdated(sft, previousSft)
    } finally {
      lock.release()
    }
  }

  /**
    * Deletes the schema metadata
    *
    * @see org.geotools.data.DataStore#removeSchema(java.lang.String)
    * @param typeName simple feature type name
    */
  override def removeSchema(typeName: String): Unit = {
    val lock = acquireCatalogLock()
    try {
      Option(getSchema(typeName)).foreach { sft =>
        onSchemaDeleted(sft)
        metadata.delete(typeName)
      }
    } finally {
      lock.release()
    }
  }

  /**
    * @see org.geotools.data.DataAccess#removeSchema(org.opengis.feature.type.Name)
    * @param typeName simple feature type name
    */
  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  /**
    * @see org.geotools.data.DataStore#getFeatureSource(java.lang.String)
    * @param typeName simple feature type name
    * @return featureStore, suitable for reading and writing
    */
  override def getFeatureSource(typeName: Name): SimpleFeatureSource = getFeatureSource(typeName.getLocalPart)

  /**
    * Create a general purpose writer that is capable of updates and deletes.
    * Does <b>not</b> allow inserts. Will return all existing features.
    *
    * @see org.geotools.data.DataStore#getFeatureWriter(java.lang.String, org.geotools.data.Transaction)
    * @param typeName feature type name
    * @param transaction transaction (currently ignored)
    * @return feature writer
    */
  override def getFeatureWriter(typeName: String, transaction: Transaction): SimpleFeatureWriter =
    getFeatureWriter(typeName, Filter.INCLUDE, transaction)

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
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): SimpleFeatureWriter

  /**
    * Creates a feature writer only for writing - does not allow updates or deletes.
    *
    * @see org.geotools.data.DataStore#getFeatureWriterAppend(java.lang.String, org.geotools.data.Transaction)
    * @param typeName feature type name
    * @param transaction transaction (currently ignored)
    * @return feature writer
    */
  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): SimpleFeatureWriter

  /**
    * @see org.geotools.data.DataAccess#getInfo()
    * @return service info
    */
  override def getInfo: ServiceInfo = {
    val info = new DefaultServiceInfo()
    info.setDescription(s"Features from ${getClass.getSimpleName}")
    info.setSchema(FeatureTypes.DEFAULT_NAMESPACE)
    info
  }

  /**
    * We always return null, which indicates that we are handling transactions ourselves.
    *
    * @see org.geotools.data.DataStore#getLockingManager()
    * @return locking manager - null
    */
  override def getLockingManager: LockingManager = null

  /**
    * Cleanup any open connections, etc. Equivalent to java.io.Closeable.close()
    *
    * @see org.geotools.data.DataAccess#dispose()
    */
  override def dispose(): Unit = {
    CloseWithLogging(metadata)
    CloseWithLogging(interceptors)
  }

  // end methods from org.geotools.data.DataStore

  /**
    * Acquires a distributed lock for all data stores sharing this catalog table.
    * Make sure that you 'release' the lock in a finally block.
    */
  protected [geomesa] def acquireCatalogLock(): Releasable = {
    import org.locationtech.geomesa.index.DistributedLockTimeout
    val path = s"/org.locationtech.geomesa/ds/${config.catalog}"
    val timeout = DistributedLockTimeout.toDuration.getOrElse {
      // note: should always be a valid fallback value so this exception should never be triggered
      throw new IllegalArgumentException(s"Couldn't convert '${DistributedLockTimeout.get}' to a duration")
    }
    acquireDistributedLock(path, timeout.toMillis).getOrElse {
      throw new RuntimeException(s"Could not acquire distributed lock at '$path' within $timeout")
    }
  }
}

object MetadataBackedDataStore {
  private val unmodifiableUserdataKeys = Set(TABLE_SHARING_KEY, SHARING_PREFIX_KEY, DEFAULT_DATE_KEY, ST_INDEX_SCHEMA_KEY)
}