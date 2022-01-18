/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.time.{Instant, ZoneOffset}
import java.util.{Locale, List => jList}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureWriter}
import org.geotools.feature.{FeatureTypes, NameImpl}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceConfig
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata._
import org.locationtech.geomesa.index.metadata.HasGeoMesaMetadata
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.utils.{DistributedLocking, Releasable}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs.TableSharingPrefix
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, GeoToolsDateFormat, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.{GeoMesaSchemaValidator, ReservedWordCheck}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
  * Abstract base class for data store implementations using metadata to track schemas
  */
abstract class MetadataBackedDataStore(config: NamespaceConfig) extends DataStore
    with HasGeoMesaMetadata[String] with DistributedLocking with LazyLogging {

  import scala.collection.JavaConverters._

  // TODO: GEOMESA-2360 - Remove global axis order hint from MetadataBackedDataStore
  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  protected [geomesa] val interceptors: QueryInterceptorFactory = QueryInterceptorFactory(this)

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
              AttributesKey      -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
              StatsGenerationKey -> GeoToolsDateFormat.format(Instant.now().atOffset(ZoneOffset.UTC))
            )
            metadata.insert(sft.getTypeName, metadataMap)

            // reload the sft so that we have any default metadata,
            // then copy over any additional keys that were in the original sft.
            // avoid calling getSchema directly, as that may trigger a remote version
            // check for indices that haven't been created yet
            val attributes = metadata.readRequired(sft.getTypeName, AttributesKey)
            val reloadedSft = SimpleFeatureTypes.createType(sft.getTypeName, attributes)
            val userData = new java.util.HashMap[AnyRef, AnyRef]()
            userData.putAll(reloadedSft.getUserData)
            reloadedSft.getUserData.putAll(sft.getUserData)
            reloadedSft.getUserData.putAll(userData)

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
    metadata.read(typeName, AttributesKey) match {
      case None => null
      case Some(spec) => SimpleFeatureTypes.createImmutableType(config.namespace.orNull, typeName, spec)
    }
  }

  /**
    * Allows the following modifications to the schema:
    *   renaming the feature type
    *   renaming attributes
    *   appending new attributes
    *   enabling/disabling indices through RichSimpleFeatureType.setIndexVersion
    *   modifying keywords through user-data
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
    *   renaming the feature type
    *   renaming attributes
    *   appending new attributes
    *   enabling/disabling indices through RichSimpleFeatureType.setIndexVersion
    *   modifying keywords through user-data
    *
    * Other modifications are not supported.
    *
    * @see org.geotools.data.DataAccess#updateSchema(org.opengis.feature.type.Name, org.opengis.feature.type.FeatureType)
    * @param typeName simple feature type name
    * @param schema new simple feature type
    */
  override def updateSchema(typeName: Name, schema: SimpleFeatureType): Unit = {
    // validate that the type name has not changed, or that the new name is not already in use
    if (typeName.getLocalPart != schema.getTypeName && getTypeNames.contains(schema.getTypeName)) {
      throw new IllegalArgumentException(
        s"Updated type name already exists: '$typeName' changed to '${schema.getTypeName}'")
    }

    val lock = acquireCatalogLock()
    try {
      // get previous schema and user data
      val previousSft = getSchema(typeName)

      if (previousSft == null) {
        throw new IllegalArgumentException(s"Schema '$typeName' does not exist")
      }

      validateSchemaUpdate(previousSft, schema)

      val sft = SimpleFeatureTypes.mutable(schema)

      // validation and normalization of the schema
      preSchemaUpdate(sft, previousSft)

      // if all is well, update the metadata - first back it up
      if (FastConverter.convertOrElse[java.lang.Boolean](sft.getUserData.get(Configs.UpdateBackupMetadata), true)) {
        metadata.backup(typeName.getLocalPart)
      }

      // rename the old rows if the type name has changed
      if (typeName.getLocalPart != schema.getTypeName) {
        metadata.scan(typeName.getLocalPart, "", cache = false).foreach { case (k, v) =>
          metadata.insert(sft.getTypeName, k, v)
          metadata.remove(typeName.getLocalPart, k)
        }
      }
      // now insert the new spec string
      metadata.insert(sft.getTypeName, AttributesKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))

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
   * Validate a call to updateSchema, throwing errors on failed validation
   *
   * @param existing existing schema
   * @param schema updated sft
   */
  protected def validateSchemaUpdate(existing: SimpleFeatureType, schema: SimpleFeatureType): Unit = {
    // validate that default geometry and date have not changed (rename is ok)
    if (schema.getGeomIndex != existing.getGeomIndex) {
      throw new UnsupportedOperationException("Changing the default geometry attribute is not supported")
    } else if (schema.getDtgIndex != existing.getDtgIndex) {
      throw new UnsupportedOperationException("Changing the default date attribute is not supported")
    }

    // check that unmodifiable user data has not changed
    MetadataBackedDataStore.UnmodifiableUserDataKeys.foreach { key =>
      if (schema.userData[Any](key) != existing.userData[Any](key)) {
        throw new UnsupportedOperationException(s"Updating '$key' is not supported")
      }
    }

    // validate that attributes weren't removed
    if (existing.getAttributeCount > schema.getAttributeCount) {
      throw new UnsupportedOperationException("Removing attributes from the schema is not supported")
    }

    // check for column type changes
    existing.getAttributeDescriptors.asScala.zipWithIndex.foreach { case (prev, i) =>
      val binding = schema.getDescriptor(i).getType.getBinding
      if (!binding.isAssignableFrom(prev.getType.getBinding)) {
        throw new UnsupportedOperationException(
          s"Incompatible schema column type change: ${schema.getDescriptor(i).getLocalName} " +
              s"from ${prev.getType.getBinding.getName} to ${binding.getName}")
      }
    }

    // check for reserved words - only check for new/renamed attributes
    val reserved = schema.getAttributeDescriptors.asScala.map(_.getLocalName).exists { name =>
      existing.getDescriptor(name) == null && FeatureUtils.ReservedWords.contains(name.toUpperCase(Locale.US))
    }
    if (reserved) {
      ReservedWordCheck.validateAttributeNames(schema)
    }
  }

  /**
    * Acquires a distributed lock for all data stores sharing this catalog table.
    * Make sure that you 'release' the lock in a finally block.
    */
  protected [geomesa] def acquireCatalogLock(): Releasable = {
    import org.locationtech.geomesa.index.DistributedLockTimeout
    val dsTypeName = getClass.getSimpleName.replaceAll("[^A-Za-z]", "")
    val path = s"/org.locationtech.geomesa/ds/$dsTypeName/${config.catalog}"
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

  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs._

  private val UnmodifiableUserDataKeys =
    Set(
      TableSharing,
      TableSharingPrefix,
      IndexVisibilityLevel,
      IndexZ3Interval,
      S3_INTERVAL_KEY,
      IndexXzPrecision,
      IndexZShards,
      IndexIdShards,
      IndexAttributeShards
    )
}
