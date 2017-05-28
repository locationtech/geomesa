package org.locationtech.geomesa.fs

import org.apache.hadoop.fs.FileSystem
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Created by anthony on 5/28/17.
  */
class FileSystemFeatureStore(entry: ContentEntry,
                             query: Query,
                             partitionScheme: PartitionScheme,
                             fs: FileSystem,
                             fileSystemStorage: FileSystemStorage) extends ContentFeatureStore(entry, query) {
  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = ???
  override def getBoundsInternal(query: Query): ReferencedEnvelope = ReferencedEnvelope.EVERYTHING
  override def buildFeatureType(): SimpleFeatureType = fileSystemStorage.getSimpleFeatureType
  override def getCountInternal(query: Query): Int = ???
  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] =
    new DelegateSimpleFeatureReader(fileSystemStorage.getSimpleFeatureType,
      new DelegateSimpleFeatureIterator(fileSystemStorage.query(query.getFilter)))

  override def canLimit: Boolean = false
  override def canTransact: Boolean = false
  override def canEvent: Boolean = false
  override def canReproject: Boolean = false
  override def canRetype: Boolean = true
  override def canSort: Boolean = true
  override def canFilter: Boolean = true

}
