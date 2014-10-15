/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.data

import org.apache.accumulo.core.client.{BatchWriterConfig, Connector}
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.data.AccumuloBackedMetadata._
import org.locationtech.geomesa.core.security.AuthorizationsProvider
import org.locationtech.geomesa.core.util.SelfClosingIterator

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * GeoMesa Metadata/Catalog abstraction using key/value String pairs storing
 * them on a per-featurename basis
 */
trait GeoMesaMetadata {
  def delete(featureName: String, numThreads: Int)
  
  def insert(featureName: String, key: String, value: String)
  def insert(featureName: String, kvPairs: Map[String, String])
  def insert(featureName: String, key: String, value: String, vis: String)
  
  def read(featureName: String, key: String): Option[String]
  def readRequired(featureName: String, key: String): String
  def readRequiredNoCache(featureName: String, key: String): Option[String]
  
  def expireCache(featureName: String)

  def getFeatureTypes: Array[String]
}

class AccumuloBackedMetadata(connector: Connector,
                             catalogTable: String,
                             writeVisibilities: String,
                             authorizationsProvider: AuthorizationsProvider) extends GeoMesaMetadata {

  private val metaDataCache = new mutable.HashMap[(String, String), Option[String]]()
    with mutable.SynchronizedMap[(String, String), Option[String]]

  // TODO memory should be configurable
  private val metadataBWConfig =
    new BatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(1)

  /**
   * Handles creating a mutation for writing metadata
   *
   * @param featureName
   * @return
   */
  private def getMetadataMutation(featureName: String) = new Mutation(getMetadataRowKey(featureName))

  /**
   * Handles encoding metadata into a mutation.
   *
   * @param featureName
   * @param mutation
   * @param key
   * @param value
   */
  private def putMetadata(featureName: String,
                          mutation: Mutation,
                          key: String,
                          value: String) {
    mutation.put(new Text(key), EMPTY_COLQ, new Value(value.getBytes))
    // also pre-fetch into the cache
    if (!value.isEmpty) {
      metaDataCache.put((featureName, key), Some(value))
    }
  }

  /**
   * Handles writing mutations
   *
   * @param mutations
   */
  private def writeMutations(mutations: Mutation*): Unit = {
    val writer = connector.createBatchWriter(catalogTable, metadataBWConfig)
    for (mutation <- mutations) {
      writer.addMutation(mutation)
    }
    writer.flush()
    writer.close()
  }

  /**
   * Handles deleting metadata from the catalog by using the Range obtained from the METADATA_TAG and featureName
   * and setting that as the Range to be handled and deleted by Accumulo's BatchDeleter
   *
   * @param featureName the name of the table to query and delete from
   * @param numThreads the number of concurrent threads to spawn for querying
   */
  override def delete(featureName: String, numThreads: Int): Unit = {
    val range = new Range(getMetadataRowKey(featureName))
    val deleter = connector.createBatchDeleter(catalogTable,
                                               authorizationsProvider.getAuthorizations,
                                               numThreads,
                                               metadataBWConfig)
    deleter.setRanges(List(range))
    deleter.delete()
    deleter.close()
  }

  /**
   * Creates the row id for a metadata entry
   *
   * @param featureName
   * @return
   */
  private def getMetadataRowKey(featureName: String) = new Text(METADATA_TAG + "_" + featureName)

  /**
   * Reads metadata from cache or scans if not available
   *
   * @param featureName
   * @param key
   * @return
   */
  override def read(featureName: String, key: String): Option[String] =
    metaDataCache.getOrElseUpdate((featureName, key), readRequiredNoCache(featureName, key))

  override def readRequired(featureName: String, key: String): String =
    read(featureName, key)
      .getOrElse(throw new RuntimeException(s"Unable to find required metadata property for key $key"))

  /**
   * Gets metadata by scanning the table, without the local cache
   *
   * Read metadata using scheme:  ~METADATA_featureName metadataFieldName: insertionTimestamp metadataValue
   *
   * @param featureName
   * @param key
   * @return
   */
  override def readRequiredNoCache(featureName: String, key: String): Option[String] = {
    val scanner = createCatalogScanner
    scanner.setRange(new Range(getMetadataRowKey(featureName)))
    scanner.fetchColumn(new Text(key), EMPTY_COLQ)

    SelfClosingIterator(scanner).map(_.getValue.toString).toList.headOption
  }


  /**
   * Create an Accumulo Scanner to the Catalog table to query Metadata for this store
   */
  private def createCatalogScanner = connector.createScanner(catalogTable, authorizationsProvider.getAuthorizations)

  override def expireCache(featureName: String) =
    metaDataCache.keys
      .filter { case (fn, cf) => fn == featureName }
      .foreach(metaDataCache.remove)

  override def insert(featureName: String, key: String, value: String) =
    insert(featureName, Map(key -> value))

  override def insert(featureName: String, kvPairs: Map[String, String]) = {
    val mutation = getMetadataMutation(featureName)
    kvPairs.foreach { case (k,v) =>
      putMetadata(featureName, mutation, k, v)
    }
    writeMutations(mutation)
  }

  override def insert(featureName: String, key: String, value: String, vis: String) = {
    val mutation = getMetadataMutation(featureName)
    mutation.put(new Text(key), EMPTY_COLQ, new ColumnVisibility(vis), new Value(vis.getBytes))
    writeMutations(mutation)
  }

  /**
   * Scans metadata rows and pulls out the different feature types in the table
   *
   * @return
   */
  override def getFeatureTypes: Array[String] = {
    val scanner = createCatalogScanner
    scanner.setRange(new Range(METADATA_TAG, METADATA_TAG_END))
    // restrict to just schema cf so we only get 1 hit per feature
    scanner.fetchColumnFamily(new Text(SCHEMA_KEY))
    val resultItr = new Iterator[String] {
      val src = scanner.iterator()

      def hasNext = {
        val next = src.hasNext
        if (!next) {
          scanner.close()
        }
        next
      }

      def next() = src.next().getKey.getRow.toString
    }
    resultItr.toArray.map(getFeatureNameFromMetadataRowKey)
  }

  /**
   * Reads the feature name from a given metadata row key
   *
   * @param rowKey
   * @return
   */
  private def getFeatureNameFromMetadataRowKey(rowKey: String): String = {
    val MetadataRowKeyRegex(featureName) = rowKey
    featureName
  }

}

object AccumuloBackedMetadata {
  val MetadataRowKeyRegex = (METADATA_TAG + """_(.*)""").r
}
