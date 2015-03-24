/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.index

import com.twitter.scalding._
import org.apache.accumulo.core.data.{Key, Range => AcRange, Value}
import org.apache.hadoop.conf.Configuration
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.feature.{SimpleFeatureDecoder, SimpleFeatureEncoder}
import org.locationtech.geomesa.jobs.scalding._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.`type`.AttributeDescriptor

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

class AttributeIndexJob(args: Args) extends GeoMesaBaseJob(args) {

  val (recordTable, attributeTable) = {
    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    (ds.getRecordTableForType(feature), ds.getAttrIdxTableName(feature))
  }
  val attributes = args.list(AttributeIndexJob.ATTRIBUTES_TO_INDEX)
  val coverage = args.optional(AttributeIndexJob.INDEX_COVERAGE)
      .flatMap(c => Try(IndexCoverage.withName(c)).toOption)
      .getOrElse(IndexCoverage.JOIN)

  override lazy val input  = AccumuloInputOptions(recordTable)
  override lazy val output = AccumuloOutputOptions(attributeTable)

  // scalding job
  AccumuloSource(options)
    .using(new AttributeIndexResources)
    .flatMap(('key, 'value) -> 'mutation) {
      (r: AttributeIndexResources, kv: (Key, Value)) => getMutations(kv._2, r)
    }.write(AccumuloSource(options))

  def getMutations(value: Value, r: AttributeIndexResources) = {
    val feature = r.decoder.decode(value.get())
    AttributeTable.getAttributeIndexMutations(feature, r.ive, r.fe, r.attrs, r.visibilities, r.prefix)
  }

  override def afterJobTasks() = {
    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    // schedule a table compaction to clean up the table
    ds.connector.tableOperations().compact(attributeTable, null, null, true, false)
    // update the metadata
    val sft = ds.getSchema(feature)
    def wasIndexed(ad: AttributeDescriptor) = attributes.contains(ad.getLocalName)
    sft.getAttributeDescriptors.filter(wasIndexed).foreach(_.setIndexCoverage(coverage))
    val updatedSpec = SimpleFeatureTypes.encodeType(sft)
    ds.updateIndexedAttributes(feature, updatedSpec)
  }

  class AttributeIndexResources extends GeoMesaResources {
    val prefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(sft)
    val encoding = ds.getFeatureEncoding(sft)
    val fe = SimpleFeatureEncoder(sft, encoding)
    val ive = IndexValueEncoder(sft, ds.getGeomesaVersion(sft))
    val decoder = SimpleFeatureDecoder(sft, encoding)

    // the attributes we want to index
    val attrs = sft.getAttributeDescriptors.zipWithIndex
        .filter { case (ad, idx) => attributes.contains(ad.getLocalName) }
    attrs.foreach { case (ad, idx) => ad.setIndexCoverage(coverage) }
  }
}

object AttributeIndexJob {

  val ATTRIBUTES_TO_INDEX = "geomesa.index.attributes"
  val INDEX_COVERAGE      = "geomesa.index.coverage"

  def runJob(conf: Configuration, params: Map[String, String], feature: String) = {
    val args = buildArgs(params)
    val instantiateJob = (args: Args) => new AttributeIndexJob(args)
    GeoMesaBaseJob.runJob(conf, params, feature, args, instantiateJob)
  }

  def buildArgs(params: Map[String, String]) = {
    val attributes = params.get(ATTRIBUTES_TO_INDEX).map(_.split(",").toList).getOrElse {
      throw new IllegalArgumentException(s"$ATTRIBUTES_TO_INDEX is a required argument")
    }
    params.get(INDEX_COVERAGE) match {
      case None           => Map(ATTRIBUTES_TO_INDEX -> attributes)
      case Some(coverage) => Map(ATTRIBUTES_TO_INDEX -> attributes, INDEX_COVERAGE -> List(coverage))
    }
  }
}
