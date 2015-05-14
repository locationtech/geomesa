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

package org.locationtech.geomesa.core.data.mapreduce

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper => HMapper}
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.utils.geotools.FeatureHandler
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object FeatureIngestMapper
       extends Logging {

  type Mapper = HMapper[LongWritable,Text,Key,Value]

  import org.locationtech.geomesa.core._

  class FeatureIngestMapper extends Mapper {
    var featureType: SimpleFeatureType = null
    var fw: FeatureWriter[SimpleFeatureType, SimpleFeature] = null

    override def setup(context: Mapper#Context) {
      super.setup(context)

      val featureName = context.getConfiguration.get(DEFAULT_FEATURE_NAME)
      val ds = DataStoreFinder.getDataStore(
        AccumuloDataStoreFactory.getMRAccumuloConnectionParams(
          context.getConfiguration)).asInstanceOf[AccumuloDataStore]

      featureType = ds.getSchema(featureName)
      fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    }

    override def map(key: LongWritable, value: Text, context: Mapper#Context) {
      val geom::encoded = value.toString.split(FeatureHandler.OUTPUT_FIELD_SEPARATOR_CHAR).toList

      try {
        // decode the simple-feature
        // ("mkString" is here to guard against strings broken by our separator)
        val simpleFeature = DataUtilities.createFeature(featureType,
          encoded.mkString(FeatureHandler.OUTPUT_FIELD_SEPARATOR))
        simpleFeature.setDefaultGeometry(WKBUtils.read(Base64.decode(geom)))
        simpleFeature.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(x = true))

        val next = fw.next()
        // "Clone" the simpleFeature to write into the featureWriter's "next" and then write it to Accumulo.
        next.setAttributes(simpleFeature.getAttributes)
        next.getIdentifier.asInstanceOf[FeatureIdImpl].setID(simpleFeature.getID)

        fw.write()
      } catch {
        case e : Exception => logger.warn("[WARNING] Problem writing feature; skipping it.", e)
      }
    }
  }
}