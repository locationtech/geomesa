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

package org.locationtech.geomesa.feature.kryo

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.AbstractFeatureFactoryImpl
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.utils.text.{ObjectPoolFactory, ObjectPoolUtils}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.ref.SoftReference

class KryoSimpleFeatureFactory extends AbstractFeatureFactoryImpl {

  override def createSimpleFeature(attrs: Array[AnyRef], sft: SimpleFeatureType, id: String) = {
    val sf = new KryoSimpleFeature(id, sft)
    sf.setAttributes(attrs)
    sf
  }

  override def createSimpleFeautre(attrs: Array[AnyRef], descriptor: AttributeDescriptor, id: String) =
    createSimpleFeature(attrs, descriptor.asInstanceOf[SimpleFeatureType], id)

}

object KryoSimpleFeatureFactory {

  import scala.collection.mutable.Map

  private val hints = new Hints(Hints.FEATURE_FACTORY, classOf[KryoSimpleFeatureFactory])
  private val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)

  private val cache = new ThreadLocal[Map[SimpleFeatureType, SoftReference[SimpleFeatureBuilder]]] {
    override def initialValue = Map.empty
  }

  private def getFeatureBuilder(sft: SimpleFeatureType) =
    cache.get.get(sft).flatMap(_.get) match {
      case Some(builder) => builder
      case None =>
        val builder = new SimpleFeatureBuilder(sft, featureFactory)
        cache.get.put(sft, new SoftReference(builder))
        builder
    }

  def init() = Hints.putSystemDefault(Hints.FEATURE_FACTORY, classOf[KryoSimpleFeatureFactory])

  def buildAvroFeature(sft: SimpleFeatureType, attrs: Seq[AnyRef], id: String) = {
    val builder = getFeatureBuilder(sft)
    builder.addAll(attrs)
    builder.buildFeature(id)
  }

  def featureBuilder(sft: SimpleFeatureType): SimpleFeatureBuilder =
    new SimpleFeatureBuilder(sft, featureFactory)
}
