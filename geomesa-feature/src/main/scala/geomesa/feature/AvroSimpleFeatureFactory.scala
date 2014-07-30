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

package geomesa.feature

import com.google.common.cache.{CacheLoader, CacheBuilder}
import geomesa.utils.text.{ObjectPoolFactory, ObjectPoolUtils}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.AbstractFeatureFactoryImpl
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class AvroSimpleFeatureFactory extends AbstractFeatureFactoryImpl {

  override def createSimpleFeature(attrs: Array[AnyRef],
                                   sft: SimpleFeatureType,
                                   id: String): SimpleFeature = {
    val f = new AvroSimpleFeature(new FeatureIdImpl(id), sft)
    f.setAttributes(attrs)
    f
  }

  override def createSimpleFeautre(attrs: Array[AnyRef],
                                   descriptor: AttributeDescriptor,
                                   id: String): SimpleFeature =
    createSimpleFeature(attrs, descriptor.asInstanceOf[SimpleFeatureType], id)

}

object AvroSimpleFeatureFactory {
  def init = {
    Hints.putSystemDefault(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  }

  private val builderCache =
    CacheBuilder
      .newBuilder()
      .build(
        new CacheLoader[SimpleFeatureType, ObjectPoolUtils[SimpleFeatureBuilder]] {
          override def load(sft: SimpleFeatureType): ObjectPoolUtils[SimpleFeatureBuilder] =
            ObjectPoolFactory(new SimpleFeatureBuilder(sft, featureFactory), size = 1024)
        }
      )

  private val hints = new Hints(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  private val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)

  def buildAvroFeature(sft: SimpleFeatureType, attrs: Seq[AnyRef], id: String) =
    builderCache(sft).withResource { builder =>
      builder.addAll(attrs)
      builder.buildFeature(id)
    }

  def featureBuilder(sft: SimpleFeatureType): SimpleFeatureBuilder = new SimpleFeatureBuilder(sft, featureFactory)
}
