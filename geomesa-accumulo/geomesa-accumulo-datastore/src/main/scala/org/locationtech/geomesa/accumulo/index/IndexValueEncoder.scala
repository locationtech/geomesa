/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.concurrent.ConcurrentHashMap

import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.{SerializationOption, SerializationOptions}
import org.locationtech.geomesa.features.SimpleFeatureSerializer.LimitedSerialization
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureSerializer}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.sft.ImmutableSimpleFeatureType
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Serializer for attribute join indices
  */
object IndexValueEncoder {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConversions._

  private val cache = new ConcurrentHashMap[ImmutableSimpleFeatureType, ImmutableSimpleFeatureType]()

  def apply(sft: SimpleFeatureType): SimpleFeatureSerializer =
    new ProjectingKryoFeatureSerializer(sft, getIndexSft(sft), SerializationOptions.withoutId)

  /**
   * Gets a feature type compatible with the stored index value
   *
   * @param sft simple feature type
   * @return
   */
  def getIndexSft(sft: SimpleFeatureType): SimpleFeatureType = {
    sft match {
      case immutable: ImmutableSimpleFeatureType =>
        var indexSft = cache.get(immutable)
        if (indexSft == null) {
          indexSft = SimpleFeatureTypes.immutable(buildIndexSft(sft)).asInstanceOf[ImmutableSimpleFeatureType]
          cache.put(immutable, indexSft)
        }
        indexSft

      case _ => buildIndexSft(sft)
    }
  }

  private def buildIndexSft(sft: SimpleFeatureType): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.setNamespaceURI(null: String)
    builder.setName(sft.getTypeName + "--index")
    builder.setAttributes(getIndexValueAttributes(sft))
    if (sft.getGeometryDescriptor != null) {
      builder.setDefaultGeometry(sft.getGeometryDescriptor.getLocalName)
    }
    builder.setCRS(sft.getCoordinateReferenceSystem)
    val indexSft = builder.buildFeatureType()
    indexSft.getUserData.putAll(sft.getUserData)
    indexSft
  }

  /**
   * Gets the attributes that are stored in the index value
   *
   * @param sft simple feature type
   * @return
   */
  private def getIndexValueAttributes(sft: SimpleFeatureType): Seq[AttributeDescriptor] = {
    val geom = sft.getGeometryDescriptor
    val dtg = sft.getDtgField
    val attributes = scala.collection.mutable.Buffer.empty[AttributeDescriptor]
    var i = 0
    while (i < sft.getAttributeCount) {
      val ad = sft.getDescriptor(i)
      if (ad == geom || dtg.contains(ad.getLocalName) || ad.isIndexValue()) {
        attributes.append(ad)
      }
      i += 1
    }
    attributes
  }

  /**
    * Encoder/decoder for index values. Allows customizable fields to be encoded. Not thread-safe.
    *
    * @param sft simple feature type
    */
  @deprecated
  class IndexValueEncoderImpl(sft: SimpleFeatureType) extends SimpleFeatureSerializer with LimitedSerialization {

    import scala.collection.JavaConverters._

    private val indexSft = getIndexSft(sft)
    private val encoder = KryoFeatureSerializer(indexSft)
    private val reusableFeature = new ScalaSimpleFeature(indexSft, "")
    private val indices = indexSft.getAttributeDescriptors.asScala.map(ad => sft.indexOf(ad.getLocalName)).toArray

    override val options: Set[SerializationOption] = Set.empty

    override def serialize(sf: SimpleFeature): Array[Byte] = {
      reusableFeature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
      var i = 0
      while (i < indices.length) {
        reusableFeature.setAttribute(i, sf.getAttribute(indices(i)))
        i += 1
      }
      encoder.serialize(reusableFeature)
    }

    override def deserialize(value: Array[Byte]): SimpleFeature = encoder.deserialize(value)
  }
}
