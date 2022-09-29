/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams.serde

import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.kafka.streams.{GeoMesaMessage, MessageAction}
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Serializer for GeoMesaMessages
 *
 * @param sft feature type
 * @param internal nested serializer
 */
class GeoMesaMessageSerializer(val sft: SimpleFeatureType, val internal: SimpleFeatureSerializer) {

  import scala.collection.JavaConverters._

  private val converters: Array[AnyRef => AnyRef] =
    sft.getAttributeDescriptors.toArray(Array.empty[AttributeDescriptor]).map { d =>
      val binding = d.getType.getBinding.asInstanceOf[Class[_ <: AnyRef]]
      (in: AnyRef) => FastConverter.convert(in, binding)
    }

  def serialize(data: GeoMesaMessage): Array[Byte] = {
    data.action match {
      case MessageAction.Upsert => internal.serialize(wrap(data))
      case MessageAction.Delete => null
      case null => throw new NullPointerException("action is null")
      case _ => throw new NotImplementedError(s"No serialization implemented for action '${data.action}'")
    }
  }

  def deserialize(data: Array[Byte]): GeoMesaMessage = {
    if (data == null || data.isEmpty) { GeoMesaMessage.delete() } else {
      val feature = internal.deserialize(data)
      val userData = if (feature.getUserData.isEmpty) { Map.empty[String, String] } else {
        val builder = Map.newBuilder[String, String]
        feature.getUserData.asScala.foreach {
          case (k: String, v: String) => builder += k -> v
          case (k, v) => builder += k.toString -> v.toString
        }
        builder.result
      }
      GeoMesaMessage.upsert(feature.getAttributes.asScala.toSeq, userData)
    }
  }

  /**
   * Wrap a message as a simple feature
   *
   * @param message message
   * @return
   */
  def wrap(message: GeoMesaMessage): SimpleFeature =
    new SerializableFeature(converters, message.attributes.toIndexedSeq, message.userData)
}
