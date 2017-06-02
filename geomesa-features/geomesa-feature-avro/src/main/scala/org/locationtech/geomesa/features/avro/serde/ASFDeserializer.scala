/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.apache.avro.io.Decoder
import org.locationtech.geomesa.features.avro.{AvroSimpleFeatureUtils, AvroSimpleFeature}

/**
 * Trait that encapsulates the methods needed to deserialize
 * an AvroSimpleFeature
 */
trait ASFDeserializer {

  def setString(sf: AvroSimpleFeature, field: String, in:Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readString())

  def setInt(sf: AvroSimpleFeature, field: String, in:Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readInt().asInstanceOf[Object])

  def setDouble(sf: AvroSimpleFeature, field: String, in:Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readDouble().asInstanceOf[Object])

  def setLong(sf: AvroSimpleFeature, field: String, in:Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readLong().asInstanceOf[Object])

  def setFloat(sf: AvroSimpleFeature, field: String, in:Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readFloat().asInstanceOf[Object])

  def setBool(sf: AvroSimpleFeature, field: String, in:Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readBoolean().asInstanceOf[Object])

  def setUUID(sf: AvroSimpleFeature, field: String, in:Decoder): Unit = {
    val bb = in.readBytes(null)
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeUUID(bb))
  }

  def setDate(sf: AvroSimpleFeature, field: String, in:Decoder): Unit =
    sf.setAttributeNoConvert(field, new Date(in.readLong()))

  def setList(sf: AvroSimpleFeature, field: String, in:Decoder): Unit = {
    val bb = in.readBytes(null)
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeList(bb))
  }

  def setMap(sf: AvroSimpleFeature, field: String, in:Decoder): Unit = {
    val bb = in.readBytes(null)
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeMap(bb))
  }

  def setBytes(sf: AvroSimpleFeature, field: String, in:Decoder): Unit = {
    val bb = in.readBytes(null)
    val bytes = new Array[Byte](bb.remaining)
    bb.get(bytes)
    sf.setAttributeNoConvert(field, bytes)
  }

  def setGeometry(sf: AvroSimpleFeature, field: String, in:Decoder): Unit

  def consumeGeometry(in: Decoder): Unit

  def buildConsumeFunction(cls: Class[_]) = cls match {
    case c if classOf[java.lang.String].isAssignableFrom(cls)    => (in: Decoder) => in.skipString()
    case c if classOf[java.lang.Integer].isAssignableFrom(cls)   => (in: Decoder) => in.readInt()
    case c if classOf[java.lang.Long].isAssignableFrom(cls)      => (in: Decoder) => in.readLong()
    case c if classOf[java.lang.Double].isAssignableFrom(cls)    => (in: Decoder) => in.readDouble()
    case c if classOf[java.lang.Float].isAssignableFrom(cls)     => (in: Decoder) => in.readFloat()
    case c if classOf[java.lang.Boolean].isAssignableFrom(cls)   => (in: Decoder) => in.readBoolean()
    case c if classOf[UUID].isAssignableFrom(cls)                => (in: Decoder) => in.skipBytes()
    case c if classOf[Date].isAssignableFrom(cls)                => (in: Decoder) => in.readLong()
    case c if classOf[Geometry].isAssignableFrom(cls)            => (in: Decoder) => consumeGeometry(in)
    case c if classOf[java.util.List[_]].isAssignableFrom(cls)   => (in: Decoder) => in.skipBytes()
    case c if classOf[java.util.Map[_, _]].isAssignableFrom(cls) => (in: Decoder) => in.skipBytes()
    case c if classOf[Array[Byte]].isAssignableFrom(cls)         => (in: Decoder) => in.skipBytes()
  }

}
