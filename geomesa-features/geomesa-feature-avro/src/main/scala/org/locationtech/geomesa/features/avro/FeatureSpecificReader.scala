/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.avro

import java.io.InputStream
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.apache.avro.Schema
import org.apache.avro.io._
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils._
import org.locationtech.geomesa.features.avro.serde.{Version2Deserializer, Version1Deserializer, ASFDeserializer}
import org.locationtech.geomesa.features.avro.serialization.AvroSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class FeatureSpecificReader(oldType: SimpleFeatureType, newType: SimpleFeatureType,
                            opts: SerializationOptions = SerializationOptions.none)
  extends DatumReader[AvroSimpleFeature] {

  def this(sft: SimpleFeatureType) = this(sft, sft)

  var oldSchema = generateSchema(oldType)
  val fieldsDesired = DataUtilities.attributeNames(newType).map(encodeAttributeName)

  def isDataField(f: Schema.Field) =
    !f.name.equals(FEATURE_ID_AVRO_FIELD_NAME) && !f.name.equals(AVRO_SIMPLE_FEATURE_VERSION)

  override def setSchema(schema: Schema): Unit = oldSchema = schema

  val dataFields = oldSchema.getFields.filter { isDataField }

  val typeMap: Map[String, Class[_]] =
    oldType.getAttributeDescriptors.map { ad => encodeAttributeName(ad.getLocalName) -> ad.getType.getBinding }.toMap

  val nillableAttrs: Set[String] = oldType.getAttributeDescriptors.filter(_.isNillable).map {
      ad => encodeAttributeName(ad.getLocalName)
    }.toSet

  def buildFieldReaders(deserializer: ASFDeserializer) =
    oldType.getAttributeDescriptors.map { ad =>
      val name = encodeAttributeName(ad.getLocalName)
      buildSetOrConsume(name, typeMap(name), deserializer) }

  def buildSetOrConsume(name: String, cls: Class[_], deserializer: ASFDeserializer) = {
    val f =
      if (!fieldsDesired.contains(name)) buildConsume(cls, name, deserializer)
      else buildSet(cls, name, deserializer)

    if (nillableAttrs.contains(name))
      (sf: AvroSimpleFeature, in: Decoder) =>
        if (in.readIndex() == 1) in.readNull()
        else f(sf, in)
    else
      f
  }
          
  def buildSet(clazz: Class[_], name: String, deserializer: ASFDeserializer): (AvroSimpleFeature, Decoder) => Unit = {
    val decoded = decodeAttributeName(name)
    clazz match {
      case cls if classOf[java.lang.String].isAssignableFrom(cls)    => deserializer.setString(_, decoded, _)
      case cls if classOf[java.lang.Integer].isAssignableFrom(cls)   => deserializer.setInt(_, decoded, _)
      case cls if classOf[java.lang.Long].isAssignableFrom(cls)      => deserializer.setLong(_, decoded, _)
      case cls if classOf[java.lang.Double].isAssignableFrom(cls)    => deserializer.setDouble(_, decoded, _)
      case cls if classOf[java.lang.Float].isAssignableFrom(cls)     => deserializer.setFloat(_, decoded, _)
      case cls if classOf[java.lang.Boolean].isAssignableFrom(cls)   => deserializer.setBool(_, decoded, _)
      case cls if classOf[UUID].isAssignableFrom(cls)                => deserializer.setUUID(_, decoded, _)
      case cls if classOf[Date].isAssignableFrom(cls)                => deserializer.setDate(_, decoded, _)
      case cls if classOf[Geometry].isAssignableFrom(cls)            => deserializer.setGeometry(_, decoded, _)
      case cls if classOf[java.util.List[_]].isAssignableFrom(cls)   => deserializer.setList(_, decoded, _)
      case cls if classOf[java.util.Map[_, _]].isAssignableFrom(cls) => deserializer.setMap(_, decoded, _)
    }
  }

  def buildConsume(clazz: Class[_], name: String, deserializer: ASFDeserializer) = {
    val f = deserializer.buildConsumeFunction(clazz)
    (sf: SimpleFeature, in: Decoder) => f(in)
  }

  lazy val v1fieldreaders = buildFieldReaders(Version1Deserializer)
  lazy val v2fieldreaders = buildFieldReaders(Version2Deserializer)

  def defaultRead(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = {
    val serializationVersion = in.readInt()
    readAttributes(in, serializationVersion)
  }

  def readWithUserData(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = {
    val serializationVersion = in.readInt()
    val sf = readAttributes(in, serializationVersion)

    val ar = AvroSerialization.reader

    val userData = ar.readGenericMap(serializationVersion)(in)
    sf.getUserData.putAll(userData)

    sf
  }

  def readAttributes(in: Decoder, serializationVersion: Int): AvroSimpleFeature = {

    // choose the proper deserializer
    val deserializer = serializationVersion match {
      case 1 => v1fieldreaders
      case 2 => v2fieldreaders
    }

    // Read the id
    val id = new FeatureIdImpl(in.readString())

    // Followed by the data fields
    val sf = new AvroSimpleFeature(id, newType)
    deserializer.foreach { f => f(sf, in) }
    sf
  }

  private val reader: (AvroSimpleFeature, Decoder) => AvroSimpleFeature =
    if (opts.withUserData)
      readWithUserData
    else
      defaultRead

  override def read(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = reader(reuse, in)
}

object FeatureSpecificReader {

  // use when you want the entire feature back, not a subset
  def apply(sftType: SimpleFeatureType) = new FeatureSpecificReader(sftType)

  // first field is serialization version, 2nd field is ID of simple feature
  def extractId(is: InputStream, reuse: BinaryDecoder = null): String = {
    val decoder = DecoderFactory.get().directBinaryDecoder(is, reuse)
    decoder.readInt()
    decoder.readString()
  }

}
