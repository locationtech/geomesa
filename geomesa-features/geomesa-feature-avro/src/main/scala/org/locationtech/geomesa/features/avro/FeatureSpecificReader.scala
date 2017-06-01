/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
import org.locationtech.geomesa.features.avro.serde.{ASFDeserializer, Version1Deserializer, Version2Deserializer}
import org.locationtech.geomesa.features.avro.serialization.AvroUserDataSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class FeatureSpecificReader(var oldType: SimpleFeatureType, var newType: SimpleFeatureType,
                            opts: SerializationOptions = SerializationOptions.none)
  extends DatumReader[AvroSimpleFeature] {

  def this(sft: SimpleFeatureType) = this(sft, sft)

  // DataFileStreams or DataFileReaders may set this after construction of the object
  // so all instance variables need to be be lazy
  protected var oldSchema = Option(oldType).map(generateSchema(_, opts.withUserData)).orNull

  protected var nameEncoder: FieldNameEncoder = null

  protected lazy val fieldsDesired = DataUtilities.attributeNames(newType).map(nameEncoder.decode)

  protected lazy val dataFields = oldSchema.getFields.filter { isDataField }

  lazy val typeMap: Map[String, Class[_]] =
    oldType.getAttributeDescriptors.map { ad => nameEncoder.encode(ad.getLocalName) -> ad.getType.getBinding }.toMap

  lazy val nillableAttrs: Set[String] = oldType.getAttributeDescriptors.filter(_.isNillable).map {
    ad => nameEncoder.encode(ad.getLocalName)
  }.toSet

  protected def isDataField(f: Schema.Field) =
    !f.name.equals(FEATURE_ID_AVRO_FIELD_NAME) &&
      !f.name.equals(AVRO_SIMPLE_FEATURE_VERSION) &&
      !f.name.equals(AVRO_SIMPLE_FEATURE_USERDATA)

  override def setSchema(schema: Schema): Unit = oldSchema = schema

  def setTypes(oldType: SimpleFeatureType, newType: SimpleFeatureType): Unit = {
    this.oldType = oldType
    this.newType = newType
    oldSchema = generateSchema(oldType, opts.withUserData)
  }

  protected def buildFieldReaders(deserializer: ASFDeserializer) =
    oldType.getAttributeDescriptors.map { ad =>
      val name = nameEncoder.encode(ad.getLocalName)
      buildSetOrConsume(name, typeMap(name), deserializer)
    }

  protected def buildSetOrConsume(name: String, cls: Class[_], deserializer: ASFDeserializer) = {
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

  protected def buildSet(clazz: Class[_], name: String, deserializer: ASFDeserializer): (AvroSimpleFeature, Decoder) => Unit = {
    val decoded = nameEncoder.decode(name)
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
      case cls if classOf[Array[Byte]].isAssignableFrom(cls)         => deserializer.setBytes(_, decoded, _)
    }
  }

  protected def buildConsume(clazz: Class[_], name: String, deserializer: ASFDeserializer) = {
    val f = deserializer.buildConsumeFunction(clazz)
    (sf: SimpleFeature, in: Decoder) => f(in)
  }

  protected lazy val v1fieldreaders = buildFieldReaders(Version1Deserializer)
  protected lazy val v2fieldreaders = buildFieldReaders(Version2Deserializer)

  protected def defaultRead(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = {
    val serializationVersion = in.readInt()
    readAttributes(in, serializationVersion)
  }

  protected def readWithUserData(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = {
    val serializationVersion = in.readInt()

    FeatureSpecificReader.checkVersion(serializationVersion)
    val sf = readAttributes(in, serializationVersion)

    val userData = AvroUserDataSerialization.deserialize(in)
    sf.getUserData.putAll(userData)

    sf
  }

  protected def readAttributes(in: Decoder, serializationVersion: Int): AvroSimpleFeature = {

    nameEncoder = new FieldNameEncoder(serializationVersion)

    // choose the proper deserializer
    val deserializer = serializationVersion match {
      case 1                              => v1fieldreaders
      case _ if serializationVersion >= 2 => v2fieldreaders
    }

    // Read the id
    val id = new FeatureIdImpl(in.readString())

    // Followed by the data fields
    val sf = new AvroSimpleFeature(id, newType)
    deserializer.foreach { f => f(sf, in) }
    sf
  }

  private lazy val reader: (AvroSimpleFeature, Decoder) => AvroSimpleFeature =
    if (opts.withUserData)
      readWithUserData
    else
      defaultRead

  override def read(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = reader(reuse, in)
}

object FeatureSpecificReader {

  // first field is serialization version, 2nd field is ID of simple feature
  def extractId(is: InputStream, reuse: BinaryDecoder = null): String = {
    val decoder = DecoderFactory.get().directBinaryDecoder(is, reuse)
    decoder.readInt()
    decoder.readString()
  }

  def checkVersion(version: Int) = {
    if (version > VERSION) throw new IllegalArgumentException(s"AvroSimpleFeature version $version is unsupported. " +
      s"You may need to upgrade to a new version")
  }

}