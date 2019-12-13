/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import java.io.OutputStream
import java.nio._
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID, Collection => JCollection, List => JList}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.google.common.collect.Maps
import org.locationtech.jts.geom.Geometry
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.commons.codec.binary.Hex
import org.geotools.data.DataUtilities
import org.geotools.feature.`type`.{AttributeDescriptorImpl, Types}
import org.geotools.feature.{AttributeImpl, GeometryAttributeImpl}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.Converters
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

import scala.collection.JavaConversions._
import scala.util.Try

class Version1ASF(id: FeatureId, sft: SimpleFeatureType) extends SimpleFeature {
  import Version1ASF._

  val values    = Array.ofDim[AnyRef](sft.getAttributeCount)
  val userData  = collection.mutable.HashMap.empty[AnyRef, AnyRef]
  val typeMap   = typeMapCache.get(sft)
  val names     = nameCache.get(sft)
  val nameIndex = nameIndexCache.get(sft)
  val schema    = avroSchemaCache.get(sft)

  def write(datumWriter: GenericDatumWriter[GenericRecord], encoder: BinaryEncoder){
    val record = new GenericData.Record(schema)
    record.put(Version1ASF.AVRO_SIMPLE_FEATURE_VERSION, Version1ASF.VERSION)
    record.put(Version1ASF.FEATURE_ID_AVRO_FIELD_NAME, getID)

    val converted = values.zipWithIndex.map {
      case t@(null, _) => t
      case (v, idx)    => (convertValue(idx, v), idx)
    }

    converted.foreach { case (x, idx) => record.put(names(idx), x) }
    datumWriter.write(record, encoder)
    encoder.flush()
  }

  def convertValue(idx: Int, v: AnyRef) = typeMap(names(idx)).conv.apply(v)

  val gdw = new GenericDatumWriter[GenericRecord](schema)
  var encoder: BinaryEncoder = null
  def write(os: OutputStream) {
    encoder = EncoderFactory.get.binaryEncoder(os, null)
    write(gdw, encoder)
  }

  def getFeatureType = sft
  def getType = sft
  def getIdentifier = id
  def getID = id.getID

  def getAttribute(name: String) = nameIndex.get(name).map(getAttribute).getOrElse(null)
  def getAttribute(name: Name) = getAttribute(name.getLocalPart)
  def getAttribute(index: Int) = values(index)

  def setAttribute(name: String, value: Object) = setAttribute(nameIndex(name), value)
  def setAttribute(name: Name, value: Object) = setAttribute(name.getLocalPart, value)
  def setAttribute(index: Int, value: Object) = setAttributeNoConvert(index, Converters.convert(value, getFeatureType.getDescriptor(index).getType.getBinding).asInstanceOf[AnyRef])
  def setAttributes(vals: JList[Object]) = vals.zipWithIndex.foreach { case (v, idx) => setAttribute(idx, v) }
  def setAttributes(vals: Array[Object])= vals.zipWithIndex.foreach { case (v, idx) => setAttribute(idx, v) }

  def setAttributeNoConvert(index: Int, value: Object) = values(index) = value
  def setAttributeNoConvert(name: String, value: Object): Unit = setAttributeNoConvert(nameIndex(name), value)
  def setAttributeNoConvert(name: Name, value: Object): Unit = setAttributeNoConvert(name.getLocalPart, value)
  def setAttributesNoConvert(vals: JList[Object]) = vals.zipWithIndex.foreach { case (v, idx) => values(idx) = v }
  def setAttributesNoConvert(vals: Array[Object])= vals.zipWithIndex.foreach { case (v, idx) => values(idx) = v }

  def getAttributeCount = values.length
  def getAttributes: JList[Object] = values.toList
  def getDefaultGeometry: Object = Try(sft.getGeometryDescriptor.getName).map { getAttribute }.getOrElse(null)

  def setDefaultGeometry(geo: Object) = setAttribute(sft.getGeometryDescriptor.getName, geo)

  def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry =>
      new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _ =>
      new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  def getDefaultGeometryProperty: GeometryAttribute = {
    val geoDesc = sft.getGeometryDescriptor
    geoDesc != null match {
      case true =>
        new GeometryAttributeImpl(getDefaultGeometry, geoDesc, null)
      case false =>
        null
    }
  }

  def setDefaultGeometryProperty(geoAttr: GeometryAttribute) = geoAttr != null match {
    case true =>
      setDefaultGeometry(geoAttr.getValue)
    case false =>
      setDefaultGeometry(null)
  }

  def getProperties: JCollection[Property] =
    getAttributes.zip(sft.getAttributeDescriptors).map {
      case(attribute, attributeDescriptor) =>
        new AttributeImpl(attribute, attributeDescriptor, id)
    }
  def getProperties(name: Name): JCollection[Property] = getProperties(name.getLocalPart)
  def getProperties(name: String): JCollection[Property] = getProperties.filter(_.getName.toString == name)
  def getProperty(name: Name): Property = getProperty(name.getLocalPart)
  def getProperty(name: String): Property = new AttributeImpl(getAttribute(name), sft.getDescriptor(name), id)

  def getValue: JCollection[_ <: Property] = getProperties

  def setValue(values: JCollection[Property]) = values.zipWithIndex.foreach { case (p, idx) =>
    this.values(idx) = p.getValue}

  def getDescriptor: AttributeDescriptor = new AttributeDescriptorImpl(sft, sft.getName, 0, Int.MaxValue, true, null)

  def getName: Name = sft.getName

  def getUserData = userData

  def isNillable = true

  def setValue(newValue: Object) = setValue (newValue.asInstanceOf[JCollection[Property]])

  def validate() = values.zipWithIndex.foreach { case (v, idx) => Types.validate(getType.getDescriptor(idx), v) }
}

object Version1ASF {

  def apply(sf: SimpleFeature) = {
    val asf = new Version1ASF(sf.getIdentifier, sf.getFeatureType)
    for (i <- 0 until sf.getAttributeCount) asf.setAttribute(i, sf.getAttribute(i))

    asf
  }

  import scala.collection.JavaConversions._

  val primitiveTypes =
    List(
      classOf[String],
      classOf[java.lang.Integer],
      classOf[Int],
      classOf[java.lang.Long],
      classOf[Long],
      classOf[java.lang.Double],
      classOf[Double],
      classOf[java.lang.Float],
      classOf[Float],
      classOf[java.lang.Boolean],
      classOf[Boolean]
    )

  def loadingCacheBuilder[V <: AnyRef](f: SimpleFeatureType => V) =
    Caffeine
      .newBuilder
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(
        new CacheLoader[SimpleFeatureType, V] {
          def load(sft: SimpleFeatureType): V = f(sft)
        }
      )

  case class Binding(clazz: Class[_], conv: AnyRef => Any)
  val typeMapCache: LoadingCache[SimpleFeatureType, Map[String, Binding]] =
    loadingCacheBuilder { sft =>
      sft.getAttributeDescriptors.map { ad =>
        val conv =
          ad.getType.getBinding match {
            case t if primitiveTypes.contains(t) => (v: AnyRef) => v
            case t if classOf[UUID].isAssignableFrom(t) =>
              (v: AnyRef) => {
                val uuid = v.asInstanceOf[UUID]
                val bb = ByteBuffer.allocate(16)
                bb.putLong(uuid.getMostSignificantBits)
                bb.putLong(uuid.getLeastSignificantBits)
                bb.flip
                bb
              }

            case t if classOf[Date].isAssignableFrom(t) =>
              (v: AnyRef) => v.asInstanceOf[Date].getTime

            case t if classOf[Geometry].isAssignableFrom(t) =>
              (v: AnyRef) => v.asInstanceOf[Geometry].toText

            case _ =>
              (v: AnyRef) =>
                Option(Converters.convert(v, classOf[String])).getOrElse { a: AnyRef => a.toString }
          }

        (encodeAttributeName(ad.getLocalName), Binding(ad.getType.getBinding, conv))
      }.toMap
    }

  val avroSchemaCache: LoadingCache[SimpleFeatureType, Schema] =
    loadingCacheBuilder { sft => generateSchema(sft) }

  val nameCache: LoadingCache[SimpleFeatureType, Array[String]] =
    loadingCacheBuilder { sft => DataUtilities.attributeNames(sft).map(encodeAttributeName) }

  val nameIndexCache: LoadingCache[SimpleFeatureType, Map[String, Int]] =
    loadingCacheBuilder { sft =>
      DataUtilities.attributeNames(sft).map { name => (name, sft.indexOf(name))}.toMap
    }

  val datumWriterCache: LoadingCache[SimpleFeatureType, GenericDatumWriter[GenericRecord]] =
    loadingCacheBuilder { sft =>
      new GenericDatumWriter[GenericRecord](avroSchemaCache.get(sft))
    }

  val attributeNameLookUp = Maps.newConcurrentMap[String, String]()

  final val FEATURE_ID_AVRO_FIELD_NAME: String = "__fid__"
  final val AVRO_SIMPLE_FEATURE_VERSION: String = "__version__"
  final val VERSION: Int = 1
  final val AVRO_NAMESPACE: String = "org.geomesa"

  def encode(s: String): String = "_" + Hex.encodeHexString(s.getBytes("UTF8"))

  def decode(s: String): String = new String(Hex.decodeHex(s.substring(1).toCharArray), "UTF8")

  def encodeAttributeName(s: String): String = attributeNameLookUp.getOrElseUpdate(s, encode(s))

  def decodeAttributeName(s: String): String = attributeNameLookUp.getOrElseUpdate(s, decode(s))

  def generateSchema(sft: SimpleFeatureType): Schema = {
    val initialAssembler: SchemaBuilder.FieldAssembler[Schema] =
      SchemaBuilder.record(encodeAttributeName(sft.getTypeName))
        .namespace(AVRO_NAMESPACE)
        .fields
        .name(AVRO_SIMPLE_FEATURE_VERSION).`type`.intType.noDefault
        .name(FEATURE_ID_AVRO_FIELD_NAME).`type`.stringType.noDefault

    val result =
      sft.getAttributeDescriptors.foldLeft(initialAssembler) { case (assembler, ad) =>
        addField(assembler, encodeAttributeName(ad.getLocalName), ad.getType.getBinding, ad.isNillable)
      }

    result.endRecord
  }

  def addField(assembler: SchemaBuilder.FieldAssembler[Schema],
               name: String,
               ct: Class[_],
               nillable: Boolean): SchemaBuilder.FieldAssembler[Schema] = {
    val baseType = if (nillable) assembler.name(name).`type`.nullable() else assembler.name(name).`type`
    ct match {
      case c if classOf[String].isAssignableFrom(c)             => baseType.stringType().noDefault()
      case c if classOf[java.lang.Integer].isAssignableFrom(c)  => baseType.intType.noDefault
      case c if classOf[java.lang.Long].isAssignableFrom(c)     => baseType.longType.noDefault
      case c if classOf[java.lang.Double].isAssignableFrom(c)   => baseType.doubleType.noDefault
      case c if classOf[java.lang.Float].isAssignableFrom(c)    => baseType.floatType.noDefault
      case c if classOf[java.lang.Boolean].isAssignableFrom(c)  => baseType.booleanType.noDefault
      case c if classOf[UUID].isAssignableFrom(c)               => baseType.bytesType.noDefault
      case c if classOf[Date].isAssignableFrom(c)               => baseType.longType.noDefault
      case c if classOf[Geometry].isAssignableFrom(c)           => baseType.stringType.noDefault
    }
  }

}
