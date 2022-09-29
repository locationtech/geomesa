/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams.serde

import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox


/**
 * SimpleFeature skeleton that only provides the methods required for GeoMesa serialization, which are:
 *   * `def getAttribute(i: Int): AnyRef`
 *   * `def getUserData: java.util.Map[AnyRef, AnyRef]`
 *
 * See
 *   * @see [[org.locationtech.geomesa.features.kryo.impl.KryoFeatureSerialization#writeFeature]]
 *   * @see [[org.locationtech.geomesa.features.avro.AvroSimpleFeatureWriter#write]]
 *
 * @param converters attribute converters to enforce feature type schema
 * @param attributes message attributes
 */
// noinspection NotImplementedCode
private[streams] class SerializableFeature(
    converters: Array[AnyRef => AnyRef],
    attributes: IndexedSeq[AnyRef],
    userData: Map[String, String]
  ) extends SimpleFeature {

  import scala.collection.JavaConverters._

  override def getAttribute(i: Int): AnyRef = converters(i).apply(attributes(i))
  override def getUserData: java.util.Map[AnyRef, AnyRef] =
    userData.asJava.asInstanceOf[java.util.Map[AnyRef, AnyRef]]

  override def getID: String = ???
  override def getType: SimpleFeatureType = ???
  override def getFeatureType: SimpleFeatureType = ???
  override def getAttributes: java.util.List[AnyRef] = ???
  override def setAttributes(list:java.util.List[AnyRef]): Unit = ???
  override def setAttributes(objects: Array[AnyRef]): Unit = ???
  override def getAttribute(s: String): AnyRef = ???
  override def setAttribute(s: String, o: Any): Unit = ???
  override def getAttribute(name: Name): AnyRef = ???
  override def setAttribute(name: Name, o: Any): Unit = ???
  override def setAttribute(i: Int, o: Any): Unit = ???
  override def getAttributeCount: Int = ???
  override def getDefaultGeometry: AnyRef = ???
  override def setDefaultGeometry(o: Any): Unit = ???
  override def getIdentifier: FeatureId = ???
  override def getBounds: BoundingBox = ???
  override def getDefaultGeometryProperty: GeometryAttribute = ???
  override def setDefaultGeometryProperty(geometryAttribute: GeometryAttribute): Unit = ???
  override def setValue(collection:java.util.Collection[Property]): Unit = ???
  override def getValue:java.util.Collection[_ <: Property] = ???
  override def getProperties(name: Name):java.util.Collection[Property] = ???
  override def getProperty(name: Name): Property = ???
  override def getProperties(s: String):java.util.Collection[Property] = ???
  override def getProperties:java.util.Collection[Property] = ???
  override def getProperty(s: String): Property = ???
  override def validate(): Unit = ???
  override def getDescriptor: AttributeDescriptor = ???
  override def setValue(o: Any): Unit = ???
  override def getName: Name = ???
  override def isNillable: Boolean = ???
}
