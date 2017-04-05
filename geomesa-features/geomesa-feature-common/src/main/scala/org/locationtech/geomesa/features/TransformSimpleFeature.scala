/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features

import java.util.{Collection => jCollection, List => jList, Map => jMap}

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.{FunctionExpressionImpl, MathExpressionImpl}
import org.geotools.filter.expression.PropertyAccessors
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess
import org.geotools.process.vector.TransformProcess.Definition
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

/**
  * Simple feature implementation that wraps another feature type and applies a transform/projection
  *
  * @param transformSchema transformed feature type
  * @param attributes attribute evaluations, in order
  */
class TransformSimpleFeature(transformSchema: SimpleFeatureType,
                             attributes: Array[(SimpleFeature) => AnyRef],
                             private var underlying: SimpleFeature = null) extends SimpleFeature {

  private lazy val geomIndex = transformSchema.indexOf(transformSchema.getGeometryDescriptor.getLocalName)

  def setFeature(sf: SimpleFeature): Unit = underlying = sf

  override def getAttribute(index: Int): AnyRef = attributes(index).apply(underlying)

  override def getIdentifier: FeatureId = underlying.getIdentifier
  override def getID: String = underlying.getID

  override def getUserData: jMap[AnyRef, AnyRef] = underlying.getUserData

  override def getType: SimpleFeatureType = transformSchema
  override def getFeatureType: SimpleFeatureType = transformSchema
  override def getName: Name = transformSchema.getName

  override def getAttribute(name: Name): AnyRef = getAttribute(name.getLocalPart)
  override def getAttribute(name: String): Object = {
    val index = transformSchema.indexOf(name)
    if (index == -1) null else getAttribute(index)
  }

  override def getDefaultGeometry: AnyRef = getAttribute(geomIndex)
  override def getAttributeCount: Int = transformSchema.getAttributeCount

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry => new ReferencedEnvelope(g.getEnvelopeInternal, transformSchema.getCoordinateReferenceSystem)
    case _           => new ReferencedEnvelope(transformSchema.getCoordinateReferenceSystem)
  }

  override def getAttributes: jList[AnyRef] = {
    val attributes = new java.util.ArrayList[AnyRef](transformSchema.getAttributeCount)
    var i = 0
    while (i < transformSchema.getAttributeCount) {
      attributes.add(getAttribute(i))
      i += 1
    }
    attributes
  }

  override def getDefaultGeometryProperty = throw new NotImplementedError
  override def getProperties: jCollection[Property] = throw new NotImplementedError
  override def getProperties(name: Name) = throw new NotImplementedError
  override def getProperties(name: String) = throw new NotImplementedError
  override def getProperty(name: Name) = throw new NotImplementedError
  override def getProperty(name: String) = throw new NotImplementedError
  override def getValue = throw new NotImplementedError
  override def getDescriptor = throw new NotImplementedError

  override def setAttribute(name: Name, value: Object) = throw new NotImplementedError
  override def setAttribute(name: String, value: Object) = throw new NotImplementedError
  override def setAttribute(index: Int, value: Object) = throw new NotImplementedError
  override def setAttributes(vals: jList[Object]) = throw new NotImplementedError
  override def setAttributes(vals: Array[Object]) = throw new NotImplementedError
  override def setDefaultGeometry(geo: Object) = throw new NotImplementedError
  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute) = throw new NotImplementedError
  override def setValue(newValue: Object) = throw new NotImplementedError
  override def setValue(values: jCollection[Property]) = throw new NotImplementedError

  override def isNillable = true
  override def validate() = throw new NotImplementedError

  override def toString = s"TransformSimpleFeature:$getID"
}

object TransformSimpleFeature {

  import scala.collection.JavaConversions._
  import scala.collection.JavaConverters._

  def apply(sft: SimpleFeatureType, transformSchema: SimpleFeatureType, transforms: String): TransformSimpleFeature = {
    val a = attributes(sft, transformSchema, transforms)
    new TransformSimpleFeature(transformSchema, a)
  }

  def apply(sft: SimpleFeatureType, properties: Seq[String]): TransformSimpleFeature = {

    val (transformSft, transforms) = queryToTransformSFT(sft, properties)

    TransformSimpleFeature(sft, transformSft, transforms)
  }

  def attributes(sft: SimpleFeatureType,
                 transformSchema: SimpleFeatureType,
                 transforms: String): Array[(SimpleFeature) => AnyRef] = {
    TransformProcess.toDefinition(transforms).map(attribute(sft, _)).toArray
  }

  def queryToTransformSFT(sft: SimpleFeatureType, properties: Seq[String]): (SimpleFeatureType, String) = {
    import scala.collection.JavaConversions._

    if (properties != null && properties.nonEmpty &&
      properties != sft.getAttributeDescriptors.map(_.getLocalName)) {
      val (transformProps, regularProps) = properties.partition(_.contains('='))
      val convertedRegularProps = regularProps.map { p => s"$p=$p" }
      val allTransforms = convertedRegularProps ++ transformProps
      // ensure that the returned props includes geometry, otherwise we get exceptions everywhere
      val geomTransform = {
        val allGeoms = sft.getAttributeDescriptors.collect {
          case d if classOf[Geometry].isAssignableFrom(d.getType.getBinding) => d.getLocalName
        }
        val geomMatches = for (t <- allTransforms.iterator; g <- allGeoms) yield { t.matches(s"$g\\s*=.*") }
        if (geomMatches.contains(true)) { Nil } else {
          Option(sft.getGeometryDescriptor).map(_.getLocalName).map(geom => s"$geom=$geom").toSeq
        }
      }
      val transforms = (allTransforms ++ geomTransform).mkString(";")
      val transformDefs = TransformProcess.toDefinition(transforms)
      val derivedSchema = computeSchema(sft, transformDefs.asScala)

      (derivedSchema, transforms)
    } else {
      (sft, "")
    }

  }

  private def computeSchema(origSFT: SimpleFeatureType, transforms: Seq[Definition]): SimpleFeatureType = {
    import scala.collection.JavaConversions._
    val descriptors: Seq[AttributeDescriptor] = transforms.map { definition =>
      val name = definition.name
      val cql  = definition.expression
      cql match {
        case p: PropertyName =>
          val prop = p.getPropertyName
          if (origSFT.getAttributeDescriptors.exists(_.getLocalName == prop)) {
            val origAttr = origSFT.getDescriptor(prop)
            val ab = new AttributeTypeBuilder()
            ab.init(origAttr)
            val descriptor = if (origAttr.isInstanceOf[GeometryDescriptor]) {
              ab.buildDescriptor(name, ab.buildGeometryType())
            } else {
              ab.buildDescriptor(name, ab.buildType())
            }
            descriptor.getUserData.putAll(origAttr.getUserData)
            descriptor
          } else if (PropertyAccessors.findPropertyAccessors(new ScalaSimpleFeature("", origSFT), prop, null, null).nonEmpty) {
            // note: we return String as we have to use a concrete type, but the json might return anything
            val ab = new AttributeTypeBuilder().binding(classOf[String])
            ab.buildDescriptor(name, ab.buildType())
          } else {
            throw new IllegalArgumentException(s"Attribute '$prop' does not exist in SFT '${origSFT.getTypeName}'.")
          }

        case f: FunctionExpressionImpl  =>
          val clazz = f.getFunctionName.getReturn.getType
          val ab = new AttributeTypeBuilder().binding(clazz)
          if (classOf[Geometry].isAssignableFrom(clazz)) {
            ab.buildDescriptor(name, ab.buildGeometryType())
          } else {
            ab.buildDescriptor(name, ab.buildType())
          }
        // Do math ops always return doubles?
        case a: MathExpressionImpl =>
          val ab = new AttributeTypeBuilder().binding(classOf[java.lang.Double])
          ab.buildDescriptor(name, ab.buildType())

        //TODO Add other classes here?
      }
    }

    val geomAttributes = descriptors.filter(_.isInstanceOf[GeometryDescriptor]).map(_.getLocalName)
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.setName(origSFT.getName)
    sftBuilder.addAll(descriptors.toArray)
    if (geomAttributes.nonEmpty) {
      val defaultGeom = if (geomAttributes.size == 1) { geomAttributes.head } else {
        // try to find a geom with the same name as the original default geom
        val origDefaultGeom = origSFT.getGeometryDescriptor.getLocalName
        geomAttributes.find(_ == origDefaultGeom).getOrElse(geomAttributes.head)
      }
      sftBuilder.setDefaultGeometry(defaultGeom)
    }
    val schema = sftBuilder.buildFeatureType()
    schema.getUserData.putAll(origSFT.getUserData)
    schema
  }

  private def attribute(sft: SimpleFeatureType, d: TransformProcess.Definition): (SimpleFeature) => AnyRef = {
    d.expression match {
      case p: PropertyName => val i = sft.indexOf(p.getPropertyName); (sf) => sf.getAttribute(i)
      case e: Expression   => (sf) => e.evaluate(sf)
    }
  }
}
