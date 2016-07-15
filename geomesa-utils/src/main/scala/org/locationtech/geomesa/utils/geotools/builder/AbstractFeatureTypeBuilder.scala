package org.locationtech.geomesa.utils.geotools.builder

import org.geotools.feature
import org.geotools.feature.NameImpl
import org.geotools.feature.`type`.{ComplexTypeImpl, FeatureTypeImpl}
import org.geotools.gml3.GMLSchema
import org.opengis.feature.`type`.{ComplexType, Name, PropertyDescriptor, PropertyType, AttributeDescriptor}

import scala.collection.Iterator.iterate
import scala.collection.JavaConversions._
import scala.collection.mutable

object AbstractFeatureTypeBuilder {
  def isDescendedFrom(child: PropertyType, ancestor: PropertyType): Boolean = iterate(child)(_.getSuper).takeWhile(_ != null).contains(ancestor)
  def subclassByRestriction(child: PropertyType) =
    isDescendedFrom(child, GMLSchema.FEATUREPROPERTYTYPE_TYPE)
}

abstract class AbstractFeatureTypeBuilder[ReturnType] {


  import AbstractFeatureTypeBuilder._
  var targetNs: String = null
  var _name: NameImpl = null
  var superType: ComplexType = null
  var descIndex = 0
  final val descriptors: mutable.Buffer[PropertyDescriptor] = mutable.Buffer.empty

  def namespace(ns: String): this.type = {
    targetNs = ns
    this
  }

  def `type`(name: String): DescriptorBuilder[_<:ReturnType] = `type`(name, GMLSchema.ABSTRACTFEATURETYPE_TYPE)

  def `type`(name: String, superType: ComplexType): DescriptorBuilder[ReturnType] = {
    _name = new NameImpl(targetNs, name)
    this.superType = superType
    new DescriptorBuilder[ReturnType](this)
  }

  protected[builder] def addDescriptor(desc: AttributeDescriptor): Unit =
      descriptors += desc

  def end: ReturnType

  def makeType =
    if (isDescendedFrom(superType, GMLSchema.ABSTRACTFEATURETYPE_TYPE))
      saveType(new FeatureTypeImpl(_name, descriptors, null, false, null, superType, null))
    else
      saveType(new ComplexTypeImpl(_name, descriptors, false, false, null, superType, null))

  def saveType[T<:ComplexType](t:T): T
  def getType(name:Name): Option[ComplexType]
  def getType(name:Name, computeFn: =>ComplexType): ComplexType
}