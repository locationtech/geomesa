package org.locationtech.geomesa.utils.geotools.builder

import org.geotools.feature.NameImpl
import org.geotools.feature.`type`.AttributeDescriptorImpl
import org.opengis.feature.`type`.{AttributeDescriptor, ComplexType, Name}

import scala.collection.mutable

class TopLevelDescriptorBuilder extends AbstractFeatureTypeBuilder[AttributeDescriptor] {
  def name(n: String): DescriptorBuilder[_<:AttributeDescriptor] = {
    _name = new NameImpl(targetNs, n)
    `type`(n + "Type")
  }

  def end = new AttributeDescriptorImpl(makeType, _name, 1, 1, false, null)

  val typeMap: scala.collection.mutable.Map[Name,ComplexType] = mutable.Map.empty

  override def saveType[T <: ComplexType](t: T) = {typeMap(t.getName)=t;t}

  override def getType(name:Name) = typeMap.get(name)

  override def getType(name:Name, computeFn: =>ComplexType) = typeMap.getOrElseUpdate(name, computeFn)
}