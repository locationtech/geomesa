package org.locationtech.geomesa.utils.geotools.builder

import org.opengis.feature.`type`.{ComplexType, FeatureType, Name}

import scala.collection.mutable

class TopLevelFeatureTypeBuilder extends AbstractFeatureTypeBuilder[ComplexType] {

  def end = makeType

  val typeMap: mutable.Map[Name,ComplexType] = mutable.Map.empty

  override def saveType[T <: ComplexType](t: T) = {typeMap(t.getName)=t;t}

  override def getType(name:Name) = typeMap.get(name)

  override def getType(name:Name, computeFn: =>ComplexType) = typeMap.getOrElseUpdate(name, computeFn)

  override def `type`(name:String): DescriptorBuilder[FeatureType] = super.`type`(name).asInstanceOf[DescriptorBuilder[FeatureType]]
}