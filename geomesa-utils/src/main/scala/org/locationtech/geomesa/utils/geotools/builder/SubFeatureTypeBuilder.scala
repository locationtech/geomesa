package org.locationtech.geomesa.utils.geotools.builder
import org.opengis.feature.`type`.{ComplexType,Name}

class SubFeatureTypeBuilder[ReturnType](val target: DescriptorBuilder[ReturnType]) extends AbstractFeatureTypeBuilder[DescriptorBuilder[ReturnType]] {
  override def namespace(ns: String) = super.namespace(ns)

  def end = {
    target.finish(makeType)
    target
  }

  override def saveType[T <: ComplexType](t: T): T = target.target.saveType(t)
  override def getType(name:Name) = target.target.getType(name)
  override def getType(name:Name, computeFn: =>ComplexType) = target.target.getType(name, computeFn)
}