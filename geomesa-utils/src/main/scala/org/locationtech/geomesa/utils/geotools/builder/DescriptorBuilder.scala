package org.locationtech.geomesa.utils.geotools.builder

import java.util

import org.geotools.feature.NameImpl
import org.geotools.feature.`type`.{AttributeDescriptorImpl, AttributeTypeImpl, GeometryDescriptorImpl, GeometryTypeImpl}
import org.geotools.gml3.GMLSchema
import org.geotools.xs.XSSchema
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{USER_DATA_LIST_TYPE, USER_DATA_MAP_KEY_TYPE, USER_DATA_MAP_VALUE_TYPE}
import org.opengis.feature.Property
import org.opengis.feature.`type`._
import org.opengis.filter.Filter
import org.opengis.util.InternationalString

import scala.reflect.ClassTag

class DescriptorBuilder[ReturnType](val target: AbstractFeatureTypeBuilder[ReturnType]) {

  private var _optional: Boolean = false
  private var _multi: Boolean = false
  private var ns: String = _
  private var name: String = _

  reset()

  private def reset() {
    _optional = false
    _multi = false
    ns = target.targetNs
  }

  def optional = {
    _optional = true
    this
  }

  def multi = {
    _multi = true
    this
  }

  def namespace(ns: String): DescriptorBuilder[ReturnType] = {
    this.ns = ns
    this
  }

  def string(name: String) = property(XSSchema.STRING_TYPE, name)

  def intProp(name: String) = property(XSSchema.INT_TYPE, name)

  def bool(name: String) = property(XSSchema.BOOLEAN_TYPE, name)

  def geometry(name: String) = property(GMLSchema.GEOMETRYPROPERTYTYPE_TYPE, name)
  def point(name: String) = property(GMLSchema.POINTPROPERTYTYPE_TYPE, name)
  def linestring(name: String) = property(GMLSchema.LINESTRINGPROPERTYTYPE_TYPE, name)
  def polygon(name: String) = property(GMLSchema.POLYGONPROPERTYTYPE_TYPE, name)

  def multiGeometry(name: String) = property(GMLSchema.MULTIGEOMETRYPROPERTYTYPE_TYPE, name)
  def pointArray(name: String) = property(GMLSchema.POINTARRAYPROPERTYTYPE_TYPE, name)
  def multiPoint(name: String) = property(GMLSchema.MULTIPOINTPROPERTYTYPE_TYPE, name)
  def multiLinestring(name: String) = property(GMLSchema.MULTILINESTRINGPROPERTYTYPE_TYPE, name)
  def multiPolygon(name: String) = property(GMLSchema.MULTIPOLYGONPROPERTYTYPE_TYPE, name)

  def property(`type`: AttributeType, name: String) = {
    this.name = name
    finish(`type`)
  }

  def property(name: String) = {
    this.name = name
    new SubFeatureTypeBuilder[ReturnType](this).namespace(ns)
  }

  def feature(name: String, ft: FeatureType) = {
    val descName = ft.getName.getLocalPart.stripSuffix("Type")
    val ptn = new NameImpl(ft.getName.getNamespaceURI, descName+"PropertyType")
    val pt = target.getType(ptn,
      FeatureTypeBuilder.namespace(ptn.getNamespaceURI)
        .`type`(ptn.getLocalPart, GMLSchema.FEATUREPROPERTYTYPE_TYPE)
          .property(ft, descName)
        .end
    )
    property(pt, name)
  }

  def deferredFeature(name: String, ft: =>FeatureType) = {

    val propertyType = new ComplexType {

      private[this] lazy val delegate = {
        val descName = ft.getName.getLocalPart.stripSuffix("Type")
        val ptn = new NameImpl(ft.getName.getNamespaceURI, descName+"PropertyType")
        target.getType(ptn,
          FeatureTypeBuilder.namespace(ptn.getNamespaceURI)
            .`type`(ptn.getLocalPart, GMLSchema.FEATUREPROPERTYTYPE_TYPE)
            .property(ft, descName)
            .end
        )
      }

      override def getSuper: AttributeType = delegate.getSuper
      override def isIdentified: Boolean = delegate.isIdentified
      override def getName: Name = delegate.getName
      override def getDescription: InternationalString = delegate.getDescription
      override def isAbstract: Boolean = delegate.isAbstract
      override def getBinding: Class[util.Collection[Property]] = delegate.getBinding
      override def getRestrictions: util.List[Filter] = delegate.getRestrictions
      override def getUserData: util.Map[AnyRef, AnyRef] = delegate.getUserData
      override def getDescriptor(name: Name): PropertyDescriptor = delegate.getDescriptor(name)
      override def getDescriptor(name: String): PropertyDescriptor = delegate.getDescriptor(name)
      override def isInline: Boolean = delegate.isInline
      override def getDescriptors: util.Collection[PropertyDescriptor] = delegate.getDescriptors
      override def toString = delegate.toString
      override def hashCode:Int = delegate.hashCode
      override def equals(x:Any) = delegate.equals(x)
    }

    property(propertyType, name)
  }

  def list[T](name:String)(implicit tag:ClassTag[T]) = {
    val pt = new AttributeTypeImpl(new NameImpl(ns, s"ListOf${tag.runtimeClass.getSimpleName}Type"), classOf[java.util.List[_]], false, false, java.util.Collections.emptyList(), null, null)
    pt.getUserData.put(USER_DATA_LIST_TYPE, tag.runtimeClass)
    this.name = name
    finish(pt, Map(USER_DATA_LIST_TYPE->tag.runtimeClass))
  }

  def map[K,V](name:String)(implicit kTag:ClassTag[K], vTag:ClassTag[V]) = {
    val pt = new AttributeTypeImpl(new NameImpl(ns, s"MapOf${kTag.runtimeClass.getSimpleName}To${vTag.runtimeClass.getSimpleName}Type"), classOf[java.util.Map[K,V]], false, false, java.util.Collections.emptyList(), null, null)
    pt.getUserData.put(USER_DATA_MAP_KEY_TYPE, kTag.runtimeClass)
    pt.getUserData.put(USER_DATA_MAP_VALUE_TYPE, vTag.runtimeClass)
    this.name = name
    finish(pt, Map(USER_DATA_MAP_KEY_TYPE->kTag.runtimeClass, USER_DATA_MAP_VALUE_TYPE->vTag.runtimeClass))
  }
  //private def uncapitalize(s:String) = if(s.isEmpty || s(0).isLower) s else s(0).toLower + s.substring(1)

  private[builder] def finish(`type`: AttributeType, userdata:Map[AnyRef,AnyRef]=Map.empty) = {
    val desc = if (`type`.isInstanceOf[GeometryTypeImpl]) new GeometryDescriptorImpl(
      `type`.asInstanceOf[GeometryType],
      new NameImpl(ns, name),
      if (_optional) 0 else 1,
      if (_multi) -1 else 1,
      false,
      null
    ) else new AttributeDescriptorImpl(
      `type`,
      new NameImpl(ns, name),
      if (_optional) 0 else 1,
      if (_multi) -1 else 1,
      false,
      null
    )

    for((k,v)<-userdata)
      desc.getUserData.put(k,v)

    target.addDescriptor(desc)
    reset()
    this
  }

  def end = {
    target.end
  }
}