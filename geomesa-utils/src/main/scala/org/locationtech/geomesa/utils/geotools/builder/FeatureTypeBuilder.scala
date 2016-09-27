package org.locationtech.geomesa.utils.geotools.builder

import java.util.UUID

import org.geotools.feature.`type`.AttributeDescriptorImpl
import org.geotools.feature.{AttributeImpl, ComplexAttributeImpl, FeatureImpl, NameImpl}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.gml3.GMLSchema
import org.opengis.feature.`type`._
import org.opengis.feature.{ComplexAttribute, Feature, Property}

import scala.collection.JavaConversions._
import scala.collection.mutable

object FeatureTypeBuilder {
  def namespace(name: String) = new TopLevelFeatureTypeBuilder().namespace(name)

  def descriptor(namespace: String, local: String): DescriptorBuilder[_<:AttributeDescriptor] = {
    new TopLevelDescriptorBuilder().namespace(namespace).name(local)
  }

  def feature(`type`: ComplexType, name: String, id: String, values: Any*): Feature = {
    val desc: AttributeDescriptor = new AttributeDescriptorImpl(`type`, new NameImpl(`type`.getName.getNamespaceURI, name), 1, 1, false, null)
    val props = for((t,v)<-`type`.getDescriptors.zip(values)) yield new AttributeImpl(v, t.asInstanceOf[AttributeDescriptor], null)
    new FeatureImpl(props, desc, new FeatureIdImpl(id))
  }
}

object FeatureBuilder {
  def apply(t: ComplexType)(fn: ComplexAttributeBuilder=>ComplexAttributeBuilder) = fn(new ComplexAttributeBuilder(t, makeDescriptor(t))).end
  def apply(t: FeatureType)(fn: FeatureBuilder=>FeatureBuilder):Feature = fn(new FeatureBuilder(t, makeDescriptor(t))).end

  def makeDescriptor(t:ComplexType): AttributeDescriptor = new AttributeDescriptorImpl(t, t.getName, 1, 1, false, null)

  def isDescendedFrom(child: PropertyType, ancestor: PropertyType): Boolean =
    Iterator.iterate(child)(_.getSuper).takeWhile(_ != null).contains(ancestor)

  def subclassByRestriction(child: PropertyType) =
    isDescendedFrom(child, GMLSchema.FEATUREPROPERTYTYPE_TYPE)

  def allProps(ct:ComplexType):Iterable[PropertyDescriptor] = ct.getSuper match {
    case st:ComplexType if subclassByRestriction(st) => allProps(st).drop(ct.getDescriptors.size())++ct.getDescriptors
    case st:ComplexType => allProps(st)++ct.getDescriptors
    case _ => ct.getDescriptors
  }

  @scala.annotation.tailrec
  def nearestSimpleType(at: PropertyType): PropertyType = at match {
    case ct:ComplexType => nearestSimpleType(ct.getSuper)
    case st => st
  }

  class ComplexAttributeBuilder(t: ComplexType,d:AttributeDescriptor) { self =>
    val byName = allProps(t).map(d=>d.getName.getLocalPart->d.asInstanceOf[AttributeDescriptor]).toMap
    val props = mutable.HashMap.empty[AttributeDescriptor, mutable.Buffer[Property]]

    def set(name:String, v: Any): self.type = {
      val Some(desc)=byName.get(name).orElse(if("simpleContent"==name) {
        Some(new AttributeDescriptorImpl(nearestSimpleType(t).asInstanceOf[AttributeType], new NameImpl("simpleContent"),0,1,true,null))
      } else None)

      if(desc.getType.isInstanceOf[ComplexType]) {
        return set(name) { _.set("simpleContent", v) }
      }

      if(!desc.getType.getBinding.isInstance(v)) {
        throw new IllegalArgumentException(s"$v not compatible with ${desc.getName}")
      }

      val prop=v match {
        case p: Property => p
        case _ => new AttributeImpl(v, desc, null)
      }

      internalSet(desc, prop)

      this
    }

    /*def set(name:String): SubFeatureBuilder[self.type] = {
      val desc=byName(name)
      if(!desc.getType.isInstanceOf[ComplexType]) {
        throw new IllegalArgumentException(s"Not a complex type: ${desc.getType.getName}")
      }
      new SubFeatureBuilder[FeatureBuilder.this.type](desc, this)
    }*/

    def set(name:String)(fn:ComplexAttributeBuilder=>ComplexAttributeBuilder):self.type = {
      val desc=byName(name)
      desc.getType match {
        case ft: FeatureType =>
          internalSet(desc, fn(new FeatureBuilder(ft,desc)).end)
        case ct: ComplexType =>
          internalSet(desc, fn(new ComplexAttributeBuilder(ct,desc)).end)
        case _ =>
          throw new IllegalArgumentException(s"Not a complex type: ${desc.getType.getName}")
      }
    }

    protected[builder] def internalSet(desc:AttributeDescriptor, prop:Property): self.type = {
      if(desc.getMaxOccurs != 1) {
        props.getOrElseUpdate(desc, mutable.Buffer.empty).append(prop)
      } else {
        props(desc)=mutable.Buffer(prop)
      }
      this
    }

    def end: ComplexAttribute =
        new ComplexAttributeImpl(props.values.flatten, d, new FeatureIdImpl (UUID.randomUUID ().toString) )
  }

  class FeatureBuilder(t: FeatureType, d:AttributeDescriptor) extends ComplexAttributeBuilder(t,d) {
    override def end: Feature =
        new FeatureImpl(props.values.flatten, d, new FeatureIdImpl (UUID.randomUUID ().toString) )
  }
}


