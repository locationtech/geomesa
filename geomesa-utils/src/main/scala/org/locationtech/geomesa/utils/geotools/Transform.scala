/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.Collections

import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.geotools.filter.MathExpressionImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.utils.geotools.sft.{ImmutableAttributeDescriptor, ImmutableGeometryDescriptor, ImmutableSimpleFeatureType}
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.{Expression, Function, PropertyName}

import scala.util.control.NonFatal

/**
 * An attribute transformation or projection
 */
sealed trait Transform {

  /**
   * The name of the attribute
   *
   * @return
   */
  def name: String

  /**
   * The type of the attribute
   *
   * @return
   */
  def binding: Class[_]

  /**
   * Evaluate the transform against a full feature
   *
   * @param feature simple feature, of the original, untransformed feature type
   * @return
   */
  def evaluate(feature: SimpleFeature): AnyRef
}

/**
  * Convenience methods for working with relational query projections (aka transforms)
  */
object Transform {

  val DefinitionDelimiter = ";"

  /**
   * A simple relational projection (selection)
   *
   * @param name name
   * @param binding binding
   * @param i index of the original attribute being projected
   */
  case class PropertyTransform(name: String, binding: Class[_], i: Int) extends Transform {
    override def evaluate(feature: SimpleFeature): AnyRef = feature.getAttribute(i)
  }

  /**
   * A simple relational projection, but also renamed
   *
   * @param name name
   * @param binding binding
   * @param original name of the original attribute being projected
   * @param i index of the original attribute being projected
   */
  case class RenameTransform(name: String, binding: Class[_], original: String, i: Int) extends Transform {
    override def evaluate(feature: SimpleFeature): AnyRef = feature.getAttribute(i)
  }

  /**
   * An attribute comprised of an expression (function, math transform, json-path, etc)
   *
   * @param name name
   * @param binding binding
   * @param expression expression
   */
  case class ExpressionTransform(name: String, binding: Class[_], expression: Expression) extends Transform {
    override def evaluate(feature: SimpleFeature): AnyRef = expression.evaluate(feature)
  }

  object Transforms {

    /**
     * Create transform definitions for a delimited expression string
     *
     * @param sft original simple feature type
     * @param transforms semi-colon delimited transform expressions
     * @return
     */
    def apply(sft: SimpleFeatureType, transforms: String): Seq[Transform] =
      if (transforms.isEmpty) { Seq.empty } else { apply(sft, transforms.split(DefinitionDelimiter)) }

    /**
     * Create transform definitions for a sequence of expression strings
     *
     * @param sft original simple feature type
     * @param transforms transform expressions
     * @return
     */
    def apply(sft: SimpleFeatureType, transforms: Seq[String]): Seq[Transform] =
      transforms.map(definition(sft, _))

    /**
     * Create a transform definition from an expression
     *
     * @param sft simple feature type
     * @param transform transform expression
     * @return
     */
    private def definition(sft: SimpleFeatureType, transform: String): Transform = {
      val equals = transform.indexOf('=')
      if (equals == -1) {
        val name = transform.trim()
        val i = sft.indexOf(name)
        if (i == -1) {
          attributeExpression(sft, name)
        } else {
          PropertyTransform(name, sft.getDescriptor(i).getType.getBinding, i)
        }
      } else {
        val name = transform.substring(0, equals).trim()
        val exp = transform.substring(equals + 1).trim()
        val expression = try { ECQL.toExpression(exp) } catch {
          case NonFatal(e) => throw new IllegalArgumentException(s"Unable to parse expression $transform:", e)
        }
        expression match {
          case f: Function =>
            ExpressionTransform(name, f.getFunctionName.getReturn.getType, f)

          case f: MathExpressionImpl =>
            ExpressionTransform(name, classOf[java.lang.Double], f) // math ops always return doubles?

          case p: PropertyName =>
            val orig = p.getPropertyName
            val i = sft.indexOf(orig)
            if (i == -1) {
              attributeExpression(sft, orig).copy(name = name)
            } else if (name == orig) {
              PropertyTransform(name, sft.getDescriptor(i).getType.getBinding, i)
            } else {
              RenameTransform(name, sft.getDescriptor(i).getType.getBinding, orig, i)
            }

          // TODO: Add support for LiteralExpressionImpl and/or ClassificationFunction?
          case _ =>
            throw new IllegalArgumentException(s"Unable to handle transform expression: $expression")
        }
      }
    }

    /**
     * This handles custom attribute accessors (e.g. json-path) and transforms that don't correspond
     * to any attribute (e.g. in AttributeKeyPlusValueIterator)
     *
     * @param sft simple feature type
     * @param e expression
     * @return
     */
    private def attributeExpression(sft: SimpleFeatureType, e: String): ExpressionTransform = {
      try {
        val expression = ECQL.toExpression(e)
        val descriptor = expression.evaluate(sft).asInstanceOf[AttributeDescriptor]
        val binding = if (descriptor == null) { classOf[String] } else { descriptor.getType.getBinding }
        ExpressionTransform(e, binding, expression)
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Unable to parse expression '$e':", e)
      }
    }

    /**
     * Create the feature type corresponding a transform
     *
     * @param sft original simple feature type
     * @param transforms transforms
     * @return
     */
    def schema(sft: SimpleFeatureType, transforms: Seq[Transform]): SimpleFeatureType = {
      val schema = new java.util.ArrayList[AttributeDescriptor]()
      var geom: GeometryDescriptor = null

      lazy val typeBuilder = new AttributeTypeBuilder()

      transforms.foreach { t =>
        val descriptor = t match {
          case t: PropertyTransform =>
            val d = sft.getDescriptor(t.i)
            val im = SimpleFeatureTypes.immutable(d)
            if (d == sft.getGeometryDescriptor) {
              geom = im.asInstanceOf[GeometryDescriptor]
            }
            im

          case t: RenameTransform =>
            sft.getDescriptor(t.i) match {
              case d: GeometryDescriptor =>
                val im = new ImmutableGeometryDescriptor(d.getType, new NameImpl(t.name), d.getMinOccurs,
                  d.getMaxOccurs, d.isNillable, d.getDefaultValue, d.getUserData)
                if (d == sft.getGeometryDescriptor) {
                  geom = im
                }
                im
              case d: AttributeDescriptor =>
                new ImmutableAttributeDescriptor(d.getType, new NameImpl(t.name), d.getMinOccurs, d.getMaxOccurs,
                  d.isNillable, d.getDefaultValue, d.getUserData)
            }

          case t: ExpressionTransform =>
            typeBuilder.setBinding(t.binding)
            if (classOf[Geometry].isAssignableFrom(t.binding)) {
              typeBuilder.crs(CRS_EPSG_4326)
              val typ = typeBuilder.buildGeometryType()
              new ImmutableGeometryDescriptor(typ, new NameImpl(t.name), 0, 1, true, null, Collections.emptyMap())
            } else {
              val typ = typeBuilder.buildType()
              new ImmutableAttributeDescriptor(typ, new NameImpl(t.name), 0, 1, true, null, Collections.emptyMap())
            }
        }

        schema.add(descriptor)
      }

      var i = 0
      while (geom == null && i < schema.size) {
        schema.get(i) match {
          case d: ImmutableGeometryDescriptor => geom = d
          case _ => // no-op
        }
        i += 1
      }

      // TODO reconsider default field user data?
      new ImmutableSimpleFeatureType(sft.getName, schema, geom, sft.isAbstract, sft.getRestrictions, sft.getSuper,
        sft.getDescription, sft.getUserData)
    }
  }
}
