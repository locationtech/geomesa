/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect.filter

import org.apache.commons.text.StringEscapeUtils
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter._
import org.geotools.api.filter.expression.{Expression, Literal, PropertyName}
import org.geotools.data.postgis.PostgisPSFilterToSql
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.FilterCapabilities
import org.geotools.util.Version
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisPsDialect
import org.locationtech.geomesa.utils.json.JsonPathParser.{JsonPath, PathAttribute, PathDeepScan}
import org.locationtech.geomesa.utils.json.{JsonPathFilterFunction, JsonPathPropertyAccessor}

import scala.util.control.NonFatal

/**
 * Custom filter-to-sql implementation
 *
 * @param dialect dialect
 * @param pgVersion pg version
 */
class PartitionedPostgisPsFilterToSql(dialect: PartitionedPostgisPsDialect, pgVersion: Version)
    extends PostgisPSFilterToSql(dialect, pgVersion) {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private var fnEncoding = false

  override def setFunctionEncodingEnabled(functionEncodingEnabled: Boolean): Unit = {
    super.setFunctionEncodingEnabled(functionEncodingEnabled)
    this.fnEncoding = functionEncodingEnabled
  }

  override def setFeatureType(featureType: SimpleFeatureType): Unit = {
    // convert List-type attributes to Array-types so that prepared statement bindings work correctly
    if (featureType.getAttributeDescriptors.asScala.exists(_.getType.getBinding == classOf[java.util.List[_]])) {
      val builder = new SimpleFeatureTypeBuilder() {
        override def init(`type`: SimpleFeatureType): Unit = {
          super.init(`type`)
          attributes().clear()
        }
      }
      builder.init(featureType)
      featureType.getAttributeDescriptors.asScala.foreach { descriptor =>
        val ab = new AttributeTypeBuilder(builder.getFeatureTypeFactory)
        ab.init(descriptor)
        if (descriptor.getType.getBinding == classOf[java.util.List[_]]) {
          ab.setBinding(java.lang.reflect.Array.newInstance(Option(descriptor.getListType()).getOrElse(classOf[String]), 0).getClass)
        }
        builder.add(ab.buildDescriptor(descriptor.getLocalName))
      }
      this.featureType = builder.buildFeatureType()
      this.featureType.getUserData.putAll(featureType.getUserData)
    } else {
      this.featureType = featureType
    }
  }

  // note: this would be a cleaner solution, but it doesn't get invoked due to explicit calls to
  // super.getExpressionType in PostgisPSFilterToSql :/
  override def getExpressionType(expression: Expression): Class[_] = {
    val result = Option(expression).collect { case p: PropertyName => p }.flatMap { p =>
      Option(p.evaluate(featureType).asInstanceOf[AttributeDescriptor]).map { descriptor =>
        val binding = descriptor.getType.getBinding
        if (binding == classOf[java.util.List[_]]) {
          val listType = descriptor.getListType()
          if (listType == null) {
            classOf[Array[String]]
          } else {
            java.lang.reflect.Array.newInstance(listType, 0).getClass
          }
        } else {
          binding
        }
      }
    }

    result.getOrElse(super.getExpressionType(expression))
  }

  override def visit(filter: Or, extraData: AnyRef): AnyRef = {
    // the super-class implementation merges ORs into INs
    // for array-types, skip the super class as it breaks array OR queries
    // for json-path expressions, skip the super class as it breaks json path predicates
    // for other types, keep the super handling as INs may be more efficient that ORs
    def skipOrInProcessing(attribute: String): Boolean =
      attribute.startsWith("$") || Option(featureType.getDescriptor(attribute)).exists(_.getType.getBinding.isArray)

    val names = FilterHelper.propertyNames(filter)
    if (names.exists(skipOrInProcessing)) {
      visit(filter.asInstanceOf[BinaryLogicOperator], "OR")
    } else {
      super.visit(filter, extraData)
    }
  }

  override protected def visitBinaryComparisonOperator(filter: BinaryComparisonOperator, extraData: AnyRef): Unit = {
    try {
      (filter.getExpression1, filter.getExpression2) match {
        case (f: JsonPathFilterFunction, lit: Literal) =>
          writePath(f.path(null), lit, filter, flipped = false, extraData)

        case (lit: Literal, f: JsonPathFilterFunction) =>
          writePath(f.path(null), lit, filter, flipped = true, extraData)

        case (p: PropertyName, lit: Literal) if p.getPropertyName.startsWith("$") =>
          writePath(p.getPropertyName, lit, filter, flipped = false, extraData)

        case (lit: Literal, p: PropertyName) if p.getPropertyName.startsWith("$") =>
          writePath(p.getPropertyName, lit, filter, flipped = true, extraData)

        case _ => super.visitBinaryComparisonOperator(filter, extraData)
      }
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Error encoding filter into SQL: $filter", e)
    }
  }

  private def writePath(pathString: String, lit: Literal, filter: BinaryComparisonOperator, flipped: Boolean, extraData: AnyRef): AnyRef = {
    val op = filter match {
      case _: PropertyIsEqualTo => "=="
      case _: PropertyIsGreaterThan if flipped => "<"
      case _: PropertyIsGreaterThan => ">"
      case _: PropertyIsGreaterThanOrEqualTo if flipped => "<="
      case _: PropertyIsGreaterThanOrEqualTo => ">="
      case _: PropertyIsLessThan if flipped => ">"
      case _: PropertyIsLessThan => "<"
      case _: PropertyIsLessThanOrEqualTo if flipped => ">="
      case _: PropertyIsLessThanOrEqualTo => "<="
      case _: PropertyIsNotEqualTo => "!="
      case _ => throw new UnsupportedOperationException(s"Unexpected binary comparison op: $filter")
    }
    val path = JsonPathPropertyAccessor.Paths.get(pathString)
    path.head match {
      case a: PathAttribute => writePath(a.name, path.tail, lit, op, extraData)
      case e => throw new UnsupportedOperationException(s"Json paths must start with an attribute: $e")
    }
  }

  private def writePath(attribute: String, path: JsonPath, lit: Literal, op: String, extraData: AnyRef): AnyRef = {
    if (!isPrepareEnabled) {
      throw new UnsupportedOperationException(s"Json paths expressions are only supported when using prepared statements")
    }
    if (path.elements.contains(PathDeepScan)) {
      // helps prevent denial of service
      throw new UnsupportedOperationException(s"Json paths deep scan expressions (..) are not supported")
    }
    visit(FilterHelper.ff.property(attribute), extraData)
    out.write(" @@ ")
    val expression = lit.getValue match {
      case s: String => '"' + StringEscapeUtils.escapeJava(s) + '"'
      case n: Number => String.valueOf(n)
      case b: java.lang.Boolean => String.valueOf(b)
      case null => "null"
      case v => throw new UnsupportedOperationException(s"Received unsupported literal in JSON path expression: ${v.getClass} $v")
    }
    visit(FilterHelper.ff.literal(s"$path $op $expression"), extraData)
    out.write("::jsonpath")
    extraData
  }

  override protected def createFilterCapabilities(): FilterCapabilities = {
    val caps = super.createFilterCapabilities()
    if (fnEncoding) {
      caps.addType(classOf[JsonPathFilterFunction])
    }
    caps
  }
}
