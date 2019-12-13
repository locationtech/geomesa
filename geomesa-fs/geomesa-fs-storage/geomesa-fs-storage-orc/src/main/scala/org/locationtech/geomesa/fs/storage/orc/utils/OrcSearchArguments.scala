/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.utils

import java.sql.Timestamp

import org.locationtech.jts.geom.{Geometry, Point}
import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgument, SearchArgumentFactory}
import org.apache.orc.TypeDescription
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object OrcSearchArguments {

  /**
    * Creates a push-down predicate for Orc files based on a CQL filter
    *
    * @param sft simple feature type
    * @param description orc file description
    * @param filter cql filter
    * @return
    */
  def apply(sft: SimpleFeatureType,
            description: TypeDescription,
            filter: Filter): Option[(SearchArgument, Array[String])] = {
    import org.apache.orc.TypeDescription.Category._

    val predicates = FilterHelper.propertyNames(filter, sft).distinct.flatMap { prop =>
      val binding = sft.getDescriptor(prop).getType.getBinding
      val preds = if (classOf[Geometry].isAssignableFrom(binding)) {
        if (classOf[Point] == binding) {
          FilterHelper.extractGeometries(filter, prop).values.map(addPoint(prop, _))
        } else {
          // orc doesn't support push-down predicates against complex fields
          Seq.empty
        }
      } else {
        val index = sft.indexOf(prop)

        // count any geom fields before the property, they take up two columns
        val offset = {
          var i = 0
          var geoms = 0
          while (i < index) {
            if (classOf[Geometry].isAssignableFrom(sft.getDescriptor(i).getType.getBinding)) {
              geoms += 1
            }
            i += 1
          }
          geoms
        }

        val category = description.getChildren.get(index + offset).getCategory
        val typeAndConversion = category match {
          case BOOLEAN   => Some(PredicateLeaf.Type.BOOLEAN, (v: Any) => v)
          case INT       => Some(PredicateLeaf.Type.LONG, (v: Any) => v.asInstanceOf[java.lang.Integer].longValue)
          case LONG      => Some(PredicateLeaf.Type.LONG, (v: Any) => v)
          case FLOAT     => Some(PredicateLeaf.Type.FLOAT, (v: Any) => v.asInstanceOf[java.lang.Float].doubleValue)
          case DOUBLE    => Some(PredicateLeaf.Type.FLOAT, (v: Any) => v)
          case STRING    => Some(PredicateLeaf.Type.STRING, (v: Any) => v)
          case TIMESTAMP => Some(PredicateLeaf.Type.TIMESTAMP, (v: Any) => new Timestamp(v.asInstanceOf[java.util.Date].getTime))
          case BINARY | LIST | MAP => None // orc doesn't support push-down predicates against complex fields
          case BYTE | CHAR | SHORT | DATE | DECIMAL | VARCHAR | STRUCT =>
            throw new IllegalArgumentException(s"SimpleFeature TypeDefinition should not have type '$category'")
        }
        typeAndConversion.toSeq.flatMap { case (typ, conversion) =>
          FilterHelper.extractAttributeBounds(filter, prop, binding).values.flatMap(add(prop, _, typ, conversion))
        }
      }

      if (preds.isEmpty) {
        Seq.empty
      } else {
        Seq(preds)
      }
    }

    if (predicates.isEmpty) { None } else {
      val arg = SearchArgumentFactory.newBuilder
      if (predicates.length > 1) {
        arg.startAnd()
      }
      predicates.foreach { preds =>
        if (preds.length == 1) {
          preds.head.apply(arg)
        } else {
          arg.startOr()
          preds.foreach(_.apply(arg))
          arg.end()
        }
      }
      if (predicates.length > 1) {
        arg.end()
      }
      // note: column name array does not matter at all
      Some((arg.build, Array.empty))
    }
  }

  private def add(prop: String,
                  bounds: Bounds[_],
                  typ: PredicateLeaf.Type,
                  convert: (Any) => Any): Option[(SearchArgument.Builder) => Unit] = {
    if (bounds.isRange) {
      if (bounds.isBoundedBothSides) {
        // between seems to be endpoint inclusive, so should not have any false negatives regardless of bounds inclusiveness
        Some((arg) => arg.between(prop, typ, convert(bounds.lower.value.get), convert(bounds.upper.value.get)))
      } else if (bounds.isBounded) {
        if (bounds.upper.value.isDefined) {
          if (bounds.upper.inclusive) {
            Some((arg) => arg.lessThanEquals(prop, typ, convert(bounds.upper.value.get)))
          } else {
            Some((arg) => arg.lessThan(prop, typ, convert(bounds.upper.value.get)))
          }
        } else if (bounds.lower.inclusive) {
          Some((arg) => arg.startNot().lessThan(prop, typ, convert(bounds.lower.value.get)).end())
        } else {
          Some((arg) => arg.startNot().lessThanEquals(prop, typ, convert(bounds.lower.value.get)).end())
        }
      } else {
        None
      }
    } else {
      Some((arg) => arg.equals(prop, typ, convert(bounds.lower.value.get)))
    }
  }

  private def addPoint(prop: String, bounds: Geometry): (SearchArgument.Builder) => Unit = {
    val x = OrcFileSystemStorage.geometryXField(prop)
    val y = OrcFileSystemStorage.geometryYField(prop)
    val envelope = bounds.getEnvelopeInternal

    (arg) => {
      arg.startAnd()
      if (envelope.getMinX == envelope.getMaxX) {
        arg.equals(x, PredicateLeaf.Type.FLOAT, envelope.getMinX)
      } else {
        arg.between(x, PredicateLeaf.Type.FLOAT, envelope.getMinX, envelope.getMaxX)
      }
      if (envelope.getMinY == envelope.getMaxY) {
        arg.equals(y, PredicateLeaf.Type.FLOAT, envelope.getMinY)
      } else {
        arg.between(y, PredicateLeaf.Type.FLOAT, envelope.getMinY, envelope.getMaxY)
      }
      arg.end()
    }
  }
}
