/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.expressions.Expression.Operation
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor
import org.apache.iceberg.expressions._
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.{FilterExtractingVisitor, IdExtractingVisitor}
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName, SimpleFeatureSchema}
import org.locationtech.geomesa.fs.storage.core.schemes.{PartitionScheme, SpatialScheme}
import org.locationtech.geomesa.index.strategies.{IdFilterStrategy, SpatialFilterStrategy}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, ObjectType}
import org.locationtech.geomesa.utils.json.JsonPathParser._
import org.locationtech.jts.geom.Point

import java.util.Date
import scala.reflect.ClassTag

object IcebergFilterConverter extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  /**
   * Returns an iceberg expression and a residual GeoTools filter that isn't captured by the expression (if any)
   *
   * @param schema table schema
   * @param filter geotools filter
   * @return
   */
  def apply(schema: SimpleFeatureIcebergSchema, schemes: Seq[PartitionScheme], filter: Filter): ReadFilter = {
    if (filter == Filter.INCLUDE) {
      ReadFilter(Expressions.alwaysTrue(), None, Set.empty)
    } else if (filter == Filter.EXCLUDE) {
      ReadFilter(Expressions.alwaysFalse(), None, Set.empty)
    } else {
      val fid = if (FilterHelper.hasIdFilter(filter)) { Seq(SimpleFeatureSchema.FeatureIdField) } else { Seq.empty }
      val names = (fid ++ FilterHelper.propertyNames(filter)).map(ColumnName.apply)
      names.foldLeft(ReadFilter(Expressions.alwaysTrue(), Some(filter), Set.empty))(reduce(schema, schemes))
    }
  }

  private def reduce(schema: SimpleFeatureIcebergSchema, schemes: Seq[PartitionScheme])(result: ReadFilter, name: ColumnName): ReadFilter = {
    val filter = result.remainder.orNull
    if (filter == null) {
      return result // no more filter to evaluate
    }
    val predicate =
      if (name.column == SimpleFeatureSchema.FeatureIdField) {
        fid(result)
      } else {
        val descriptor = schema.sft.getDescriptor(name.attribute)
        if (descriptor != null) {
          val bindings = ObjectType.selectType(schema.sft.getDescriptor(name.attribute))
          bindings.head match {
            case ObjectType.GEOMETRY => spatial(schema.sft, schemes, name, filter)
            case ObjectType.DATE     => attribute[Date](schema.sft, name, filter, Some(dateToMicros))
            case ObjectType.STRING   => attribute[String](schema.sft, name, filter)
            case ObjectType.INT      => attribute[Integer](schema.sft, name, filter)
            case ObjectType.LONG     => attribute[java.lang.Long](schema.sft, name, filter)
            case ObjectType.FLOAT    => attribute[java.lang.Float](schema.sft, name, filter)
            case ObjectType.DOUBLE   => attribute[java.lang.Double](schema.sft, name, filter)
            case ObjectType.BOOLEAN  => attribute[java.lang.Boolean](schema.sft, name, filter)
            case _ => ReadFilter(Expressions.alwaysTrue(), result.remainder, Set(name.column))
          }
        } else if (name.attribute.startsWith("$")) {
          jsonPath(schema, name.attribute, filter)
        } else {
          throw new IllegalArgumentException(s"Unknown attribute: ${name.attribute}")
        }
      }
    ReadFilter(Expressions.and(predicate.expression, result.expression), predicate.remainder, predicate.columns ++ result.columns)
  }

  private def fid(result: ReadFilter): ReadFilter = {
    val filter = result.remainder.orNull // not null already checked at the call sight
    val fidCol = Set(SimpleFeatureSchema.FeatureIdField)
    val (idFilters, notIds) = IdExtractingVisitor(filter)
    val ids = idFilters.fold(Set.empty[String])(IdFilterStrategy.intersectIdFilters)
    if (ids.isEmpty) {
      logger.warn(s"Detected an ID filter, but could not extract it: ${ECQL.toCQL(filter)}")
      ReadFilter(Expressions.alwaysTrue(), result.remainder, fidCol)
    } else {
      ReadFilter(ids.map(Expressions.equal(SimpleFeatureSchema.FeatureIdField, _)).reduce(Expressions.or), notIds, fidCol)
    }
  }

  private def spatial(sft: SimpleFeatureType, schemes: Seq[PartitionScheme], name: ColumnName, filter: Filter): ReadFilter = {
    val (spatial, nonSpatial) = FilterExtractingVisitor(filter, name.attribute, sft, SpatialFilterStrategy.spatialCheck)
    if (spatial.isEmpty) {
      return ReadFilter(Expressions.alwaysTrue(), Some(filter), Set.empty)
    }

    val bounds = FilterHelper.extractGeometries(spatial.get, name.attribute)
    if (bounds.disjoint) {
      return ReadFilter(Expressions.alwaysFalse(), None, Set.empty)
    }
    val xyBounds = bounds.values.map(GeometryUtils.bounds)
    if (xyBounds.isEmpty) {
      // couldn't extract anything, all evaluation will be client-side against the raw filter
      return ReadFilter(Expressions.alwaysTrue(), Some(filter), Set(name.column))
    }

    // row/group level filter against the bbox field
    val bboxPredicate = {
      val isPoint = sft.getDescriptor(name.attribute).getType.getBinding == classOf[Point]
      val predicates = xyBounds.map { case (xmin, ymin, xmax, ymax) =>
        BoundingBoxField.filterIceberg(name.column, xmin, ymin, xmax, ymax, isPoint)
      }
      predicates.reduce(Expressions.or)
    }
    val bboxCols = ExpressionVisitors.visit(bboxPredicate, ReferenceVisitor)

    // partition level filter for partition pruning
    // assertion - we don't need to include the z col in our read schema as this expression will be removed during manifest scanning
    val spatialScheme = schemes.collectFirst { case s: SpatialScheme if s.attribute == name.attribute => s }
    val partitionPredicate = spatialScheme.fold[Expression](Expressions.alwaysTrue())(_.getCoveringExpression(xyBounds))

    val (remaining, geomCol) = if (bounds.precise) { (nonSpatial, None) } else { (Some(filter), Some(name.column)) }
    val filterCols = bboxCols ++ geomCol

    ReadFilter(Expressions.and(bboxPredicate, partitionPredicate), remaining, filterCols)
  }

  private def attribute[T : ClassTag](
      sft: SimpleFeatureType,
      name: ColumnName,
      filter: Filter,
      transform: Option[T => Any] = None): ReadFilter = {
    val (attribute, nonAttribute) = FilterExtractingVisitor(filter, name.attribute, sft)
    if (attribute.isEmpty) {
      return ReadFilter(Expressions.alwaysTrue(), Some(filter), Set.empty)
    }

    val binding = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    predicate(filter, attribute.get, nonAttribute, name.attribute, name.column, binding, transform)
  }

  /**
   * Builds an iceberg predicate for a scalar attribute or nested field reference
   *
   * @param filter the full filter being reduced (used as the client-side residual when we can't fully push down)
   * @param attributePart the portion of the filter that references our attribute
   * @param nonAttributePart the portion of the filter that doesn't reference our attribute
   * @param attribute the attribute name used in the geotools filter
   * @param column the iceberg reference name (dotted for nested struct fields), including the encoded column
   * @param binding the java type binding of the referenced field
   * @param transform optional transform to convert extracted values into the iceberg storage representation
   * @tparam T value type
   * @return
   */
  private def predicate[T](
      filter: Filter,
      attributePart: Filter,
      nonAttributePart: Option[Filter],
      attribute: String,
      column: String,
      binding: Class[T],
      transform: Option[T => Any]): ReadFilter = {
    val bounds = FilterHelper.extractAttributeBounds(attributePart, attribute, binding)
    if (bounds.disjoint) {
      return ReadFilter(Expressions.alwaysFalse(), None, Set.empty)
    } else if (bounds.isEmpty || bounds.exists(b => !b.isBounded)) {
      // couldn't extract anything, all evaluation will be client-side against the raw filter
      val topLevelColumn = { val sep = column.indexOf('.'); if (sep == -1) { column } else { column.substring(0, sep) } }
      return ReadFilter(Expressions.alwaysTrue(), Some(filter), Set(topLevelColumn))
    }
    val values = transform match {
      case None => bounds.values
      case Some(t) =>
        bounds.values.map { bounds =>
          bounds.copy(bounds.lower.copy(bounds.lower.value.map(t.apply)), bounds.upper.copy(bounds.upper.value.map(t.apply)))
        }
    }
    val filters = values.map { bounds =>
      if (bounds.isEquals) {
        Expressions.equal(column, bounds.lower.value.get)
      } else {
        val lower = bounds.lower.value.map { value =>
          if (bounds.lower.inclusive) { Expressions.greaterThanOrEqual(column, value) } else { Expressions.greaterThan(column, value) }
        }
        val upper = bounds.upper.value.map { value =>
          if (bounds.upper.inclusive) { Expressions.lessThanOrEqual(column, value) } else { Expressions.lessThan(column, value) }
        }
        (lower, upper) match {
          case (Some(lo), Some(hi)) => Expressions.and(lo, hi)
          case (Some(f), None) => f
          case (None, Some(f)) => f
          case (None, None) => throw new IllegalStateException() // shouldn't happen due to checks above
        }
      }
    }
    val result = merge(filters)
    val remaining = if (bounds.precise) { nonAttributePart } else { Some(filter) }
    ReadFilter(result, remaining, Set(column))
  }

  private def jsonPath(schema: SimpleFeatureIcebergSchema, pathString: String, filter: Filter): ReadFilter = {
    val (descriptor, path) = parseJsonPath(pathString, schema.sft)
    val topLevelColumn = ColumnName.encode(descriptor.getLocalName)
    if (descriptor.getJsonSchema().isEmpty) {
      // not a structural type - the field is stored as an opaque variant, so we can't push down against it
      return ReadFilter(Expressions.alwaysTrue(), Some(filter), Set(topLevelColumn))
    }
    val field = schema.schema.findField(topLevelColumn)
    if (field == null || path.function.isDefined) {
      // field shouldn't ever be null, but guard as a sanity check
      // path functions (.min(), .length(), etc) can't be evaluated as an iceberg predicate
      return ReadFilter(Expressions.alwaysTrue(), Some(filter), Set(topLevelColumn))
    }
    // extract just the part of the filter that references our json path - no sft as it won't recognize the path as an attribute
    val (attribute, nonAttribute) = FilterExtractingVisitor(filter, pathString, null: SimpleFeatureType)
    if (attribute.isEmpty) {
      // could not extract any predicates for evaluation
      return ReadFilter(Expressions.alwaysTrue(), Some(filter), Set.empty)
    }

    // navigate the remaining path elements through the nested struct type to find the leaf fields we're filtering on
    navigate(topLevelColumn, field.`type`(), path) match {
      // can't express the json path as an iceberg predicates, evaluate it client-side instead
      case None => ReadFilter(Expressions.alwaysTrue(), Some(filter), Set(topLevelColumn))
      case Some(matchingFields) =>
        if (matchingFields.isEmpty) {
          // the path was evaluated but didn't match any fields
          throw new IllegalArgumentException(
            s"Invalid JSON path - does not match any elements of the structural type $topLevelColumn: $pathString")
        }
        val filters = matchingFields.map { case (column, leafType) =>
          val binding = leafType.typeId() match {
            case TypeID.STRING  => Some(classOf[String])
            case TypeID.INTEGER => Some(classOf[Integer])
            case TypeID.LONG    => Some(classOf[java.lang.Long])
            case TypeID.FLOAT   => Some(classOf[java.lang.Float])
            case TypeID.DOUBLE  => Some(classOf[java.lang.Double])
            case TypeID.BOOLEAN => Some(classOf[java.lang.Boolean])
            // unsupported leaf type (dates, times, uuids, decimals, binary, etc) - evaluate client-side
            // TODO seems like we should be able to support at least dates here?
            case _ => None
          }
          binding match {
            case None => ReadFilter(Expressions.alwaysTrue(), Some(filter), Set(topLevelColumn))
            case Some(b) => predicate(filter, attribute.get, nonAttribute, pathString, column, b, None)
          }
        }
        // if the path matches more than 1 leaf node, combine the filter expressions with ORs
        filters.reduceLeft[ReadFilter] { case (left, right) =>
          // predicate will always return either the full filter or the non-attribute part
          val f = if (left.remainder.contains(filter) || right.remainder.contains(filter)) { Some(filter) } else { nonAttribute }
          ReadFilter(Expressions.or(left.expression, right.expression), f, left.columns ++ right.columns)
        }
    }
  }

  /**
   * Navigates a nested struct type following a json path, returning the nested field reference names and the
   * leaf type if the path points at a scalar field, or None if the path can't be pushed down
   *
   * @param fieldPath dot-delimited path to the current field being evaluated
   * @param fieldType the type of the field the path currently points at
   * @param path the remaining path to navigate
   * @return pairs of nested field names (matching the stored schema) and the leaf primitive type, or None if the path is not supported
   */
  private def navigate(fieldPath: String, fieldType: Type, path: JsonPath): Option[Seq[(String, Type)]] = {
    if (path.isEmpty) {
      return Some(Seq(fieldPath -> fieldType).filter(_._2.isPrimitiveType))
    }
    path.head match {
      case PathAttribute(name, _) if fieldType.isStructType =>
        val nested = fieldType.asStructType().caseInsensitiveField(name)
        if (nested == null) {
          Some(Seq.empty) // path references a field that isn't in our schema
        } else {
          navigate(s"$fieldPath.${nested.name()}", nested.`type`(), path.tail)
        }

      case PathAttributeWildCard if fieldType.isStructType =>
        val children = fieldType.asStructType().fields().asScala.map { nested =>
          navigate(s"$fieldPath.${nested.name()}", nested.`type`(), path.tail)
        }
        if (children.exists(_.isEmpty)) { None } else { Some(children.flatMap(_.get).toSeq) }

      // predicates we can't evaluate as iceberg expressions
      case PathDeepScan => None
      case _: PathIndexRange if fieldType.isListType => None
      case _: PathIndices if fieldType.isListType => None
      case PathIndexWildCard if fieldType.isListType => None
      case _: PathFilter => None

      case _ => Some(Seq.empty) // doesn't match the field type
    }
  }

  /**
   * Merge OR'd filters
   *
   * Detect and re-write "not equals" filters to handle null values
   *
   * FilterHelper methods make "foo != x" comes out as "foo < x OR foo > x"
   *
   * that won't return null values - not normally a concern since we don't index them in key-value dbs,
   * but we want to handle that case here
   *
   * @param filters filters
   * @return combined filter
   */
  private def merge(filters: Seq[Expression]): Expression = {
    lazy val values = filters.collect { case f: UnboundPredicate[_] => (f.ref().name(), f.literal().value()) }.distinct
    if (filters.lengthCompare(2) == 0 &&
      filters.exists(_.op() == Operation.LT) &&
      filters.exists(_.op == Operation.GT) &&
      values.lengthCompare(1) == 0) {
      Expressions.notEqual(values.head._1, values.head._2)
    } else {
      filters.reduce(Expressions.or)
    }
  }

  private def dateToMicros(date: Date): Long = date.getTime * 1000

  /**
   * Parts of a filter
   *
   * @param expression expression to apply to the scan
   * @param remainder remaining cql filter that isn't captured by the expression
   * @param columns column names (encoded) needed for evaluating both the expression and filter
   */
  case class ReadFilter(expression: Expression, remainder: Option[Filter], columns: Set[String])

  /**
   * Visitor to extract column references (names) from an expression
   */
  private object ReferenceVisitor extends ExpressionVisitor[Set[String]] {

    override def alwaysTrue: Set[String] = Set.empty

    override def alwaysFalse: Set[String] = Set.empty

    override def not(result: Set[String]): Set[String] = result

    override def and(leftResult: Set[String], rightResult: Set[String]): Set[String] = leftResult ++ rightResult

    override def or(leftResult: Set[String], rightResult: Set[String]): Set[String] = leftResult ++ rightResult

    override def predicate[T](pred: UnboundPredicate[T]): Set[String] = unwrapTerm(pred.term())

    override def predicate[T](pred: BoundPredicate[T]): Set[String] = unwrapTerm(pred.term())

    private def unwrapTerm(term: Term): Set[String] = term match {
      case ref: Reference[_] => Set(ref.name())
      case t: UnboundTransform[_, _] => Set(t.ref.name())
      case t: BoundTransform[_, _] => Set(t.ref.name())
      case _ => Set.empty
    }
  }
}
