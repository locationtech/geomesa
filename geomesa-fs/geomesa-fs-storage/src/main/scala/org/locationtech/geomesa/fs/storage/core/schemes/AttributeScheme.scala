/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.{PartitionSpec, StructLike}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.locationtech.geomesa.fs.storage.core.schemes.AttributeScheme.Bucketing
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey

import scala.reflect.ClassTag

/**
 * Attribute partitioning scheme, supporting optional bucketing
 *
 * @param attribute attribute being partitioned
 * @param index index in the feature type of the attribute
 * @param nullValue null value placeholder (type dependent)
 * @param bucketing optional bucketing
 */
abstract class AttributeScheme[T: ClassTag](val attribute: String, index: Int, nullValue: T, val bucketing: Option[Bucketing[T]])
    extends PartitionScheme {

  override val name: String = {
    val opts = new StringBuilder(s"${AttributeScheme.Name}:attribute=$attribute")
    bucketing.foreach(b => opts.append(':').append(b.encoded))
    opts.toString()
  }

  override val column: String = ColumnName.encode(attribute)

  private val default = PartitionKey(name, toPartition(nullValue))

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val value = feature.getAttribute(index)
    if (value == null) {
      return default
    }
    val partition = toPartition(value.asInstanceOf[T])
    PartitionKey(name, partition)
  }

  override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = bucketing match {
    case None => b.identity(column)
    case Some(bucket) => b.truncate(column, bucket.width)
  }

  override def getPartition(partition: StructLike, i: Int): PartitionKey =
    PartitionKey(name, partition.get(i, implicitly[ClassTag[T]].runtimeClass).toString)

  private def toPartition(value: T): String = {
    bucketing match {
      case None => value.toString
      case Some(b) => b(value).toString
    }
  }
}

object AttributeScheme extends PartitionSchemeFactory {

  import FilterHelper.ff

  val Name = "attribute"

  override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
    val opts = SchemeOpts(scheme)
    if (opts.name != Name) { None } else {
      val attribute = opts.getSingle("attribute").orNull
      require(attribute != null, s"Attribute scheme requires an attribute to be specified with 'attribute=<attribute>'")
      val index = attributeIndex(sft, attribute)
      val binding = sft.getDescriptor(index).getType.getBinding
      require(AttributeIndexKey.encodable(binding), s"Invalid type binding '${binding.getName}' of attribute '$attribute'")

      val width = opts.getSingle("width").map(w => WidthBucketing(w.toInt))
      val divisor = opts.getSingle("divisor").map(_.toInt)
      val scale = opts.getSingle("scale").map(_.toInt)

      val isString = classOf[String].isAssignableFrom(binding)
      val isWholeNumber = binding == classOf[Integer] || binding == classOf[java.lang.Long]
      val isDecimalNumber = binding == classOf[java.lang.Float] || binding == classOf[java.lang.Double]

      if (width.isDefined && !isString) {
        throw new IllegalArgumentException(
          s"'width' option is only supported for String-type attributes, not ${binding.getSimpleName}")
      } else if (divisor.isDefined && !isWholeNumber) {
        throw new IllegalArgumentException(
          s"'divisor' option is only supported for Integer and Long-type attributes, not ${binding.getSimpleName}")
      } else if (scale.isDefined && !isDecimalNumber) {
        throw new IllegalArgumentException(
          s"'scale' option is only supported for Float and Double-type attributes, not ${binding.getSimpleName}")
      }

      if (opts.getMulti("allow").nonEmpty) {
        throw new IllegalArgumentException("`allow` option is no longer supported for attribute schemes")
      } else if (opts.getSingle("default").isDefined) {
        throw new IllegalArgumentException("`default` option is no longer supported for attribute schemes")
      }

      if (isString) {
        width match {
          case None => Some(new StringScheme(attribute, index))
          case Some(w) => Some(new TruncatedStringScheme(attribute, index, w))
        }
      } else if (binding == classOf[Integer]) {
        val bucketing = divisor.map(IntegralBucketing.apply[Int])
        Some(new IntScheme(attribute, index, bucketing))
      } else if (binding == classOf[java.lang.Long]) {
        val bucketing = divisor.map(d => IntegralBucketing(d.toLong))
        Some(new LongScheme(attribute, index, bucketing))
      } else if (binding == classOf[java.lang.Float]) {
        val bucketing = scale.map(FractionalBucketing.apply[Float])
        Some(new FloatScheme(attribute, index, bucketing))
      } else if (binding == classOf[java.lang.Double]) {
        val bucketing = scale.map(FractionalBucketing.apply[Double])
        Some(new DoubleScheme(attribute, index, bucketing))
      } else {
        throw new IllegalArgumentException(
          s"Attribute scheme is not supported for type ${binding.getSimpleName} - " +
            s"supported types are String Integer, Long, Float, and Double")
      }
    }
  }

  /**
   * Bucketing abstraction
   *
   * @tparam T attribute type
   */
  sealed trait Bucketing[T] extends (T => T) {

    /**
     * Encoded option, used to identify this bucketing
     *
     * @return
     */
    def encoded: String

    /**
     * Truncation width
     *
     * @return
     */
    def width: Int
  }

  private case class WidthBucketing(width: Int) extends Bucketing[String] {
    override def apply(value: String): String = value.slice(0, width)
    override def encoded: String = s"width=$width"
  }

  private case class IntegralBucketing[T: Integral](divisor: T) extends Bucketing[T] {
    import Integral.Implicits.infixIntegralOps
    override def apply(value: T): T = value - (((value % divisor) + divisor) % divisor)
    override def encoded: String = s"divisor=$divisor"
    // TODO longs not supported by the java api
    override def width: Int = implicitly[Integral[T]].toInt(divisor)
  }

  // scale here refers to the number of digits to the right of the decimal place that are kept
  private case class FractionalBucketing[T: Fractional](scale: Int) extends Bucketing[T] {
    import Fractional.Implicits.infixFractionalOps
    private val fractional = implicitly[Fractional[T]]
    val scaleT: T = fractional.fromInt(math.pow(10, scale).toInt)
    override def apply(value: T): T = fractional.fromInt(Math.floor((value * scaleT).toDouble).toInt) / scaleT
    override def encoded: String = s"scale=$scale"
    // TODO only decimal types support truncate but not double/float
    override def width: Int = scale
  }

  /**
   * String attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   */
  private class StringScheme(attribute: String, index: Int) extends AttributeScheme[String](attribute, index, "", None) {

    override def getCoveringFilter(partition: PartitionKey): Filter =
      ff.equals(ff.property(attribute), ff.literal(partition.value))

    override def getCoveringExpression(partition: PartitionKey): Expression =
      Expressions.equal[String](column, partition.value)
  }

  /**
   * String attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param maxWidth max width for partition values
   */
  private class TruncatedStringScheme(attribute: String, index: Int, maxWidth: WidthBucketing)
      extends AttributeScheme[String](attribute, index, "", Some(maxWidth)) {

    override def getCoveringFilter(partition: PartitionKey): Filter = {
      val escapes = Seq("""\""" -> """\\""", """%""" -> """\%""", """_""" -> """\_""")
      val escaped =
        escapes.foldLeft(partition.value) { case (s, (literal, replacement)) => s.replaceAllLiterally(literal, replacement) }
      ff.like(ff.property(attribute), escaped + "%", "%", "_", "\\", false)
    }

    override def getCoveringExpression(partition: PartitionKey): Expression =
      Expressions.equal[String](Expressions.truncate[String](column, maxWidth.width), partition.value)
  }

  /**
   * Whole-number attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param divisor divisor for bucketing values
   */
  private abstract class IntegralScheme[T: Integral: ClassTag](
      attribute: String,
      index: Int,
      divisor: Option[IntegralBucketing[T]],
      decoder: String => T
    ) extends AttributeScheme[T](attribute, index, implicitly[Integral[T]].zero, divisor) {

    import Integral.Implicits.infixIntegralOps

    private val integral = implicitly[Integral[T]]

    override def getCoveringFilter(partition: PartitionKey): Filter = {
      val value = decoder(partition.value)
      val attr = ff.property(attribute)
      divisor match {
        case None => ff.equals(attr, ff.literal(value))
        case Some(d) =>
          val lower = ff.literal(value)
          val upper = ff.literal(value + d.divisor)
          ff.and(ff.greaterOrEqual(attr, lower), ff.less(attr, upper))
      }
    }

    override def getCoveringExpression(partition: PartitionKey): Expression = divisor match {
      case None => Expressions.equal[T](column, decoder(partition.value))
      case Some(d) => Expressions.equal[T](Expressions.truncate[T](column, integral.toInt(d.divisor)), decoder(partition.value))
    }
  }

  /**
   * Integer attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param divisor divisor for bucketing values
   */
  private class IntScheme(attribute: String, index: Int, divisor: Option[IntegralBucketing[Int]])
      extends IntegralScheme[Int](attribute, index, divisor, _.toInt)

  /**
   * Long attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param divisor divisor for bucketing values
   */
  private class LongScheme(attribute: String, index: Int, divisor: Option[IntegralBucketing[Long]])
      extends IntegralScheme[Long](attribute, index, divisor, _.toLong)

  /**
   * Fractional number attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param scale scale for bucketing values
   */
  private abstract class FractionalScheme[T: Fractional: ClassTag](
      attribute: String,
      index: Int,
      scale: Option[FractionalBucketing[T]],
      decoder: String => T,
    ) extends AttributeScheme[T](attribute, index, implicitly[Fractional[T]].zero, scale) {

    import FilterHelper.ff

    import Fractional.Implicits.infixFractionalOps

    private val fractional = implicitly[Fractional[T]]
    private val oneBucket: Option[T] = scale.map(s => fractional.one / s.scaleT)

    override def getCoveringFilter(partition: PartitionKey): Filter = {
      val value = decoder(partition.value)
      val attr = ff.property(attribute)
      oneBucket match {
        case None => ff.equals(attr, ff.literal(value))
        case Some(one) =>
          val lower = ff.literal(value)
          val upper = ff.literal(value + one)
          ff.and(ff.greaterOrEqual(attr, lower), ff.less(attr, upper))
      }
    }

    override def getCoveringExpression(partition: PartitionKey): Expression = scale match {
      case None => Expressions.equal[T](column, decoder(partition.value))
      case Some(s) => Expressions.equal[T](Expressions.truncate[T](column, s.scale), decoder(partition.value))
    }
  }

  /**
   * Float attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param scale scale for bucketing values
   */
  private class FloatScheme(attribute: String, index: Int, scale: Option[FractionalBucketing[Float]])
      extends FractionalScheme[Float](attribute, index, scale, _.toFloat)

  /**
   * Double attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param scale scale for bucketing values
   */
  private class DoubleScheme(attribute: String, index: Int, scale: Option[FractionalBucketing[Double]])
      extends FractionalScheme[Double](attribute, index, scale, _.toDouble)
}
