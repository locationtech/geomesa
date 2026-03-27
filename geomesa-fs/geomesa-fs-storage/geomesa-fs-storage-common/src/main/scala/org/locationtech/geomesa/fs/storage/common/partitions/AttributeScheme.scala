/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.calrissian.mango.types.{LexiTypeEncoders, TypeEncoder}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionFilter, PartitionRange, RangeBuilder, SinglePartition}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.partitions.AttributeScheme.Bucketing
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey

import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.reflect.ClassTag

abstract class AttributeScheme[T: ClassTag](
    attribute: String,
    index: Int,
    defaultValue: Option[T],
    allowedValues: Seq[T],
    nullValue: T,
    bucketing: Option[Bucketing[T]],
    lexicoder: TypeEncoder[T, String],
  ) extends PartitionScheme {

  private val clas = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
  private val default = toPartition(defaultValue.getOrElse(nullValue))
  private val allowed = if (allowedValues.isEmpty) { Seq.empty } else { (allowedValues.map(toPartition) :+ default).distinct }

  override val name: String = {
    val opts = new StringBuilder(s"${AttributeScheme.Name}:attribute=$attribute")
    bucketing.foreach(b => opts.append(':').append(b.encoded))
    defaultValue.filter(_ != nullValue).foreach(v => opts.append(':').append(s"default=$v"))
    allowedValues.foreach(v => opts.append(':').append(s"allow=$v"))
    opts.toString()
  }

  override def getPartition(feature: SimpleFeature): String = {
    val value = feature.getAttribute(index)
    if (value == null) {
      return default
    }
    val partition = toPartition(value.asInstanceOf[T])
    if (allowed.isEmpty || allowed.contains(partition)) {
      partition
    } else {
      default
    }
  }

  override def getIntersectingPartitions(filter: Filter): Option[Seq[PartitionFilter]] = {
    val bounds = FilterHelper.extractAttributeBounds(filter, attribute, clas)
    if (bounds.isEmpty) {
      None
    } else if (bounds.disjoint) {
      Some(Seq.empty)
    } else {
      val rangeFilter = Some(filter)
      // TODO we can remove some filters based on whether a partition is fully covered or not
      // if (bounds.precise) {
      //   // remove the attribute filter that we've already accounted for in our covered partitions
      //   val coveredFilter = FilterExtractingVisitor(filter, attribute, AttributeScheme.propertyIsEquals _)._2
      //   val simplified = SimplifiedFilter(coveredFilter.getOrElse(Filter.INCLUDE), covered, partial = false)
      //  }
      val builder = new RangeBuilder()
      bounds.values.foreach { bound =>
        if (bound.isEquals) {
          builder += SinglePartition(name, toPartition(bound.lower.value.get))
        } else {
          val lower = bound.lower.value.fold("")(toPartition)
          val upper = bound.upper.value.fold("zzz"/*TODO*/)(encodeUpperBound(_, bound.upper.exclusive))
          builder += PartitionRange(name, lower, upper)
        }
      }
      val all = builder.result()
      val ranges = if (allowed.isEmpty) { all } else {
        allowed.collect { case v if all.exists(_.contains(v)) => SinglePartition(name, v) }
      }
      Some(Seq(PartitionFilter(ranges, rangeFilter)))
    }
  }

  private def toPartition(value: T): String = {
    bucketing match {
      case None => lexicoder.encode(value)
      case Some(b) => lexicoder.encode(b(value))
    }
  }

  private def encodeUpperBound(value: T, exclusive: Boolean): String = {
    val encoded = toPartition(value)
    if (exclusive && bucketing.isEmpty) {
      encoded
    } else {
      encoded + AttributeScheme.ZeroChar
    }
  }
}

object AttributeScheme {

  import FilterHelper.ff

  val Name = "attribute"

  private val ZeroChar = new String(Array[Byte](0), StandardCharsets.UTF_8)

  private val IntEncoder = LexiTypeEncoders.integerEncoder().asInstanceOf[TypeEncoder[Int, String]]
  private val LongEncoder = LexiTypeEncoders.longEncoder().asInstanceOf[TypeEncoder[Long, String]]
  private val FloatEncoder = LexiTypeEncoders.floatEncoder().asInstanceOf[TypeEncoder[Float, String]]
  private val DoubleEncoder = LexiTypeEncoders.doubleEncoder().asInstanceOf[TypeEncoder[Double, String]]

  private object StringEncoder extends TypeEncoder[String, String]() {
    override def getAlias: String = "string"
    override def resolves(): Class[String] = classOf[String]
    override def encode(value: String): String = value.toLowerCase(Locale.US)
    override def decode(value: String): String = value
  }

  class AttributePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      if (opts.name != Name) { None } else {
        val attribute = opts.getSingle("attribute").orNull
        require(attribute != null, s"Attribute scheme requires an attribute to be specified with 'attribute=<attribute>'")
        val index = sft.indexOf(attribute)
        require(index != -1, s"Attribute '$attribute' does not exist in schema '${sft.getTypeName}'")
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

        val allowedValues = opts.getMulti("allow")
        val defaultValue = opts.getSingle("default")
        require(allowedValues.isEmpty || defaultValue.forall(allowedValues.contains),
          "Default partition must be one of the allowed values")

        if (isString) {
          Some(StringScheme(attribute, index, width, defaultValue, allowedValues))
        } else if (binding == classOf[Integer]) {
          Some(IntScheme(attribute, index, divisor.map(IntegralBucketing.apply[Int]), defaultValue.map(_.toInt), allowedValues.map(_.toInt)))
        } else if (binding == classOf[java.lang.Long]) {
          Some(LongScheme(attribute, index, divisor.map(d => IntegralBucketing(d.toLong)), defaultValue.map(_.toLong), allowedValues.map(_.toLong)))
        } else if (binding == classOf[java.lang.Float]) {
          Some(FloatScheme(attribute, index, scale.map(FractionalBucketing.apply[Float]), defaultValue.map(_.toFloat), allowedValues.map(_.toFloat)))
        } else if (binding == classOf[java.lang.Double]) {
          Some(DoubleScheme(attribute, index, scale.map(FractionalBucketing.apply[Double]), defaultValue.map(_.toDouble), allowedValues.map(_.toDouble)))
        } else {
          throw new IllegalArgumentException(
            s"Attribute scheme is not supported for type ${binding.getSimpleName} - " +
              s"supported types are String Integer, Long, Float, and Double")
        }
      }
    }
  }

  /**
   * Bucketing abstraction
   *
   * @tparam T attribute type
   */
  private trait Bucketing[T] extends (T => T) {

    /**
     * Encoded option, used to identify this bucketing
     *
     * @return
     */
    def encoded: String
  }

  private case class WidthBucketing(max: Int) extends Bucketing[String] {
    override def apply(value: String): String = value.slice(0, max)
    override def encoded: String = s"width=$max"
  }

  private case class IntegralBucketing[T: Integral](divisor: T) extends Bucketing[T] {
    import Integral.Implicits.infixIntegralOps
    override def apply(value: T): T = divisor * (value / divisor)
    override def encoded: String = s"divisor=$divisor"
  }

  // scale here refers to the number of digits to the right of the decimal place that are kept
  private case class FractionalBucketing[T: Fractional](scale: Int) extends Bucketing[T] {
    import Fractional.Implicits.infixFractionalOps
    private val fractional = implicitly[Fractional[T]]
    val scaleT: T = fractional.fromInt(math.pow(10, scale).toInt)
    override def apply(value: T): T = fractional.fromInt(Math.floor((value * scaleT).toDouble()).toInt) / scaleT
    override def encoded: String = s"scale=$scale"
  }

  /**
   * Lexicoded attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param maxWidth max width for partition values
   * @param defaultValue default value
   * @param allowedValues list of allowedValues to partition
   */
  private case class StringScheme(
      attribute: String,
      index: Int,
      maxWidth: Option[WidthBucketing],
      defaultValue: Option[String],
      allowedValues: Seq[String],
    ) extends AttributeScheme[String](attribute, index, defaultValue, allowedValues, "", maxWidth, StringEncoder) {

    override def getCoveringFilter(partition: String): Filter = {
      val escaped =
        partition.replaceAllLiterally("""\""", """\\""").replaceAllLiterally("""%""", """\%""").replaceAllLiterally("""_""", """\_""")
      val regex = if (maxWidth.isDefined) { escaped + "%" } else { escaped }
      ff.like(ff.property(attribute), regex, "%", "_", "\\", false)
    }
  }

  /**
   * Lexicoded integer attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param divisor divisor for bucketing values
   * @param defaultValue default value
   * @param allowedValues list of allowedValues to partition
   */
  private abstract class IntegralScheme[T: Integral: ClassTag](
      attribute: String,
      index: Int,
      divisor: Option[IntegralBucketing[T]],
      defaultValue: Option[T],
      allowedValues: Seq[T],
      lexicoder: TypeEncoder[T, String],
    ) extends AttributeScheme[T](attribute, index, defaultValue, allowedValues, implicitly[Integral[T]].zero, divisor, lexicoder) {

    import Integral.Implicits.infixIntegralOps

    override def getCoveringFilter(partition: String): Filter = {
      val value = lexicoder.decode(partition)
      val attr = ff.property(attribute)
      divisor match {
        case None => ff.equals(attr, ff.literal(value))
        case Some(d) =>
          val lower = ff.literal(value)
          val upper = ff.literal(value + d.divisor)
          ff.and(ff.greaterOrEqual(attr, lower), ff.less(attr, upper))
      }
    }
  }

  /**
   * Lexicoded integer attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param divisor divisor for bucketing values
   * @param defaultValue default value
   * @param allowedValues list of allowedValues to partition
   */
  private case class IntScheme(
      attribute: String,
      index: Int,
      divisor: Option[IntegralBucketing[Int]],
      defaultValue: Option[Int],
      allowedValues: Seq[Int],
    ) extends IntegralScheme[Int](attribute, index, divisor, defaultValue, allowedValues, IntEncoder)

  /**
   * Lexicoded integer attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param divisor divisor for bucketing values
   * @param defaultValue default value
   * @param allowedValues list of allowedValues to partition
   */
  private case class LongScheme(
      attribute: String,
      index: Int,
      divisor: Option[IntegralBucketing[Long]],
      defaultValue: Option[Long],
      allowedValues: Seq[Long],
    ) extends IntegralScheme[Long](attribute, index, divisor, defaultValue, allowedValues, LongEncoder)

  /**
   * Lexicoded decimal number attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param scale scale for bucketing values
   * @param defaultValue default value
   * @param allowedValues list of allowedValues to partition
   */
  private abstract class FractionalScheme[T: Fractional: ClassTag](
      attribute: String,
      index: Int,
      scale: Option[FractionalBucketing[T]],
      defaultValue: Option[T],
      allowedValues: Seq[T],
      lexicoder: TypeEncoder[T, String],
    ) extends AttributeScheme[T](attribute, index, defaultValue, allowedValues, implicitly[Fractional[T]].zero, scale, lexicoder) {

    import FilterHelper.ff

    import Fractional.Implicits.infixFractionalOps

    private val oneBucket: Option[T] = scale.map(s => implicitly[Fractional[T]].one / s.scaleT)

    override def getCoveringFilter(partition: String): Filter = {
      val value = lexicoder.decode(partition)
      val attr = ff.property(attribute)
      oneBucket match {
        case None => ff.equals(attr, ff.literal(value))
        case Some(one) =>
          val lower = ff.literal(value)
          val upper = ff.literal(value + one)
          ff.and(ff.greaterOrEqual(attr, lower), ff.less(attr, upper))
      }
    }
  }

  /**
   * Lexicoded float attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param scale scale for bucketing values
   * @param defaultValue default value
   * @param allowedValues list of allowedValues to partition
   */
  private case class FloatScheme(
      attribute: String,
      index: Int,
      scale: Option[FractionalBucketing[Float]],
      defaultValue: Option[Float],
      allowedValues: Seq[Float],
    ) extends FractionalScheme[Float](attribute, index, scale, defaultValue, allowedValues, FloatEncoder)

  /**
   * Lexicoded double attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param scale scale for bucketing values
   * @param defaultValue default value
   * @param allowedValues list of allowedValues to partition
   */
  private case class DoubleScheme(
      attribute: String,
      index: Int,
      scale: Option[FractionalBucketing[Double]],
      defaultValue: Option[Double],
      allowedValues: Seq[Double],
    ) extends FractionalScheme[Double](attribute, index, scale, defaultValue, allowedValues, DoubleEncoder)
}

