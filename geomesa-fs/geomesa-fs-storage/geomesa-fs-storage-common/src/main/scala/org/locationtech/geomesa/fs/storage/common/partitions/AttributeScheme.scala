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
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionRange, RangeBuilder}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionKey
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.partitions.AttributeScheme.Bucketing
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey

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

  override val name: String = {
    val opts = new StringBuilder(s"${AttributeScheme.Name}:attribute=$attribute")
    bucketing.foreach(b => opts.append(':').append(b.encoded))
    defaultValue.filter(_ != nullValue).foreach(v => opts.append(':').append(s"default=$v"))
    allowedValues.foreach(v => opts.append(':').append(s"allow=$v"))
    opts.toString()
  }

  private val clas = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
  private val default = PartitionKey(name, toPartition(defaultValue.getOrElse(nullValue)))
  private val allowed = if (allowedValues.isEmpty) { Seq.empty } else { (allowedValues.map(toPartition) :+ default.value).distinct }

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val value = feature.getAttribute(index)
    if (value == null) {
      return default
    }
    val partition = toPartition(value.asInstanceOf[T])
    if (allowed.isEmpty || allowed.contains(partition)) {
      PartitionKey(name, partition)
    } else {
      default
    }
  }

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = {
    getBounds(filter).map { bounds =>
      val builder = new RangeBuilder()
      bounds.foreach { bound =>
        val lower = bound.lower.value.fold("")(toPartition)
        val upper = exclusiveUpperBound(bound).getOrElse(AttributeScheme.UnboundedUpper)
        builder += PartitionRange(name, lower, upper)
      }
      val all = builder.result()
      if (allowed.isEmpty || all.isEmpty) { all } else {
        allowed.collect { case v if all.exists(_.contains(v)) => PartitionRange(name, v, v + ZeroChar) }
      }
    }
  }

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getBounds(filter).map { bounds =>
      bounds.flatMap { bound =>
        if (!bound.isBoundedBothSides) {
          throw new IllegalArgumentException(s"Filter does not constrain partitions for scheme $name: ${ECQL.toCQL(filter)} ")
        }
        enumerate(bound)
      }
    }
  }

  protected def enumerate(bounds: Bounds[T]): Seq[PartitionKey]

  protected def getBounds(filter: Filter): Option[Seq[Bounds[T]]] = {
    val bounds = FilterHelper.extractAttributeBounds(filter, attribute, clas)
    if (bounds.isEmpty) {
      None
    } else if (bounds.disjoint) {
      Some(Seq.empty)
    } else {
      Some(bounds.values)
    }
  }

  protected def toPartition(value: T): String = {
    bucketing match {
      case None => lexicoder.encode(value)
      case Some(b) => lexicoder.encode(b(value))
    }
  }

  protected def exclusiveUpperBound(bounds: Bounds[T]): Option[String] = {
    bounds.upper.value.map { v =>
      val encoded = toPartition(v)
      if (bounds.upper.exclusive && bucketing.isEmpty) {
        encoded
      } else {
        encoded + ZeroChar
      }
    }
  }
}

object AttributeScheme {

  import FilterHelper.ff

  val Name = "attribute"

  private val UnboundedUpper = Seq.fill(3)(Character.toString(Character.MAX_CODE_POINT)).mkString("")

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

    override protected def enumerate(bounds: Bounds[String]): Seq[PartitionKey] = {

      throw new UnsupportedOperationException("Can't enumerate string attribute scheme")
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

    private val integral = implicitly[Integral[T]]

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

    protected def enumerate(bounds: Bounds[T]): Seq[PartitionKey] = {
      val lower = bounds.lower.value.get
      val delta = bounds.upper.value.get - lower
      divisor.map(_.divisor) match {
        case None => Seq.tabulate(delta.toInt())(i => PartitionKey(name, lexicoder.encode(lower + integral.fromInt(i))))
        case Some(d) => Seq.tabulate((delta / d).toInt())(i => PartitionKey(name, lexicoder.encode(lower + (d * integral.fromInt(i)))))
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

    private val fractional = implicitly[Fractional[T]]
    private val oneBucket: Option[T] = scale.map(s => fractional.one / s.scaleT)

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

    protected def enumerate(bounds: Bounds[T]): Seq[PartitionKey] = {
      scale match {
        case None =>
          val lower = toPartition(bounds.lower.value.get)
          val upper = exclusiveUpperBound(bounds).get
          // since lexicoding is lossy, we can't increment the fractional numbers directly, instead we increment the hex string
          (Iterator.single(lower) ++ Iterator.iterate(lower)(incrementHex).takeWhile(_ < upper)).map(PartitionKey(name, _)).toSeq
        case Some(s) =>
          // here we assume the scale will be enough to increment the encoded string
          val lower = s(bounds.lower.value.get)
          val increment = fractional.one / s.scaleT
          val upper = exclusiveUpperBound(bounds).get
          (Iterator.single(toPartition(lower)) ++ Iterator.iterate(lower)(_ + increment).map(toPartition).takeWhile(v => v < upper)).map(PartitionKey(name, _)).toSeq
      }
    }

    // note that this is tailored to the specific hex encoding used by mango
    // also note this will fail if we get to the max hex value
    private def incrementHex(hex: String): String = {
      (hex.last + 1).toChar match {
        case d   => hex.dropRight(1) + d
        case 'g' => incrementHex(hex.dropRight(1)) + "0"
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
