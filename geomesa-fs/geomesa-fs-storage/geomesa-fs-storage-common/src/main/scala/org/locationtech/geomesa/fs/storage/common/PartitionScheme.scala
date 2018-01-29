/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.Serializable
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{ZoneOffset, ZonedDateTime}
import java.util
import java.util.Date

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter.FilterHelper.extractGeometries
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.utils.geotools.{GeoMesaParam, GeometryUtils, WholeWorldPolygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

object PartitionOpts {
  val DateTimeFormatOpt = "datetime-format"
  val StepUnitOpt       = "step-unit"
  val StepOpt           = "step"
  val DtgAttribute      = "dtg-attribute"
  val GeomAttribute     = "geom-attribute"
  val Z2Resolution      = "z2-resolution"
  val LeafStorage       = "leaf-storage"

  def parseDateTimeFormat(opts: Map[String, String]): String = {
    val fmtStr = opts(DateTimeFormatOpt)
    if (fmtStr.endsWith("/")) throw new IllegalArgumentException("Format cannot end with a slash")
    fmtStr
  }

  def parseDtgAttr(opts: Map[String, String]): Option[String]  = opts.get(DtgAttribute)
  def parseGeomAttr(opts: Map[String, String]): Option[String] = opts.get(GeomAttribute)
  def parseStepUnit(opts: Map[String, String]): ChronoUnit     = ChronoUnit.valueOf(opts(StepUnitOpt).toUpperCase)
  def parseStep(opts: Map[String, String]): Int                = opts.get(StepOpt).map(_.toInt).getOrElse(1)
  def parseZ2Resolution(opts: Map[String, String]): Int        = opts(Z2Resolution).toInt
  def parseLeafStorage(opts: Map[String, String]): Boolean     = opts.get(LeafStorage).forall(_.toBoolean)
}

object CommonSchemeLoader {
  import DateTimeScheme.Formats._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
  def build(name: String, sft: SimpleFeatureType): PartitionScheme = {
    val schemes = name.toLowerCase.split(PartitionScheme.SchemeSeparator).map {

      case "julian-minute" =>
        DateTimeScheme(JulianMinute, ChronoUnit.MINUTES, 1, sft.getDtgField.get, leafStorage = true)

      case "julian-hourly" =>
        DateTimeScheme(JulianHourly, ChronoUnit.HOURS, 1, sft.getDtgField.get, leafStorage = true)

      case "julian-daily" =>
        DateTimeScheme(JulianDay, ChronoUnit.DAYS, 1, sft.getDtgField.get, leafStorage = true)

      case "minute" =>
        DateTimeScheme(Minute, ChronoUnit.MINUTES, 1, sft.getDtgField.get, leafStorage = true)

      case "hourly" =>
        DateTimeScheme(Hourly, ChronoUnit.HOURS, 1, sft.getDtgField.get, leafStorage = true)

      case "daily" =>
        DateTimeScheme(Daily, ChronoUnit.DAYS, 1, sft.getDtgField.get, leafStorage = true)

      case "weekly" =>
        DateTimeScheme(Weekly, ChronoUnit.WEEKS, 1, sft.getDtgField.get, leafStorage = true)

      case "monthly" =>
        DateTimeScheme(Monthly, ChronoUnit.MONTHS, 1, sft.getDtgField.get, leafStorage = true)

      case z2 if z2.matches("z2-[0-9]+bit") =>
        val bits = "z2-([0-9]+)bit".r("bits").findFirstMatchIn(z2).get.group("bits").toInt
        Z2Scheme(bits, sft.getGeomField, leafStorage = true)

      case _ =>
        throw new IllegalArgumentException(s"Unable to find well known scheme(s) for argument $name")
    }
    if (schemes.length == 1) {
      schemes.head
    } else {
      CompositeScheme(schemes.toSeq)
    }
  }
}

object PartitionScheme {

  val SchemeSeparator = ","

  // Must begin with GeoMesa in order to be persisted
  val PartitionSchemeKey = "geomesa.fs.partition-scheme.config"
  val PartitionOptsPrefix = "fs.partition-scheme.opts."
  val PartitionSchemeParam = new GeoMesaParam[String]("fs.partition-scheme.name", "Partition scheme name")

  def addToSft(sft: SimpleFeatureType, scheme: PartitionScheme): Unit =
    sft.getUserData.put(PartitionSchemeKey, scheme.toString)

  def extractFromSft(sft: SimpleFeatureType): PartitionScheme = {
    if (!sft.getUserData.containsKey(PartitionSchemeKey)) {
      throw new IllegalArgumentException("SFT does not have partition scheme in hints")
    }
    apply(sft, sft.getUserData.get(PartitionSchemeKey).asInstanceOf[String])
  }

  def apply(sft: SimpleFeatureType, dsParams: util.Map[String, Serializable]): PartitionScheme = {
    val pName = PartitionSchemeParam.lookup(dsParams)
    import scala.collection.JavaConversions._
    val pOpts = dsParams.keySet.filter(_.startsWith(PartitionOptsPrefix)).map { opt =>
      opt.replace(PartitionOptsPrefix, "") -> dsParams.get(opt).toString
    }.toMap
    PartitionScheme(sft, pName, pOpts)
  }

  // TODO delegate out, etc. make a loader, etc
  def apply(sft: SimpleFeatureType, pName: String, opts: Map[String, String]): PartitionScheme = {
    import PartitionOpts._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
    val leaf = parseLeafStorage(opts)
    val schemes = pName.split(SchemeSeparator).map {
      case DateTimeScheme.Name =>
        val attr = parseDtgAttr(opts).orElse(sft.getDtgField)
          .getOrElse(throw new IllegalArgumentException("SFT must have a Date attribute"))
        val fmt = parseDateTimeFormat(opts)
        val su = parseStepUnit(opts)
        val s = parseStep(opts)
        DateTimeScheme(fmt, su, s, attr, leaf)

      case Z2Scheme.Name =>
        val geomAttr = parseGeomAttr(opts).orElse(Option(sft.getGeomField))
          .getOrElse(throw new IllegalArgumentException("SFT must have Geometry attribute"))
        val z2Res = parseZ2Resolution(opts)
        Z2Scheme(z2Res, geomAttr, leaf)

      case _ =>
        throw new IllegalArgumentException(s"Unknown scheme name $pName")
    }

    if (schemes.length == 1) {
      schemes.head
    } else {
      CompositeScheme(schemes.toSeq)
    }
  }

  def apply(sft: SimpleFeatureType, conf: Config): PartitionScheme = {
    if (!conf.hasPath("scheme")) throw new IllegalArgumentException("config must have a scheme")
    if (!conf.hasPath("options")) throw new IllegalArgumentException("config must have options for scheme")

    val schemeName = conf.getString("scheme")
    val optConf = conf.getConfig("options")
    val opts = conf.getConfig("options").entrySet().map { e =>
        e.getKey -> optConf.getString(e.getKey)
    }.toMap

    apply(sft, schemeName, opts)
  }

  def toConfig(scheme: PartitionScheme): Config =
    ConfigFactory.empty()
      .withValue("scheme", ConfigValueFactory.fromAnyRef(scheme.name))
      .withValue("options", ConfigValueFactory.fromMap(scheme.getOptions))

  def stringify(schemeName: String, opts: util.Map[String, String]): String = {
    import scala.collection.JavaConverters._
    val conf = ConfigFactory.parseMap(Map(
      "scheme" -> schemeName,
      "options" -> opts).asJava)
    conf.root().render(ConfigRenderOptions.concise)
  }

  def apply(sft: SimpleFeatureType, conf: String): PartitionScheme = {
    apply(sft, ConfigFactory.parseString(conf))
  }
}

case class DateTimeScheme(fmtStr: String,
                          stepUnit: ChronoUnit,
                          step: Int,
                          dtgAttribute: String,
                          leafStorage: Boolean) extends PartitionScheme {

  private val MinDateTime = ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  private val MaxDateTime = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)

  private val fmt = DateTimeFormatter.ofPattern(fmtStr)
  override def getPartitionName(sf: SimpleFeature): String = {
    val instant = sf.getAttribute(dtgAttribute).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    fmt.format(instant)
  }

  override def getCoveringPartitions(f: Filter): java.util.List[String] = {
    val bounds = FilterHelper.extractIntervals(f, dtgAttribute, handleExclusiveBounds = true)
    val intervals = bounds.values.map { b =>
      (b.lower.value.getOrElse(MinDateTime), b.upper.value.getOrElse(MaxDateTime))
    }

    intervals.flatMap { case (start, end) =>
      val count = stepUnit.between(start, end).toInt + 1
      Seq.tabulate(count)(i => fmt.format(start.plus(step * i, stepUnit)))
    }
  }

  // TODO This may not be the best way to calculate max depth...
  // especially if we are going to use other separators
  override def maxDepth(): Int = fmtStr.count(_ == '/')

  override def toString: String = PartitionScheme.stringify(name(), getOptions)

  override def fromString(sft: SimpleFeatureType, s: String): PartitionScheme =
    PartitionScheme(sft, ConfigFactory.parseString(s))

  override def isLeafStorage: Boolean = leafStorage

  override def name(): String = DateTimeScheme.Name

  override def getOptions: java.util.Map[String, String] = {
    import PartitionOpts._

    import scala.collection.JavaConverters._
    Map(
        DtgAttribute      -> dtgAttribute,
        DateTimeFormatOpt -> fmtStr,
        StepUnitOpt       -> stepUnit.toString,
        StepOpt           -> step.toString,
        LeafStorage       -> leafStorage.toString).asJava
  }
}

object DateTimeScheme {

  val Name = "datetime"

  // TODO create enumeration and fix match statement above to match it
  object Formats {
    val JulianDay    = "yyyy/DDD"
    val JulianHourly = "yyyy/DDD/HH"
    val JulianMinute = "yyyy/DDD/HH/mm"

    val Monthly      = "yyyy/MM"
    val Weekly       = "yyyy/ww"
    val Daily        = "yyyy/MM/dd"
    val Hourly       = "yyyy/MM/dd/HH"
    val Minute       = "yyyy/MM/dd/HH/mm"
  }
}

case class Z2Scheme(bits: Int, geomAttribute: String, leafStorage: Boolean) extends PartitionScheme {

  require(bits % 2 == 0, "Resolution must be an even number")

  // note: z2sfc resolution is per dimension
  private val z2 = new Z2SFC(bits / 2)
  private val digits = math.ceil(math.log10(math.pow(2, bits))).toInt

  override def getPartitionName(sf: SimpleFeature): String = {
    val pt = sf.getAttribute(geomAttribute).asInstanceOf[Point]
    val idx = z2.index(pt.getX, pt.getY).z
    idx.formatted(s"%0${digits}d")
  }

  override def getCoveringPartitions(f: Filter): java.util.List[String] = {
    val geometries: FilterValues[Geometry] = {
      // TODO support something other than point geoms
      val extracted = extractGeometries(f, geomAttribute, true)
      if (extracted.nonEmpty) {
        extracted
      } else {
        FilterValues(Seq(WholeWorldPolygon))
      }
    }

    if (geometries.disjoint) {
      return List.empty[String]
    }

    val xy = geometries.values.map(GeometryUtils.bounds)
    val ranges = z2.ranges(xy)
    val enumerations = ranges.flatMap(ir => ir.lower to ir.upper)
    enumerations.map(_.formatted(s"%0${digits}d"))
  }

  override def maxDepth(): Int = 1

  override def toString: String = PartitionScheme.stringify(name(), getOptions)

  override def fromString(sft: SimpleFeatureType, s: String): PartitionScheme =
    PartitionScheme(sft, ConfigFactory.parseString(s))

  override def isLeafStorage: Boolean = leafStorage

  override def name(): String = Z2Scheme.Name

  override def getOptions: util.Map[String, String] = {
    import PartitionOpts._

    import scala.collection.JavaConverters._
    Map(GeomAttribute -> geomAttribute,
        Z2Resolution -> bits.toString,
        LeafStorage -> leafStorage.toString).asJava
  }
}

object Z2Scheme {
  val Name = "z2"
}

case class CompositeScheme(schemes: Seq[PartitionScheme]) extends PartitionScheme {

  require(schemes.lengthCompare(1) > 0, "Must provide at least 2 schemes for a composite scheme")
  require(schemes.map(_.isLeafStorage).distinct.lengthCompare(1) == 0, "All schemes must share the same value for isLeafStorage")

  override def getPartitionName(sf: SimpleFeature): String = schemes.map(_.getPartitionName(sf)).mkString("/")

  override def getCoveringPartitions(f: Filter): util.List[String] =
    schemes.map(_.getCoveringPartitions(f)).reduce((a, b) => for (i <- a; j <-b) yield { s"$i/$j"})

  override def maxDepth(): Int = schemes.map(_.maxDepth()).sum

  override def isLeafStorage: Boolean = schemes.head.isLeafStorage

  override def fromString(sft: SimpleFeatureType, s: String): PartitionScheme =
    PartitionScheme(sft, ConfigFactory.parseString(s))

  override def name(): String = schemes.map(_.name()).mkString(PartitionScheme.SchemeSeparator)

  override def toString: String = PartitionScheme.stringify(name(), getOptions)

  override def getOptions: util.Map[String, String] = schemes.map(_.getOptions).reduceLeft(_ ++ _)
}