/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.feature.AttributeTypeBuilder
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{TimePeriod, XZSFC}
import org.locationtech.geomesa.utils.conf.{FeatureExpiration, IndexId}
import org.locationtech.geomesa.utils.geometry.GeometryPrecision
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{Configs, InternalConfigs}
import org.locationtech.geomesa.utils.index.Cardinality._
import org.locationtech.geomesa.utils.index.VisibilityLevel.VisibilityLevel
import org.locationtech.geomesa.utils.index.{Cardinality, VisibilityLevel}
import org.locationtech.jts.geom._

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
import scala.util.Try

object Conversions {

  implicit class RichGeometry(val geom: Geometry) extends AnyVal {
    def bufferMeters(meters: Double): Geometry = geom.buffer(distanceDegrees(meters))
    def distanceDegrees(meters: Double): Double = GeometryUtils.distanceDegrees(geom, meters)._2
    def safeCentroid(): Point = {
      val centroid = geom.getCentroid
      if (java.lang.Double.isNaN(centroid.getCoordinate.x) || java.lang.Double.isNaN(centroid.getCoordinate.y)) {
        geom.getEnvelope.getCentroid
      } else {
        centroid
      }
    }
  }

  implicit class RichSimpleFeature(val sf: SimpleFeature) extends AnyVal {

    /**
      * Gets the feature ID as a parsed UUID consisting of (msb, lsb). Caches the bits
      * in the user data for retrieval.
      *
      * Note: this method assumes that the feature ID is a UUID - should first check this
      * with `sft.isUuid`
      *
      * @return (most significant bits, least significant bits)
      */
    def getUuid: (Long, Long) = {
      var bits: (Long, Long) = sf.getUserData.get("uuid").asInstanceOf[(Long, Long)]
      if (bits == null) {
        val uuid = UUID.fromString(sf.getID)
        bits = (uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
        sf.getUserData.put("uuid", bits)
      }
      bits
    }

    /**
      * Cache a parsed uuid for later lookup with `getUuid`
      *
      * @param uuid (most significant bits, least significant bits)
      */
    def cacheUuid(uuid: (Long, Long)): Unit = sf.getUserData.put("uuid", uuid)
  }
}

object PrimitiveConversions {

  trait Conversion[T] {
    def convert(value: AnyRef): T
  }

  implicit object ConvertToBoolean extends ConvertToBoolean

  trait ConvertToBoolean extends Conversion[Boolean] {
    override def convert(value: AnyRef): Boolean = value match {
      case v: String => v.equalsIgnoreCase("true")
      case v: java.lang.Boolean => v.booleanValue
      case _ => throw new IllegalArgumentException(s"Input $value is not a Boolean type")
    }
  }

  implicit object ConvertToDouble extends ConvertToDouble

  trait ConvertToDouble extends Conversion[Double] {
    override def convert(value: AnyRef): Double = value match {
      case v: String => v.toDouble
      case v: Number => v.doubleValue()
      case _         => throw new IllegalArgumentException(s"Input $value is not a numeric type")
    }
  }

  implicit object ConvertToInt extends ConvertToInt

  trait ConvertToInt extends Conversion[Int] {
    override def convert(value: AnyRef): Int = value match {
      case v: String => v.toInt
      case v: Number => v.intValue()
      case _         => throw new IllegalArgumentException(s"Input $value is not a numeric type")
    }
  }

  implicit object ConvertToShort extends ConvertToShort

  trait ConvertToShort extends Conversion[Short] {
    override def convert(value: AnyRef): Short = value match {
      case v: String => v.toShort
      case v: Number => v.shortValue()
      case _         => throw new IllegalArgumentException(s"Input $value is not a numeric type")
    }
  }

  implicit object ConvertToString extends ConvertToString

  trait ConvertToString extends Conversion[String] {
    override def convert(value: AnyRef): String = value match {
      case v: String => v
      case v         => v.toString
    }
  }
}

trait Conversions {

  import PrimitiveConversions._

  protected def boolean(value: AnyRef, default: Boolean = false): Boolean =
    Try(ConvertToBoolean.convert(value)).getOrElse(default)

  protected def double(value: AnyRef): Double = ConvertToDouble.convert(value)

  protected def int(value: AnyRef): Int = ConvertToInt.convert(value)

  protected def short(value: AnyRef): Short = ConvertToShort.convert(value)
}

/**
 * Contains GeoMesa specific attribute descriptor information
 */
object RichAttributeDescriptors extends Conversions {

  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs._
  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._

  // noinspection AccessorLikeMethodIsEmptyParen
  implicit class RichAttributeDescriptor(val ad: AttributeDescriptor) extends AnyVal {

    def isKeepStats(): Boolean = boolean(ad.getUserData.get(OptStats))

    def isIndexValue(): Boolean = boolean(ad.getUserData.get(OptIndexValue))

    def getColumnGroups(): Set[String] =
      Option(ad.getUserData.get(OptColumnGroups).asInstanceOf[String]).map(_.split(",").toSet).getOrElse(Set.empty)

    def getCardinality(): Cardinality =
      Option(ad.getUserData.get(OptCardinality).asInstanceOf[String])
          .flatMap(c => Try(Cardinality.withName(c)).toOption).getOrElse(Cardinality.UNKNOWN)

    def isJson(): Boolean = boolean(ad.getUserData.get(OptJson))

    def getListType(): Class[_] = tryClass(ad.getUserData.get(UserDataListType).asInstanceOf[String])

    def getMapTypes(): (Class[_], Class[_]) =
      (tryClass(ad.getUserData.get(UserDataMapKeyType)), tryClass(ad.getUserData.get(UserDataMapValueType)))

    private def tryClass(value: AnyRef): Class[_] = Try(Class.forName(value.asInstanceOf[String])).getOrElse(null)

    def isList: Boolean = classOf[java.util.List[_]].isAssignableFrom(ad.getType.getBinding)
    def isMap: Boolean = classOf[java.util.Map[_, _]].isAssignableFrom(ad.getType.getBinding)
    def isMultiValued: Boolean = isList || isMap

    def getPrecision: GeometryPrecision = {
      Option(ad.getUserData.get(OptPrecision).asInstanceOf[String]).map(_.split(',')) match {
        case None => GeometryPrecision.FullPrecision
        case Some(Array(xy)) => GeometryPrecision.TwkbPrecision(xy.toByte)
        case Some(Array(xy, z)) => GeometryPrecision.TwkbPrecision(xy.toByte, z.toByte)
        case Some(Array(xy, z, m)) => GeometryPrecision.TwkbPrecision(xy.toByte, z.toByte, m.toByte)
        case Some(p) => throw new IllegalArgumentException(s"Invalid geometry precision: ${p.mkString(",")}")
      }
    }
  }

  implicit class RichAttributeTypeBuilder(val builder: AttributeTypeBuilder) extends AnyVal {

    def indexValue(indexValue: Boolean): AttributeTypeBuilder = builder.userData(OptIndexValue, indexValue)

    def cardinality(cardinality: Cardinality): AttributeTypeBuilder =
      builder.userData(OptCardinality, cardinality.toString)

    def collectionType(typ: Class[_]): AttributeTypeBuilder = builder.userData(UserDataListType, typ)
  }
}

object RichSimpleFeatureType extends Conversions {

  // in general we store everything as strings so that it's easy to pass to accumulo iterators
  implicit class RichSimpleFeatureType(val sft: SimpleFeatureType) extends AnyVal {

    import SimpleFeatureTypes.Configs._
    import SimpleFeatureTypes.InternalConfigs._

    import scala.collection.JavaConverters._

    def getGeomField: String = {
      val gd = sft.getGeometryDescriptor
      if (gd == null) { null } else { gd.getLocalName }
    }
    def getGeomIndex: Int = Option(getGeomField).map(sft.indexOf).getOrElse(-1)

    def getDtgField: Option[String] = userData[String](DefaultDtgField)
    def getDtgIndex: Option[Int] = getDtgField.map(sft.indexOf).filter(_ != -1)
    def clearDtgField(): Unit = sft.getUserData.remove(DefaultDtgField)
    def setDtgField(dtg: String): Unit = {
      val descriptor = sft.getDescriptor(dtg)
      require(descriptor != null && classOf[Date].isAssignableFrom(descriptor.getType.getBinding),
        s"Invalid date field '$dtg' for schema $sft")
      sft.getUserData.put(DefaultDtgField, dtg)
    }

    def statsEnabled: Boolean = boolean(sft.getUserData.get(StatsEnabled), default = true)
    def setStatsEnabled(enabled: Boolean): Unit = sft.getUserData.put(StatsEnabled, enabled.toString)

    def isLogicalTime: Boolean = boolean(sft.getUserData.get(TableLogicalTime), default = true)

    def isPoints: Boolean = {
      val gd = sft.getGeometryDescriptor
      gd != null && gd.getType.getBinding == classOf[Point]
    }
    def nonPoints: Boolean = {
      val gd = sft.getGeometryDescriptor
      gd != null && gd.getType.getBinding != classOf[Point]
    }
    def isLines: Boolean = {
      val gd = sft.getGeometryDescriptor
      gd != null && gd.getType.getBinding == classOf[LineString]
    }

    def getVisibilityLevel: VisibilityLevel = userData[String](IndexVisibilityLevel) match {
      case None        => VisibilityLevel.Feature
      case Some(level) => VisibilityLevel.withName(level.toLowerCase)
    }

    def isVisibilityRequired: Boolean = boolean(sft.getUserData.get(RequireVisibility), default = false)

    def getZ3Interval: TimePeriod = userData[String](IndexZ3Interval) match {
      case None    => TimePeriod.Week
      case Some(i) => TimePeriod.withName(i.toLowerCase)
    }

    def getS3Interval: TimePeriod = userData[String](IndexS3Interval) match {
      case None    => TimePeriod.Week
      case Some(i) => TimePeriod.withName(i.toLowerCase)
    }

    def getXZPrecision: Short = userData(IndexXzPrecision).map(short).getOrElse(XZSFC.DefaultPrecision)

    // note: defaults to false now
    @deprecated("table sharing no longer supported")
    def isTableSharing: Boolean = userData[String](TableSharing).exists(_.toBoolean)
    @deprecated("table sharing no longer supported")
    def getTableSharingPrefix: String = userData[String](TableSharingPrefix).getOrElse("")

    // noinspection ScalaDeprecation
    @deprecated("table sharing no longer supported")
    def getTableSharingBytes: Array[Byte] = if (sft.isTableSharing) {
      sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
    } else {
      Array.empty[Byte]
    }

    def setCompression(c: String): Unit = sft.getUserData.put(TableCompressionType, c)
    def getCompression: Option[String] = {
      userData[String](TableCompressionType).orElse {
        // check deprecated 'enabled' config, which defaults to 'gz'
        userData[String]("geomesa.table.compression.enabled").collect { case e if e.toBoolean => "gz" }
      }
    }

    // gets indices configured for this sft
    def getIndices: Seq[IndexId] =
      userData[String](IndexVersions).map(_.split(",").map(IndexId.apply).toSeq).getOrElse(Seq.empty)
    def setIndices(indices: Seq[IndexId]): Unit =
      sft.getUserData.put(IndexVersions, indices.map(_.encoded).mkString(","))

    def setUserDataPrefixes(prefixes: Seq[String]): Unit = sft.getUserData.put(UserDataPrefix, prefixes.mkString(","))
    def getUserDataPrefixes: Seq[String] =
      (Seq(GeomesaPrefix) ++ userData[String](UserDataPrefix).map(_.split(",")).getOrElse(Array.empty)).toSeq

    def setZ2Shards(splits: Int): Unit = sft.getUserData.put(IndexZ2Shards, splits.toString)
    def getZ2Shards: Int =
      userData(IndexZ2Shards).map(int).getOrElse(userData(IndexZShards).map(int).getOrElse(4))

    def setZ3Shards(splits: Int): Unit = sft.getUserData.put(IndexZ3Shards, splits.toString)
    def getZ3Shards: Int =
      userData(IndexZ3Shards).map(int).getOrElse(userData(IndexZShards).map(int).getOrElse(4))

    def setAttributeShards(splits: Int): Unit = sft.getUserData.put(IndexAttributeShards, splits.toString)
    def getAttributeShards: Int = userData(IndexAttributeShards).map(int).getOrElse(4)

    def setIdShards(splits: Int): Unit = sft.getUserData.put(IndexIdShards, splits.toString)
    def getIdShards: Int = userData(IndexIdShards).map(int).getOrElse(4)

    def isUuid: Boolean = boolean(sft.getUserData.get(FidsAreUuids))
    def isUuidEncoded: Boolean = isUuid && boolean(sft.getUserData.get(FidsAreUuidEncoded), default = true)

    def setFeatureExpiration(expiration: FeatureExpiration): Unit = {
      val org.locationtech.geomesa.utils.conf.FeatureExpiration(string) = expiration
      sft.getUserData.put(Configs.FeatureExpiration, string)
    }
    def getFeatureExpiration: Option[FeatureExpiration] =
      userData[String](Configs.FeatureExpiration).map(org.locationtech.geomesa.utils.conf.FeatureExpiration.apply(sft, _))
    def isFeatureExpirationEnabled: Boolean = sft.getUserData.containsKey(Configs.FeatureExpiration)

    def isTemporalPriority: Boolean = boolean(sft.getUserData.get(TemporalPriority))

    def isPartitioned: Boolean = sft.getUserData.containsKey(Configs.TablePartitioning)

    def getTableProps: Map[String, String] = {
      val key = (if (sft.isPartitioned) { InternalConfigs.PartitionTableProps } else { Configs.TableProps }) + "."
      val  props = sft.getUserData.asScala.collect {
        case (k: String, v: String) if k.startsWith(key) => k.substring(key.length) -> v
      }
      props.toMap
    }

    def getTablePrefix(indexName: String): Option[String] = {
      val key = if (isPartitioned) { InternalConfigs.PartitionTablePrefix } else { Configs.IndexTablePrefix }
      userData[String](s"$key.$indexName").orElse(userData[String](key))
    }

    def getQueryInterceptors: Seq[String] = userData[String](QueryInterceptors).toSeq.flatMap(_.split(","))

    def getKeywords: Set[String] =
      userData[String](Keywords).map(_.split(KeywordsDelimiter).toSet).getOrElse(Set.empty)

    def addKeywords(keywords: Set[String]): Unit =
      sft.getUserData.put(Keywords, getKeywords.union(keywords).mkString(KeywordsDelimiter))

    def removeKeywords(keywords: Set[String]): Unit =
      sft.getUserData.put(Keywords, getKeywords.diff(keywords).mkString(KeywordsDelimiter))

    def userData[T](key: AnyRef): Option[T] = Option(sft.getUserData.get(key).asInstanceOf[T])
  }
}
