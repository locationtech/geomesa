/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import org.geotools.feature.AttributeTypeBuilder
import org.geotools.geometry.DirectPosition2D
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{TimePeriod, XZSFC}
import org.locationtech.geomesa.utils.conf.{FeatureExpiration, IndexId, SemanticVersion}
import org.locationtech.geomesa.utils.geometry.GeometryPrecision
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.geomesa.utils.index.VisibilityLevel.VisibilityLevel
import org.locationtech.geomesa.utils.stats.Cardinality
import org.locationtech.geomesa.utils.stats.Cardinality._
import org.locationtech.geomesa.utils.stats.IndexCoverage._
import org.locationtech.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.reflect.ClassTag
import scala.util.Try

object Conversions {

  implicit class RichCoord(val c: Coordinate) extends AnyVal {
    def toPoint2D = new DirectPosition2D(c.x, c.y)
  }

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
    def geometry: Geometry = sf.getDefaultGeometry.asInstanceOf[Geometry]
    def polygon: Polygon = sf.getDefaultGeometry.asInstanceOf[Polygon]
    def point: Point = sf.getDefaultGeometry.asInstanceOf[Point]
    def lineString: LineString = sf.getDefaultGeometry.asInstanceOf[LineString]
    def multiPolygon: MultiPolygon = sf.getDefaultGeometry.asInstanceOf[MultiPolygon]
    def multiPoint: MultiPoint = sf.getDefaultGeometry.asInstanceOf[MultiPoint]
    def multiLineString: MultiLineString = sf.getDefaultGeometry.asInstanceOf[MultiLineString]

    def get[T](i: Int): T = sf.getAttribute(i).asInstanceOf[T]
    def get[T](name: String): T = sf.getAttribute(name).asInstanceOf[T]

    def getNumericDouble(i: Int): Double = getAsDouble(sf.getAttribute(i))
    def getNumericDouble(name: String): Double = getAsDouble(sf.getAttribute(name))

    private def getAsDouble(v: AnyRef): Double = v match {
      case n: Number => n.doubleValue()
      case _         => throw new Exception(s"Input $v is not a numeric type.")
    }

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

    def userData[T](key: AnyRef)(implicit ct: ClassTag[T]): Option[T] =
      Option(sf.getUserData.get(key)).collect { case ct(x) => x }
  }
}

/**
 * Contains GeoMesa specific attribute descriptor information
 */
object RichAttributeDescriptors {

  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs._
  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._

  // noinspection AccessorLikeMethodIsEmptyParen
  implicit class RichAttributeDescriptor(val ad: AttributeDescriptor) extends AnyVal {

    def setKeepStats(enabled: Boolean): Unit = if (enabled) {
      ad.getUserData.put(OptStats, "true")
    } else {
      ad.getUserData.remove(OptStats)
    }
    def isKeepStats(): Boolean = Option(ad.getUserData.get(OptStats)).contains("true")

    def isIndexValue(): Boolean = Option(ad.getUserData.get(OptIndexValue)).contains("true")

    def getColumnGroups(): Set[String] =
      Option(ad.getUserData.get(OptColumnGroups).asInstanceOf[String]).map(_.split(",").toSet).getOrElse(Set.empty)

    def setCardinality(cardinality: Cardinality): Unit =
      ad.getUserData.put(OptCardinality, cardinality.toString)

    def getCardinality(): Cardinality =
      Option(ad.getUserData.get(OptCardinality).asInstanceOf[String])
          .flatMap(c => Try(Cardinality.withName(c)).toOption).getOrElse(Cardinality.UNKNOWN)

    def isJson(): Boolean = Option(ad.getUserData.get(OptJson)).contains("true")

    def setListType(typ: Class[_]): Unit = ad.getUserData.put(UserDataListType, typ.getName)

    def getListType(): Class[_] = tryClass(ad.getUserData.get(UserDataListType).asInstanceOf[String])

    def setMapTypes(keyType: Class[_], valueType: Class[_]): Unit = {
      ad.getUserData.put(UserDataMapKeyType, keyType.getName)
      ad.getUserData.put(UserDataMapValueType, valueType.getName)
    }

    def getMapTypes(): (Class[_], Class[_]) =
      (tryClass(ad.getUserData.get(UserDataMapKeyType)), tryClass(ad.getUserData.get(UserDataMapValueType)))

    private def tryClass(value: AnyRef): Class[_] = Try(Class.forName(value.asInstanceOf[String])).getOrElse(null)

    def isList: Boolean = ad.getUserData.containsKey(UserDataListType)

    def isMap: Boolean =
      ad.getUserData.containsKey(UserDataMapKeyType) && ad.getUserData.containsKey(UserDataMapValueType)

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

    def indexCoverage(coverage: IndexCoverage): AttributeTypeBuilder = builder.userData(OptIndex, coverage.toString)

    def indexValue(indexValue: Boolean): AttributeTypeBuilder = builder.userData(OptIndexValue, indexValue)

    def cardinality(cardinality: Cardinality): AttributeTypeBuilder =
      builder.userData(OptCardinality, cardinality.toString)

    def collectionType(typ: Class[_]): AttributeTypeBuilder = builder.userData(UserDataListType, typ)

    def mapTypes(keyType: Class[_], valueType: Class[_]): AttributeTypeBuilder =
      builder.userData(UserDataMapKeyType, keyType).userData(UserDataMapValueType, valueType)
  }
}

object RichSimpleFeatureType {

  // in general we store everything as strings so that it's easy to pass to accumulo iterators
  implicit class RichSimpleFeatureType(val sft: SimpleFeatureType) extends AnyVal {

    import SimpleFeatureTypes.Configs._
    import SimpleFeatureTypes.InternalConfigs._

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

    def statsEnabled: Boolean =
      Option(sft.getUserData.get(StatsEnabled)).forall(FastConverter.convert(_, classOf[java.lang.Boolean]))
    def setStatsEnabled(enabled: Boolean): Unit = sft.getUserData.put(StatsEnabled, enabled.toString)

    def isLogicalTime: Boolean = userData[String](TableLogicalTime).forall(_.toBoolean)

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
    def setVisibilityLevel(vis: VisibilityLevel): Unit = sft.getUserData.put(IndexVisibilityLevel, vis.toString)

    def getZ3Interval: TimePeriod = userData[String](IndexZ3Interval) match {
      case None    => TimePeriod.Week
      case Some(i) => TimePeriod.withName(i.toLowerCase)
    }
    def setZ3Interval(i: TimePeriod): Unit = sft.getUserData.put(IndexZ3Interval, i.toString)

    def getS3Interval: TimePeriod = userData[String](S3_INTERVAL_KEY) match {
      case None    => TimePeriod.Week
      case Some(i) => TimePeriod.withName(i.toLowerCase)
    }
    def setS3Interval(i: TimePeriod): Unit = sft.getUserData.put(S3_INTERVAL_KEY, i.toString)

    def getXZPrecision: Short = userData[String](IndexXzPrecision).map(_.toShort).getOrElse(XZSFC.DefaultPrecision)
    def setXZPrecision(p: Short): Unit = sft.getUserData.put(IndexXzPrecision, p.toString)

    // note: defaults to false now
    @deprecated("table sharing no longer supported")
    def isTableSharing: Boolean = userData[String](TableSharing).exists(_.toBoolean)
    @deprecated("table sharing no longer supported")
    def getTableSharingPrefix: String = userData[String](TableSharingPrefix).getOrElse("")

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
      Seq(GeomesaPrefix) ++ userData[String](UserDataPrefix).map(_.split(",")).getOrElse(Array.empty)

    def setZShards(splits: Int): Unit = sft.getUserData.put(IndexZShards, splits.toString)
    def getZShards: Int = userData[String](IndexZShards).map(_.toInt).getOrElse(4)

    def setAttributeShards(splits: Int): Unit = sft.getUserData.put(IndexAttributeShards, splits.toString)
    def getAttributeShards: Int = userData[String](IndexAttributeShards).map(_.toInt).getOrElse(4)

    def setIdShards(splits: Int): Unit = sft.getUserData.put(IndexIdShards, splits.toString)
    def getIdShards: Int = userData[String](IndexIdShards).map(_.toInt).getOrElse(4)

    def setUuid(uuid: Boolean): Unit = sft.getUserData.put(FidsAreUuids, String.valueOf(uuid))
    def isUuid: Boolean = userData[String](FidsAreUuids).exists(java.lang.Boolean.parseBoolean)
    def isUuidEncoded: Boolean = isUuid && userData[String](FidsAreUuidEncoded).forall(java.lang.Boolean.parseBoolean)

    def setFeatureExpiration(expiration: FeatureExpiration): Unit = {
      val org.locationtech.geomesa.utils.conf.FeatureExpiration(string) = expiration
      sft.getUserData.put(Configs.FeatureExpiration, string)
    }
    def getFeatureExpiration: Option[FeatureExpiration] =
      userData[String](Configs.FeatureExpiration).map(org.locationtech.geomesa.utils.conf.FeatureExpiration.apply(sft, _))
    def isFeatureExpirationEnabled: Boolean = sft.getUserData.containsKey(Configs.FeatureExpiration)

    def isTemporalPriority: Boolean = userData[String](TemporalPriority).exists(java.lang.Boolean.parseBoolean)

    def getRemoteVersion: Option[SemanticVersion] =
      Option(sft.getUserData.get(RemoteVersion).asInstanceOf[String]).map(SemanticVersion.apply)

    def getKeywords: Set[String] =
      userData[String](Keywords).map(_.split(KeywordsDelimiter).toSet).getOrElse(Set.empty)

    def addKeywords(keywords: Set[String]): Unit =
      sft.getUserData.put(Keywords, getKeywords.union(keywords).mkString(KeywordsDelimiter))

    def removeKeywords(keywords: Set[String]): Unit =
      sft.getUserData.put(Keywords, getKeywords.diff(keywords).mkString(KeywordsDelimiter))

    def removeAllKeywords(): Unit = sft.getUserData.remove(Keywords)

    def userData[T](key: AnyRef): Option[T] = Option(sft.getUserData.get(key).asInstanceOf[T])
  }
}
