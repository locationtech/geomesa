/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import org.locationtech.jts.geom._
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.geometry.DirectPosition2D
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{TimePeriod, XZSFC}
import org.locationtech.geomesa.utils.conf.{IndexId, SemanticVersion}
import org.locationtech.geomesa.utils.geometry.GeometryPrecision
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.geomesa.utils.index.VisibilityLevel.VisibilityLevel
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.locationtech.geomesa.utils.stats.Cardinality._
import org.locationtech.geomesa.utils.stats.IndexCoverage._
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

    @deprecated
    def setIndexCoverage(coverage: IndexCoverage): Unit = ad.getUserData.put(OPT_INDEX, coverage.toString)
    @deprecated("Use AttributeIndex.indexed()")
    def getIndexCoverage(): IndexCoverage = IndexCoverage.NONE

    def setKeepStats(enabled: Boolean): Unit = if (enabled) {
      ad.getUserData.put(OPT_STATS, "true")
    } else {
      ad.getUserData.remove(OPT_STATS)
    }
    def isKeepStats(): Boolean = Option(ad.getUserData.get(OPT_STATS)).contains("true")

    def isIndexValue(): Boolean = Option(ad.getUserData.get(OPT_INDEX_VALUE)).contains("true")

    def getColumnGroups(): Set[String] =
      Option(ad.getUserData.get(OPT_COL_GROUPS).asInstanceOf[String]).map(_.split(",").toSet).getOrElse(Set.empty)

    def setCardinality(cardinality: Cardinality): Unit =
      ad.getUserData.put(OPT_CARDINALITY, cardinality.toString)

    def getCardinality(): Cardinality =
      Option(ad.getUserData.get(OPT_CARDINALITY).asInstanceOf[String])
          .flatMap(c => Try(Cardinality.withName(c)).toOption).getOrElse(Cardinality.UNKNOWN)

    def isJson(): Boolean = Option(ad.getUserData.get(OPT_JSON)).contains("true")

    @deprecated
    def setBinTrackId(opt: Boolean): Unit = {}
    @deprecated
    def isBinTrackId(): Boolean = false

    def setListType(typ: Class[_]): Unit = ad.getUserData.put(USER_DATA_LIST_TYPE, typ.getName)

    def getListType(): Class[_] = tryClass(ad.getUserData.get(USER_DATA_LIST_TYPE).asInstanceOf[String])

    def setMapTypes(keyType: Class[_], valueType: Class[_]): Unit = {
      ad.getUserData.put(USER_DATA_MAP_KEY_TYPE, keyType.getName)
      ad.getUserData.put(USER_DATA_MAP_VALUE_TYPE, valueType.getName)
    }

    def getMapTypes(): (Class[_], Class[_]) =
      (tryClass(ad.getUserData.get(USER_DATA_MAP_KEY_TYPE)), tryClass(ad.getUserData.get(USER_DATA_MAP_VALUE_TYPE)))

    private def tryClass(value: AnyRef): Class[_] = Try(Class.forName(value.asInstanceOf[String])).getOrElse(null)

    @deprecated("Use AttributeIndex.indexed()")
    def isIndexed: Boolean = false

    def isList: Boolean = ad.getUserData.containsKey(USER_DATA_LIST_TYPE)

    def isMap: Boolean =
      ad.getUserData.containsKey(USER_DATA_MAP_KEY_TYPE) && ad.getUserData.containsKey(USER_DATA_MAP_VALUE_TYPE)

    def isMultiValued: Boolean = isList || isMap

    def getPrecision: GeometryPrecision = {
      Option(ad.getUserData.get(OPT_PRECISION).asInstanceOf[String]).map(_.split(',')) match {
        case None => GeometryPrecision.FullPrecision
        case Some(Array(xy)) => GeometryPrecision.TwkbPrecision(xy.toByte)
        case Some(Array(xy, z)) => GeometryPrecision.TwkbPrecision(xy.toByte, z.toByte)
        case Some(Array(xy, z, m)) => GeometryPrecision.TwkbPrecision(xy.toByte, z.toByte, m.toByte)
        case Some(p) => throw new IllegalArgumentException(s"Invalid geometry precision: ${p.mkString(",")}")
      }
    }
  }

  implicit class RichAttributeTypeBuilder(val builder: AttributeTypeBuilder) extends AnyVal {

    def indexCoverage(coverage: IndexCoverage): AttributeTypeBuilder = builder.userData(OPT_INDEX, coverage.toString)

    def indexValue(indexValue: Boolean): AttributeTypeBuilder = builder.userData(OPT_INDEX_VALUE, indexValue)

    def cardinality(cardinality: Cardinality): AttributeTypeBuilder =
      builder.userData(OPT_CARDINALITY, cardinality.toString)

    def collectionType(typ: Class[_]): AttributeTypeBuilder = builder.userData(USER_DATA_LIST_TYPE, typ)

    def mapTypes(keyType: Class[_], valueType: Class[_]): AttributeTypeBuilder =
      builder.userData(USER_DATA_MAP_KEY_TYPE, keyType).userData(USER_DATA_MAP_VALUE_TYPE, valueType)
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

    def getDtgField: Option[String] = userData[String](DEFAULT_DATE_KEY)
    def getDtgIndex: Option[Int] = getDtgField.map(sft.indexOf).filter(_ != -1)
    @deprecated
    def getDtgDescriptor: Option[AttributeDescriptor] = getDtgIndex.map(sft.getDescriptor)
    def clearDtgField(): Unit = sft.getUserData.remove(DEFAULT_DATE_KEY)
    def setDtgField(dtg: String): Unit = {
      val descriptor = sft.getDescriptor(dtg)
      require(descriptor != null && classOf[Date].isAssignableFrom(descriptor.getType.getBinding),
        s"Invalid date field '$dtg' for schema $sft")
      sft.getUserData.put(DEFAULT_DATE_KEY, dtg)
    }

    @deprecated("GeoHash index is not longer supported")
    def getStIndexSchema: String = null
    @deprecated("GeoHash index is not longer supported")
    def setStIndexSchema(schema: String): Unit = {}

    def isLogicalTime: Boolean = userData[String](LOGICAL_TIME_KEY).forall(_.toBoolean)

    @deprecated
    def getBinTrackId: Option[String] = None

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

    def getVisibilityLevel: VisibilityLevel = userData[String](VIS_LEVEL_KEY) match {
      case None        => VisibilityLevel.Feature
      case Some(level) => VisibilityLevel.withName(level.toLowerCase)
    }
    def setVisibilityLevel(vis: VisibilityLevel): Unit = sft.getUserData.put(VIS_LEVEL_KEY, vis.toString)

    def getZ3Interval: TimePeriod = userData[String](Z3_INTERVAL_KEY) match {
      case None    => TimePeriod.Week
      case Some(i) => TimePeriod.withName(i.toLowerCase)
    }
    def setZ3Interval(i: TimePeriod): Unit = sft.getUserData.put(Z3_INTERVAL_KEY, i.toString)

    def getXZPrecision: Short = userData[String](XZ_PRECISION_KEY).map(_.toShort).getOrElse(XZSFC.DefaultPrecision)
    def setXZPrecision(p: Short): Unit = sft.getUserData.put(XZ_PRECISION_KEY, p.toString)

    // note: defaults to false now
    def isTableSharing: Boolean = userData[String](TABLE_SHARING_KEY).exists(_.toBoolean)
    @deprecated("table sharing no longer supported")
    def setTableSharing(sharing: Boolean): Unit = sft.getUserData.put(TABLE_SHARING_KEY, sharing.toString)

    def getTableSharingPrefix: String = userData[String](SHARING_PREFIX_KEY).getOrElse("")
    @deprecated("table sharing no longer supported")
    def setTableSharingPrefix(prefix: String): Unit = sft.getUserData.put(SHARING_PREFIX_KEY, prefix)

    def getTableSharingBytes: Array[Byte] = if (sft.isTableSharing) {
      sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
    } else {
      Array.empty[Byte]
    }

    def setCompression(c: String): Unit = {
      sft.getUserData.put(COMPRESSION_ENABLED, "true")
      sft.getUserData.put(COMPRESSION_TYPE, c)
    }

    // gets indices configured for this sft
    def getIndices: Seq[IndexId] =
      userData[String](INDEX_VERSIONS).map(_.split(",").map(IndexId.apply).toSeq).getOrElse(Seq.empty)
    def setIndices(indices: Seq[IndexId]): Unit =
      sft.getUserData.put(INDEX_VERSIONS, indices.map(_.encoded).mkString(","))

    def setUserDataPrefixes(prefixes: Seq[String]): Unit = sft.getUserData.put(USER_DATA_PREFIX, prefixes.mkString(","))
    def getUserDataPrefixes: Seq[String] =
      Seq(GEOMESA_PREFIX) ++ userData[String](USER_DATA_PREFIX).map(_.split(",")).getOrElse(Array.empty)

    @deprecated("Table splitter key can vary with partitioning scheme")
    def getTableSplitter: Option[Class[_]] = userData[String](TABLE_SPLITTER).map(Class.forName)
    @deprecated("Table splitter options key can vary with partitioning scheme")
    def getTableSplitterOptions: String = userData[String](TABLE_SPLITTER_OPTS).orNull

    def setZShards(splits: Int): Unit = sft.getUserData.put(Z_SPLITS_KEY, splits.toString)
    def getZShards: Int = userData[String](Z_SPLITS_KEY).map(_.toInt).getOrElse(4)

    def setAttributeShards(splits: Int): Unit = sft.getUserData.put(ATTR_SPLITS_KEY, splits.toString)
    def getAttributeShards: Int = userData[String](ATTR_SPLITS_KEY).map(_.toInt).getOrElse(4)

    def setIdShards(splits: Int): Unit = sft.getUserData.put(ID_SPLITS_KEY, splits.toString)
    def getIdShards: Int = userData[String](ID_SPLITS_KEY).map(_.toInt).getOrElse(4)

    def setUuid(uuid: Boolean): Unit = sft.getUserData.put(FID_UUID_KEY, String.valueOf(uuid))
    def isUuid: Boolean = userData[String](FID_UUID_KEY).exists(java.lang.Boolean.parseBoolean)
    def isUuidEncoded: Boolean = isUuid && userData[String](FID_UUID_ENCODED_KEY).forall(java.lang.Boolean.parseBoolean)

    @deprecated
    def setRemoteVersion(version: SemanticVersion): Unit = sft.getUserData.put(REMOTE_VERSION, String.valueOf(version))
    def getRemoteVersion: Option[SemanticVersion] =
      Option(sft.getUserData.get(REMOTE_VERSION).asInstanceOf[String]).map(SemanticVersion.apply)

    def getKeywords: Set[String] =
      userData[String](KEYWORDS_KEY).map(_.split(KEYWORDS_DELIMITER).toSet).getOrElse(Set.empty)

    def addKeywords(keywords: Set[String]): Unit =
      sft.getUserData.put(KEYWORDS_KEY, getKeywords.union(keywords).mkString(KEYWORDS_DELIMITER))

    def removeKeywords(keywords: Set[String]): Unit =
      sft.getUserData.put(KEYWORDS_KEY, getKeywords.diff(keywords).mkString(KEYWORDS_DELIMITER))

    def removeAllKeywords(): Unit = sft.getUserData.remove(KEYWORDS_KEY)

    def userData[T](key: AnyRef): Option[T] = Option(sft.getUserData.get(key).asInstanceOf[T])
  }
}
