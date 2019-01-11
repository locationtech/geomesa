/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geojson

import java.io.Closeable

import com.github.benmanes.caffeine.cache.Caffeine
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Geometry, Point}
import org.geotools.data.{DataStore, FeatureWriter, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.geojson.geom.GeometryJSON
import org.json4s.native.JsonMethods._
import org.json4s.{JObject, _}
import org.locationtech.geomesa.features.kryo.json.JsonPathParser
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.{PathAttribute, PathElement}
import org.locationtech.geomesa.geojson.query.{GeoJsonQuery, PropertyTransformer}
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.parboiled.errors.ParsingException

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

/**
  * GeoJSON index backed by a GeoMesa data store.
  *
  * Note: data store must be disposed of separately
  *
  * @param ds GeoMesa data store
  */
class GeoJsonGtIndex(ds: DataStore) extends GeoJsonIndex with LazyLogging {

  // TODO GEOMESA-1450 support json-schema validation
  // TODO GEOMESA-1451 optimize serialization for json-schema

  def getTransformer(schema:SimpleFeatureType):PropertyTransformer = {
    val idPath = GeoJsonGtIndex.getIdPath(schema)
    val dtgPath = GeoJsonGtIndex.getDtgPath(schema)
    new GeoMesaIndexPropertyTransformer(idPath, dtgPath)
  }

  override def createIndex(name: String, id: Option[String], dtg: Option[String], points: Boolean): Unit = {
    try {
      // validate json-paths
      id.foreach(JsonPathParser.parse(_, report = true))
      dtg.foreach(JsonPathParser.parse(_, report = true))
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Error parsing paths", e)
    }
    ds.createSchema(SimpleFeatureTypes.createType(name, GeoJsonGtIndex.spec(id, dtg, points)))
  }

  override def deleteIndex(name: String): Unit = ds.removeSchema(name)

  override def add(name: String, json: String): Seq[String] = {
    import org.json4s.native.JsonMethods._

    val schema = ds.getSchema(name)
    if (schema == null) {
      throw new IllegalArgumentException(s"Index $name does not exist - please call 'createIndex'")
    }

    val features = try { GeoJsonGtIndex.parseFeatures(json) } catch {
      case e: IllegalArgumentException => throw e
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid geojson:\n$json", e)
    }

    val ids = ArrayBuffer.empty[String]
    val (getGeom, getId, getDtg) = GeoJsonGtIndex.jsonExtractors(schema)

    val writer = ds.getFeatureWriterAppend(name, Transaction.AUTO_COMMIT)

    features.foreach { feature =>
      try {
        val sf = writer.next()
        sf.setAttribute(0, compact(render(feature)))
        sf.setAttribute(1, getGeom(feature))
        getDtg(feature).foreach(sf.setAttribute(2, _))
        getId(feature).foreach(sf.getUserData.put(Hints.PROVIDED_FID, _))
        writer.write()
        ids.append(sf.getID)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error writing json:\n${Try(compact(render(feature))).getOrElse(feature)}", e)
      }
    }

    writer.close()

    ids
  }

  override def update(name: String, json: String): Unit = {
    val schema = ds.getSchema(name)
    if (schema == null) {
      throw new IllegalArgumentException(s"Index $name does not exist - please call 'createIndex'")
    } else if (!schema.getUserData.containsKey(GeoJsonGtIndex.IdPathKey)) {
      throw new IllegalArgumentException(s"ID path was not specified - please use `update(String, Seq[String], String)`")
    }

    val features = try { GeoJsonGtIndex.parseFeatures(json) } catch {
      case e: IllegalArgumentException => throw e
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid geojson:\n$json", e)
    }

    val (_, getId, _) = GeoJsonGtIndex.jsonExtractors(schema)
    update(schema, features.flatMap(feature => getId(feature).map(_ -> feature)).toMap)
  }

  override def update(name: String, ids: Seq[String], json: String): Unit = {
    val schema = ds.getSchema(name)
    if (schema == null) {
      throw new IllegalArgumentException(s"Index $name does not exist - please call 'createIndex'")
    }

    val features = try { GeoJsonGtIndex.parseFeatures(json) } catch {
      case e: IllegalArgumentException => throw e
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid geojson:\n$json", e)
    }

    if (ids.length != features.length) {
      throw new IllegalArgumentException(s"ID mismatch - ids: ${ids.length}, features: ${features.length}")
    }
    update(schema, ids.zip(features).toMap)
  }

  private def update(schema: SimpleFeatureType, features: Map[String, JObject]): Unit = {
    import org.locationtech.geomesa.filter.ff

    val (getGeom, _, getDtg) = GeoJsonGtIndex.jsonExtractors(schema)
    val filter = ff.id(features.keys.map(ff.featureId).toSeq: _*)

    var writer: FeatureWriter[SimpleFeatureType, SimpleFeature] = null

    try {
      writer = ds.getFeatureWriter(schema.getTypeName, filter, Transaction.AUTO_COMMIT)
      while (writer.hasNext) {
        val sf = writer.next()
        features.get(sf.getID).foreach { feature =>
          sf.setAttribute(0, compact(render(feature)))
          sf.setAttribute(1, getGeom(feature))
          getDtg(feature).foreach(sf.setAttribute(2, _))
          writer.write()
        }
      }
    } finally {
      CloseWithLogging(writer)
    }
  }

  override def delete(name: String, ids: Iterable[String]): Unit = {
    import org.locationtech.geomesa.filter.ff

    var writer: FeatureWriter[SimpleFeatureType, SimpleFeature] = null
    try {
      writer = ds.getFeatureWriter(name, ff.id(ids.map(ff.featureId).toSeq: _*), Transaction.AUTO_COMMIT)
      while (writer.hasNext) {
        writer.next()
        writer.remove()
      }
    } finally {
      CloseWithLogging(writer)
    }
  }

  override def get(name: String, ids: Iterable[String], transform: Map[String, String]): Iterator[String] with Closeable = {
    import org.locationtech.geomesa.filter.ff

    val schema = ds.getSchema(name)
    if (schema == null) {
      throw new IllegalArgumentException(s"Index $name does not exist - please call 'createIndex'")
    }

    val paths = try { transform.mapValues(JsonPathParser.parse(_)) } catch {
      case e: ParsingException => throw new IllegalArgumentException("Invalid attribute json-paths", e)
    }

    val filter = ff.id(ids.map(ff.featureId).toSeq: _*)
    val features = SelfClosingIterator(ds.getFeatureReader(new Query(name, filter), Transaction.AUTO_COMMIT))
    val results = features.map(_.getAttribute(0).asInstanceOf[String])

    jsonTransform(results, paths)
  }

  override def query(name: String, query: String, transform: Map[String, String]): Iterator[String] with Closeable = {
    val schema = ds.getSchema(name)
    if (schema == null) {
      throw new IllegalArgumentException(s"Index $name does not exist - please call 'createIndex'")
    }

    val filter = try { GeoJsonQuery(query).toFilter(getTransformer(schema)) } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Invalid query syntax", e)
    }
    val paths = try { transform.mapValues(JsonPathParser.parse(_)) } catch {
      case e: ParsingException => throw new IllegalArgumentException("Invalid attribute json-paths", e)
    }

    val features = SelfClosingIterator(ds.getFeatureReader(new Query(name, filter), Transaction.AUTO_COMMIT))
    val results = features.map(_.getAttribute(0).asInstanceOf[String])

    jsonTransform(results, paths)
  }

  private def jsonTransform(features: CloseableIterator[String],
                            transform: Map[String, Seq[PathElement]]): Iterator[String] with Closeable = {
    if (transform.isEmpty) { features } else {
      // recursively construct a json object
      def toObj(path: Seq[String], value: JValue): JValue = {
        if (path.isEmpty) { value } else {
          JObject((path.head, toObj(path.tail, value)))
        }
      }
      val splitPaths = transform.map { case (k, p) => (k.split('.'), p) }
      features.map { j =>
        val obj = parse(j).asInstanceOf[JObject]
        val elems = splitPaths.flatMap { case (key, path) =>
          GeoJsonGtIndex.evaluatePath(obj, path).map { jvalue => JObject((key.head, toObj(key.tail, jvalue))) }
        }
        // merge each transform result together into one object
        compact(render(elems.reduceLeftOption(_ merge _).getOrElse(JObject())))
      }
    }
  }
}

object GeoJsonGtIndex {

  val IdPathKey  = s"${SimpleFeatureTypes.InternalConfigs.GEOMESA_PREFIX}json.id"
  val DtgPathKey = s"${SimpleFeatureTypes.InternalConfigs.GEOMESA_PREFIX}json.dtg"

  private type ExtractGeometry = (JObject) => Geometry
  private type ExtractId = (JObject) => Option[String]
  private type ExtractDate = (JObject) => Option[AnyRef]
  private type JsonExtractors = (ExtractGeometry, ExtractId, ExtractDate)

  private val jsonGeometry = new GeometryJSON()

  private val extractorCache = Caffeine.newBuilder().build[String, JsonExtractors]()

  /**
    * Gets a simple feature type spec. NOTE: the following attribute indices are assumed:
    *   0: json
    *   1: geometry
    *   2: date (if defined)
    *
    * @param idPath json-path to select a feature ID from an input geojson feature
    * @param dtgPath json-path to select a date from an input geojson feature
    *                dates must be convertable to java.lang.Date by geotools Converters
    * @param points store points (or centroids) of input geojson, or store complex geometries with extents
    * @return simple feature type spec
    */
  private def spec(idPath: Option[String], dtgPath: Option[String], points: Boolean): String = {
    val geomType = if (points) { "Point" } else { "Geometry" }

    val spec = new StringBuilder(s"json:String:json=true,*geom:$geomType:srid=4326")

    if (dtgPath.isDefined) {
      spec.append(",dtg:Date")
    }

    val mixedGeoms = if (points) { Seq.empty } else { Seq(s"${SimpleFeatureTypes.Configs.MIXED_GEOMETRIES}='true'") }
    val id = idPath.map(p => s"$IdPathKey='$p'")
    val dtg = dtgPath.map(p => s"$DtgPathKey='$p'")

    val userData = (mixedGeoms ++ id ++ dtg).mkString(";", ",", "")

    spec.append(userData)

    spec.toString
  }

  /**
    * Creates functions for extracting data from parsed geojson features
    *
    * @param schema simple feature type
    * @return (method to get geometry, method to get feature ID, method to get date)
    */
  private def jsonExtractors(schema: SimpleFeatureType): JsonExtractors = {
    def load(): JsonExtractors = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val getGeom: (JObject) => Geometry =
        if (schema.isPoints) {
          (feature) => getGeometry(feature) match {
            case p: Point    => p
            case g: Geometry => g.getCentroid
            case null        => null
          }
        } else {
          getGeometry
        }

      val getId: (JObject) => Option[String] = getIdPath(schema) match {
        case None => (_) => None
        case Some(path) => (feature) => evaluatePathValue(feature, path).map(_.toString)
      }

      val getDtg: (JObject) => Option[AnyRef] = getDtgPath(schema) match {
        case None => (_) => None
        case Some(path) => (feature) => evaluatePathValue(feature, path)
      }

      (getGeom, getId, getDtg)
    }

    val loader = new java.util.function.Function[String, JsonExtractors] {
      override def apply(ignored: String): JsonExtractors = load()
    }

    extractorCache.get(CacheKeyGenerator.cacheKey(schema), loader)
  }

  private def getIdPath(schema: SimpleFeatureType): Option[Seq[PathElement]] =
    Option(schema.getUserData.get(IdPathKey).asInstanceOf[String]).map(JsonPathParser.parse(_))

  private def getDtgPath(schema: SimpleFeatureType): Option[Seq[PathElement]] =
    Option(schema.getUserData.get(DtgPathKey).asInstanceOf[String]).map(JsonPathParser.parse(_))

  /**
    * Parses a geojson string and returns the feature objects. Will accept either
    * a single feature or a feature collection.
    *
    * @param json geojson string to parse
    * @return parsed feature objects
    */
  private def parseFeatures(json: String): Seq[JObject] = {
    import org.json4s._
    import org.json4s.native.JsonMethods._

    val parsed = parse(json) match {
      case j: JObject => j
      case _ => throw new IllegalArgumentException("Invalid input - expected JSON object")
    }

    getByKey(parsed, "type").map(_.values) match {
      case Some("Feature") => Seq(parsed)

      case Some("FeatureCollection") =>
        val features = getByKey(parsed, "features")
        features.collect { case j: JArray => j.arr.asInstanceOf[List[JObject]] }.getOrElse(List.empty)

      case t =>
        throw new IllegalArgumentException(s"Invalid input type '${t.orNull}' - expected [Feature, FeatureCollection]")
    }
  }

  /**
    * Gets the geometry from a geojson feature object
    *
    * @param feature geojson feature object
    * @return geometry, if present
    */
  private def getGeometry(feature: JObject): Geometry =
    getByKey(feature, "geometry").map(g => jsonGeometry.read(compact(render(g)))).orNull

  /**
    * Gets a value from a json object by it's key
    *
    * @param o json object
    * @param key key to lookup
    * @return value if present
    */
  private def getByKey(o: JObject, key: String): Option[JValue] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator
    o.obj.toIterator.collect { case (k, v) if k == key => v }.headOption
  }

  /**
    * Evaluate a json path against a json object
    *
    * @param json parsed json object
    * @param path sequence of parsed json path elements
    * @return matched value, if any
    */
  private def evaluatePathValue(json: JObject, path: Seq[PathElement]): Option[AnyRef] = {
    def renderValue(jval: JValue): AnyRef = jval match {
      case j: JObject => compact(render(j))
      case j: JArray  => j.arr.map(renderValue)
      case j: JValue  => j.values.asInstanceOf[AnyRef]
    }
    evaluatePath(json, path).map(renderValue)
  }

  /**
    * Evaluate a json path against a json object
    *
    * @param json parsed json object
    * @param path sequence of parsed json path elements
    * @return matched element, if any
    */
  private def evaluatePath(json: JObject, path: Seq[PathElement]): Option[JValue] = {
    import org.locationtech.geomesa.features.kryo.json.JsonPathParser._

    var selected: Seq[JValue] = Seq(json)

    path.foreach {
      case PathAttribute(name, _) =>
        selected = selected.flatMap {
          case j: JObject => j.obj.collect { case (n, v) if n == name => v }
          case _ => Seq.empty
        }
      case PathIndex(index: Int) =>
        selected = selected.flatMap {
          case j: JArray if j.arr.length > index => Seq(j.arr(index))
          case _ => Seq.empty
        }
      case PathIndices(indices: Seq[Int]) =>
        selected = selected.flatMap {
          case j: JArray => indices.flatMap(i => if (j.arr.length > i) Option(j.arr(i)) else None)
          case _ => Seq.empty
        }
      case PathAttributeWildCard =>
        selected = selected.flatMap {
          case j: JArray => j.arr
          case _ => Seq.empty
        }
      case PathIndexWildCard =>
        selected = selected.flatMap {
          case j: JObject => j.obj.map(_._2)
          case _ => Seq.empty
        }
      case PathFunction(function) => throw new NotImplementedError("Path functions not implemented")
      case PathDeepScan => throw new NotImplementedError("Deep scan not implemented")
    }

    selected.headOption
  }
}
