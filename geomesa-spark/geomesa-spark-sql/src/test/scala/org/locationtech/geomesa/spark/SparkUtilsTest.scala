/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
=======
>>>>>>> 7181c84cc0 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> eb34170404 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> 61533f9a14 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> d136bd4a2c (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91465b5762 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> b368bb796e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c8d2cfae9c (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 544d6f2353 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
<<<<<<< HEAD
>>>>>>> 0b01e2802b (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
=======
=======
>>>>>>> 019a35a728 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> 25dfeecf10 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> 7f520da00a (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 544d6f235 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 07d3783d19 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
<<<<<<< HEAD
>>>>>>> 7181c84cc0 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
=======
>>>>>>> 019a35a728 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
<<<<<<< HEAD
>>>>>>> eb34170404 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
=======
>>>>>>> 25dfeecf10 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
<<<<<<< HEAD
>>>>>>> 61533f9a14 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> d136bd4a2c (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7f520da00a (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
<<<<<<< HEAD
>>>>>>> 91465b5762 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b368bb796e (Merge branch 'feature/postgis-fixes')
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> c8d2cfae9c (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
<<<<<<< HEAD
import org.geotools.api.feature.simple.SimpleFeature
=======
<<<<<<< HEAD
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.sql.{SQLTypes, SparkUtils}
<<<<<<< HEAD
=======
import org.apache.spark.sql.{Row, SQLContext, SQLTypes, SparkSession}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
<<<<<<< HEAD
>>>>>>> 544d6f2353 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> 544d6f235 (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
>>>>>>> 7f520da00a (GEOMESA-3078 Support Bytes, List and Map attribute types in GeoMesa Spark SQL)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.sql.{SQLTypes, SparkUtils}
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.UUID
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SparkUtilsTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  var spark: SparkSession = _
  var sc: SQLContext = _

  val simpleTypeValueMap: Map[String, AnyRef] = Map(
    "Integer" -> Int.box(10),
    "Long" -> Long.box(42),
    "Float" -> Float.box(3.14f),
    "Double" -> Double.box(42.10),
    "String" -> "test_string",
    "Boolean" -> Boolean.box(true),
    "Date" -> new java.util.Date(System.currentTimeMillis()),
    "Bytes" -> Array.fill(10)((Random.nextInt(256) - 128).toByte)
  )

  val geomTypeValueMap: Map[String, AnyRef] = Map(
    "Point" -> FastConverter.convert(
      "POINT(45.0 49.0)",
      classOf[org.locationtech.jts.geom.Point]),
    "LineString" -> FastConverter.convert(
      "LINESTRING(0 2, 2 0, 8 6)", classOf[org.locationtech.jts.geom.LineString]),
    "Polygon" -> FastConverter.convert(
      "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))", classOf[org.locationtech.jts.geom.Polygon]),
    "MultiPoint" -> FastConverter.convert(
      "MULTIPOINT(0 0, 2 2)", classOf[org.locationtech.jts.geom.MultiPoint]),
    "MultiLineString" -> FastConverter.convert(
      "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))", classOf[org.locationtech.jts.geom.MultiLineString]),
    "MultiPolygon" -> FastConverter.convert(
      "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))",
      classOf[org.locationtech.jts.geom.MultiPolygon]),
    "GeometryCollection" -> FastConverter.convert(
      "GEOMETRYCOLLECTION(POINT(45.0 49.0),POINT(45.1 49.1))",
      classOf[org.locationtech.jts.geom.GeometryCollection]),
    "Geometry" -> FastConverter.convert(
      "POINT(45.0 49.0)",
      classOf[org.locationtech.jts.geom.Point])
  )

  val unsupportedTypeValueMap: Map[String, AnyRef] = Map(
    "UUID" -> UUID.randomUUID()
  )

  def validateDataTypeConversions(sf: SimpleFeature): Unit = {
    val sft = sf.getFeatureType
    val featureNames = sft.getAttributeDescriptors.asScala.map(d => d.getLocalName)
    val schema = SparkUtils.createStructType(sft)
    val extractors = SparkUtils.getExtractors(schema.fieldNames, schema)
    val row = SparkUtils.sf2row(schema, sf, extractors)
    validateRow(sf, row)

    // create dataframe to validate correctness of schema
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)
    val row2 = df.collect()(0)
    validateRow(sf, row2)

    // simple feature should preserve its value when converted back from Row
    val sf2 = SparkUtils.rowsToFeatures(sft, schema).apply(row2)
    featureNames.foreach { f =>
      val attrType = sft.getDescriptor(f).getType
      if (attrType.getBinding == classOf[java.util.List[_]]) {
        sf.getAttribute(f).asInstanceOf[java.util.List[_]].toArray mustEqual
          sf2.getAttribute(f).asInstanceOf[java.util.List[_]].toArray
      } else if (attrType.getBinding == classOf[java.util.Map[_, _]]) {
        val sfMap = sf.getAttribute(f).asInstanceOf[java.util.Map[_, _]].asScala
        val sf2Map = sf2.getAttribute(f).asInstanceOf[java.util.Map[_, _]].asScala
        sfMap.size mustEqual sf2Map.size
        sfMap.keys.toArray mustEqual sf2Map.keys.toArray
        sfMap.values.toArray mustEqual sf2Map.values.toArray
      } else {
        sf.getAttribute(f) mustEqual sf2.getAttribute(f)
      }
    }

    // simple feature type should be generated from struct type correctly
    val sft2 = SparkUtils.createFeatureType("created_from_struct_type", schema)
    featureNames.foreach { f =>
      val binding = sft.getDescriptor(f).getType.getBinding
      binding mustEqual sft2.getDescriptor(f).getType.getBinding
      // We cannot check user data for basic or geometry types, since we cannot tell if it is a
      // default field, or what srid the geom field is. Here we just want to make sure that the
      // element type of list field or key/value types of map field is correctly populated.
      if (binding == classOf[java.util.List[_]] || binding == classOf[java.util.Map[_, _]]) {
        sft.getDescriptor(f).getUserData mustEqual sft2.getDescriptor(f).getUserData
      }
    }
  }

  private def validateRow(sf: SimpleFeature, row: Row): Unit = {
    sf.getID mustEqual(row.getAs[String]("__fid__"))
    val sft = sf.getFeatureType
    val featureNames = sft.getAttributeDescriptors.asScala.map(d => d.getLocalName)
    featureNames.foreach { f =>
      val attrType = sft.getDescriptor(f).getType
      if (attrType.getBinding == classOf[java.util.List[_]]) {
        sf.getAttribute(f).asInstanceOf[java.util.List[_]].toArray() mustEqual row.getAs[Seq[_]](f).toArray
      } else if (attrType.getBinding == classOf[java.util.Map[_, _]]) {
        val rowValue = row.getAs[Map[_, _]](f)
        val featureValue = sf.getAttribute(f).asInstanceOf[java.util.Map[_, _]].asScala
        featureValue.size mustEqual rowValue.size
        featureValue.keys.toArray mustEqual rowValue.keys.toArray
        featureValue.values.toArray mustEqual rowValue.values.toArray
      } else {
        sf.getAttribute(f) mustEqual row.getAs[AnyRef](f)
      }
    }
  }

  step {
    spark = SparkSQLTestUtils.createSparkSession()
    sc = spark.sqlContext
    SQLTypes.init(sc)
  }

  "SparkUtils" should {
    "convert simple type to SparkSQL data type correctly" >> {
      val spec = simpleTypeValueMap.keys.map(t => s"f_$t:$t").mkString(",")
      val sft = SimpleFeatureTypes.createType("simple", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      simpleTypeValueMap.foreach { case (t, value) => sf.setAttribute(s"f_$t", value) }
      validateDataTypeConversions(sf)
      ok
    }

    "convert geom type to SparkSQL UDT correctly" >> {
      val spec = geomTypeValueMap.keys.map(t => s"f_$t:$t:srid=4326").mkString(",")
      val sft = SimpleFeatureTypes.createType("geom", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      geomTypeValueMap.foreach { case (t, value) => sf.setAttribute(s"f_$t", value) }
      validateDataTypeConversions(sf)
      ok
    }

    "convert list type to SparkSQL Array type correctly" >> {
      val spec = simpleTypeValueMap.keys.map(t => s"f_$t:List[$t]").mkString(",")
      val sft = SimpleFeatureTypes.createType("complex_list", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      simpleTypeValueMap.foreach { case (t, value) => sf.setAttribute(s"f_$t", List(value)) }
      validateDataTypeConversions(sf)
      ok
    }

    "convert map type to SparkSQL Map type correctly" >> {
      // using byte[] as key of HashMap will cause weird problems
      val keyTypes = simpleTypeValueMap.keys.filter(_ != "Bytes")
      val valueTypes = simpleTypeValueMap.keys

      // make a cross join of keyTypes and valueTypes to generate all possible combinations
      val mapKeyValueTypes = keyTypes.flatMap(keyType => valueTypes.map(valueType => (keyType, valueType)))

      val spec = mapKeyValueTypes.map {
        case (keyType, valueType) => s"f_${keyType}_${valueType}:Map[$keyType, $valueType]"
      }.mkString(",")
      val sft = SimpleFeatureTypes.createType("complex_map", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      mapKeyValueTypes.foreach {
        case (keyType, valueType) =>
          val key = simpleTypeValueMap(keyType)
          val value = simpleTypeValueMap(valueType)
          sf.setAttribute(s"f_${keyType}_${valueType}", Map(key -> value))
      }
      validateDataTypeConversions(sf)
      ok
    }

    "ignore unsupported types" >> {
      val spec = unsupportedTypeValueMap.keys.map(t => s"f_$t:$t").mkString(",")
      val sft = SimpleFeatureTypes.createType("unsupported", spec)
      val sf = new ScalaSimpleFeature(sft, "fake_id")
      unsupportedTypeValueMap.foreach { case (t, value) => sf.setAttribute(s"f_$t", value) }
      val featureNames = sft.getAttributeDescriptors.asScala.map(d => d.getLocalName)
      val schema = SparkUtils.createStructType(sft)
      val extractors = SparkUtils.getExtractors(schema.fieldNames, schema)
      val row = SparkUtils.sf2row(schema, sf, extractors)
      row.length mustEqual 1
      row.getAs[String](0) mustEqual sf.getID
    }

    "ignore complex types containing unsupported types" >> {
      val spec = unsupportedTypeValueMap.keys.flatMap(
        t => Seq(s"list_$t:List[$t]", s"mapKey_$t:Map[$t, String]", s"mapValue_$t:Map[String, $t]")).mkString(",")
      val sft = SimpleFeatureTypes.createType("unsupported_complex", spec)
      val schema = SparkUtils.createStructType(sft)
      schema.fieldNames mustEqual(Array("__fid__"))
    }
  }
}
