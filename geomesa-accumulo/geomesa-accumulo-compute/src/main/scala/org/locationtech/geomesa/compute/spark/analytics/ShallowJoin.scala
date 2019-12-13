/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.compute.spark.analytics

import org.locationtech.jts.geom.Geometry
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, _}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.compute.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.{SchemaBuilder, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

@deprecated
object ShallowJoin {
  val countriesDsParams = Map(
    "instanceId" -> "mycloud",
    "zookeepers" -> "zoo1,zoo2,zoo3",
    "user"       -> "user",
    "password"   -> "password",
    "tableName"  -> "countries")

  val gdeltDsParams = Map(
    "instanceId" -> "mycloud",
    "zookeepers" -> "zoo1,zoo2,zoo3",
    "user"       -> "user",
    "password"   -> "password",
    "tableName"  -> "gdelt")

  val countriesDs = DataStoreFinder.getDataStore(countriesDsParams).asInstanceOf[AccumuloDataStore]
  val gdeltDs = DataStoreFinder.getDataStore(gdeltDsParams).asInstanceOf[AccumuloDataStore]

  def main(args: Array[String]) {

    val sc = new SparkContext(GeoMesaSpark.init(new SparkConf(true), countriesDs))
    GeoMesaSparkKryoRegistrator.register(gdeltDs)

    val countriesRdd: RDD[SimpleFeature] = GeoMesaSpark.rdd(new Configuration(), sc, countriesDsParams, new Query("states"))
    val gdeltRdd: RDD[SimpleFeature] = GeoMesaSpark.rdd(new Configuration(), sc, gdeltDsParams, new Query("gdelt"))

    val aggregated = shallowJoin(sc, countriesRdd, gdeltRdd, "STATE_NAME")

    aggregated.collect.foreach {println}

    countriesDs.dispose()
    gdeltDs.dispose()

  }

  def shallowJoin(sc: SparkContext, coveringSet: RDD[SimpleFeature], data: RDD[SimpleFeature], key: String): RDD[SimpleFeature] = {
    // Broadcast sfts to executors
    GeoMesaSparkKryoRegistrator.broadcast(data)

    // Broadcast covering set to executors
    val broadcastedCover = sc.broadcast(coveringSet.collect)

    // Key data by cover name
    val keyedData = data.mapPartitions { iter =>
      import org.locationtech.geomesa.utils.geotools.Conversions._

      iter.flatMap { sf =>
        // Iterate over covers until a match is found
        val it = broadcastedCover.value.iterator
        var container: Option[String] = None

        while (it.hasNext) {
          val cover = it.next()
          // If the cover's polygon contains the feature,
          // or in the case of non-point geoms, if they intersect, set the container
          if (cover.geometry.intersects(sf.geometry)) {
            container = Some(cover.getAttribute(key).asInstanceOf[String])
          }
        }
        // return the found cover as the key
        if (container.isDefined) {
          Some(container.get, sf)
        } else {
          None
        }
      }
    }

    // Get the indices and types of the attributes that can be aggregated and send them to the partitions
    val countableTypes = Seq("Integer", "Long", "Double")
    val typeNames = data.first.getType.getTypes.toIndexedSeq.map{t => t.getBinding.getSimpleName.toString}

    val countableIndices = typeNames.indices.flatMap { index =>
      val featureType = typeNames(index)
      // Only grab countable types, skipping the ID field
      if ((countableTypes contains featureType) && index != 0) {
        Some(index, featureType)
      } else {
        None
      }
    }.toArray
    val countable = sc.broadcast(countableIndices)

    // Create a Simple Feature Type based on what can be aggregated
    val sftBuilder = SchemaBuilder.builder()
    sftBuilder.addString(key)
    sftBuilder.addMultiPolygon("geom")
    sftBuilder.addInt("count")
    val featureProperties = data.first.getProperties.toSeq
    countableIndices.foreach{ case (index, clazz) =>
      val featureName = featureProperties.apply(index).getName
      clazz match {
        case "Integer" => sftBuilder.addInt(s"total_$featureName")
        case "Long" => sftBuilder.addLong(s"total_$featureName")
        case "Double" => sftBuilder.addDouble(s"total_$featureName")
      }
      sftBuilder.addDouble(s"avg_${featureProperties.apply(index).getName}")
    }
    val coverSft = sftBuilder.build("aggregate")

    // Register it with kryo and send it to executors
    GeoMesaSparkKryoRegistrator.register(Seq(coverSft))
    GeoMesaSparkKryoRegistrator.broadcast(keyedData)
    val coverSftBroadcast = sc.broadcast(SimpleFeatureTypes.encodeType(coverSft))

    // Pre-compute known indices and send them to workers
    val stringAttrs = coverSft.getAttributeDescriptors.map(_.getLocalName)
    val countIndex = sc.broadcast(stringAttrs.indexOf("count"))
    // Reduce features by their covering area
    val aggregate = reduceAndAggregate(keyedData, countable, countIndex, coverSftBroadcast)

    // Send a map of cover name -> geom to the executors
    import org.locationtech.geomesa.utils.geotools.Conversions._
    val coverMap: scala.collection.Map[String, Geometry] =
      coveringSet.map{ sf =>
        sf.getAttribute(key).asInstanceOf[String] -> sf.geometry
      }.collectAsMap

    val broadcastedCoverMap = sc.broadcast(coverMap)

    // Compute averages and set cover names and geometries
    aggregate.mapPartitions { iter =>
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

      iter.flatMap{ case (coverName, sf) =>
        if (sf.getType.getTypeName == "aggregate") {
          sf.getProperties.foreach{ prop =>
            val name = prop.getName.toString
            if (name.startsWith("total_")) {
              val count = sf.get[Integer]("count")
              val avg = prop.getValue match {
                case a: Integer => a.toDouble / count
                case a: java.lang.Long => a.toDouble / count
                case a: java.lang.Double => a / count
                case _ => throw new Exception(s"couldn't match $name")
              }
              sf.setAttribute(s"avg_${name.substring(6)}", avg)
            }
          }
          sf.setAttribute(key, coverName)
          sf.setDefaultGeometry(broadcastedCoverMap.value.getOrElse(coverName, null))
          Some(sf)
        } else {
          None
        }
      }
    }
  }

  def reduceAndAggregate(keyedData: RDD[(String, SimpleFeature)],
                         countable: Broadcast[Array[(Int, String)]],
                         countIndex: Broadcast[Int],
                         coverSftBroadcast: Broadcast[String]): RDD[(String, SimpleFeature)] = {

    // Reduce features by their covering area
    val aggregate = keyedData.reduceByKey((featureA, featureB) => {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

      val aggregateSft = SimpleFeatureTypes.createType("aggregate", coverSftBroadcast.value)

      val typeA = featureA.getType.getTypeName
      val typeB = featureB.getType.getTypeName
      // Case: combining two aggregate features
      if (typeA == "aggregate" && typeB == "aggregate") {
        // Combine the "total" properties
        (featureA.getProperties, featureB.getProperties).zipped.foreach((propA, propB) => {
          val name = propA.getName.toString
          if (propA.getName.toString.startsWith("total_") || propA.getName.toString == "count") {
            val sum = (propA.getValue, propB.getValue) match {
              case (a: Integer, b: Integer) => a + b
              case (a: java.lang.Long, b: java.lang.Long) => a + b
              case (a: java.lang.Double, b: java.lang.Double) => a + b
              case _ => throw new Exception("Couldn't match countable type.")
            }
            featureA.setAttribute(propA.getName, sum)
          }
        })
        featureA
        // Case: combining two regular features
      } else if (typeA != "aggregate" && typeB != "aggregate") {
        // Grab each feature's properties
        val featurePropertiesA = featureA.getProperties.toSeq
        val featurePropertiesB = featureB.getProperties.toSeq
        // Create a new aggregate feature to hold the result
        val featureFields = Seq("empty", featureA.geometry) ++ Seq.fill(aggregateSft.getTypes.size - 2)("0")
        val aggregateFeature = ScalaSimpleFeatureFactory.buildFeature(aggregateSft, featureFields, featureA.getID)

        // Loop over the countable properties and sum them for both geonames simple features
        countable.value.foreach { case (index, clazz) =>
          val propA = featurePropertiesA(index)
          val propB = featurePropertiesB(index)
          val valA = if (propA == null) 0 else propA.getValue
          val valB = if (propB == null) 0 else propB.getValue

          // Set the total
          if( propA != null && propB != null) {
            val sum  = (valA, valB) match {
              case (a: Integer, b: Integer) => a + b
              case (a: java.lang.Long, b: java.lang.Long) => a + b
              case (a: java.lang.Double, b: java.lang.Double) => a + b
              case x => throw new Exception(s"Couldn't match countable type. $x")
            }
            aggregateFeature.setAttribute(s"total_${propA.getName.toString}", sum)
          } else {
            val sum = if (valA != null) valA else if (valB != null) valB else 0
            aggregateFeature.setAttribute(s"total_${propB.getName.toString}", sum)
          }
        }
        aggregateFeature.setAttribute(countIndex.value, new Integer(2))
        aggregateFeature
        // Case: combining a mix
      } else {

        // Figure out which feature is which
        val (aggFeature: SimpleFeature, geoFeature: SimpleFeature) = if (typeA == "aggregate" && typeB != "aggregate") {
          (featureA, featureB)
        } else if (typeA != "aggregate" && typeB == "aggregate") {
          (featureB, featureA)
        }

        // Loop over the aggregate feature's properties, adding on the regular feature's properties
        aggFeature.getProperties.foreach { prop =>
          val name = prop.getName.toString
          if (name.startsWith("total_")) {
            val geoProp = geoFeature.getProperty(name.substring(6))
            if (geoProp != null) {
              val sum = (prop.getValue, geoProp.getValue) match {
                case (a: Integer, b: Integer) => a + b
                case (a: java.lang.Long, b: java.lang.Long) => a + b
                case (a: java.lang.Double, b: java.lang.Double) => a + b
                case _ => 0
              }
              aggFeature.setAttribute(name, sum)
            }
          }

        }
        aggFeature.setAttribute(countIndex.value, aggFeature.get[Integer](countIndex.value) + 1)
        aggFeature
      }
    })
    aggregate
  }

}
