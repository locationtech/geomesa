/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.geotools.api.data.DataStore
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.spark.GeoMesaSpark
import org.locationtech.geomesa.spark.sql.GeoMesaSparkSQL._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithStore

// Spark DataSource for GeoMesa
// enables loading a GeoMesa DataFrame as
// {{
// val df = spark.read
//   .format("geomesa")
//   .option(GM.instanceIdParam.getName, "mycloud")
//   .option(GM.userParam.getName, "user")
//   .option(GM.passwordParam.getName, "password")
//   .option(GM.tableNameParam.getName, "sparksql")
//   .option("geomesa.feature", "chicago")
//   .load()
// }}
class GeoMesaDataSource extends DataSourceRegister
    with RelationProvider with SchemaRelationProvider with CreatableRelationProvider with LazyLogging {

  import scala.collection.JavaConverters._

  override def shortName(): String = "geomesa"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    SQLTypes.init(sqlContext)
    GeoMesaRelation(sqlContext, parameters)
  }

  // JNH: Q: Why doesn't this method have the call to SQLTypes.init(sqlContext)?
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    GeoMesaRelation(sqlContext, parameters, schema)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    val newFeatureName = parameters(GEOMESA_SQL_FEATURE)
    val rddSft = SparkUtils.createFeatureType(newFeatureName, data.schema)

    val storeSft = WithStore[DataStore](parameters) { ds =>
      if (ds.getTypeNames.contains(newFeatureName)) {
        val existing = ds.getSchema(newFeatureName)
        if (!compatible(existing, rddSft)) {
          throw new IllegalStateException(
            "The dataframe is not compatible with the existing schema in the datastore:" +
              s"\n  Dataframe schema: ${SimpleFeatureTypes.encodeType(rddSft)}" +
              s"\n  Datastore schema: ${SimpleFeatureTypes.encodeType(existing)}")
        }
        existing
      } else {
        rddSft.getUserData.put("override.reserved.words", java.lang.Boolean.TRUE)
        ds.createSchema(rddSft)
        rddSft
      }
    }

    val structType = if (data.queryExecution == null) {
      SparkUtils.createStructType(rddSft)
    } else {
      data.schema
    }

    // we need to pass schema to every worker in a serializable way
    val sftString = SimpleFeatureTypes.encodeType(storeSft, includeUserData = true)
    val typeName = storeSft.getTypeName
    val rddToSave: RDD[SimpleFeature] = data.rdd.mapPartitions { partition =>
      val sft = SimpleFeatureTypes.createType(typeName, sftString)
      val mappings = SparkUtils.rowsToFeatures(sft, structType)
      partition.map { row =>
        val sf = mappings.apply(row)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf
      }
    }

    GeoMesaSpark(parameters.asJava).save(rddToSave, parameters, newFeatureName)

    GeoMesaRelation(sqlContext, parameters, data.schema, rddSft)
  }

  // are schemas compatible? we're flexible with order, but require the same number, names and types
  private def compatible(sft: SimpleFeatureType, dataframe: SimpleFeatureType): Boolean = {
    sft.getAttributeCount == dataframe.getAttributeCount && sft.getAttributeDescriptors.asScala.forall { ad =>
      val df = dataframe.getDescriptor(ad.getLocalName)
      df != null && ad.getType.getBinding.isAssignableFrom(df.getType.getBinding)
    }
  }
}
