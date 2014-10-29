/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils.geotools

import java.io.{File, Serializable}
import java.util.{Map => JMap}

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object GeneralShapefileIngest {
  def shpToDataStoreViaParams(shapefilePath: String, params: JMap[String, Serializable]): DataStore =
   shpToDataStoreViaParams(shapefilePath, params, null)

  def shpToDataStoreViaParams(shapefilePath: String,
                              params: JMap[String, Serializable], featureName: String): DataStore = {
    val shapefile =  FileDataStoreFinder.getDataStore(new File(shapefilePath))
    val features = shapefile.getFeatureSource.getFeatures
    val newDS = featuresToDataStoreViaParams(features, params, featureName)
    shapefile.dispose()
    newDS
  }

  def shpToDataStore(shapefilePath: String, ds: DataStore, featureName: String): DataStore = {
    val shapefile =  FileDataStoreFinder.getDataStore(new File(shapefilePath))
    val features = shapefile.getFeatureSource.getFeatures
    val newDS = featuresToDataStore(features, ds, featureName)
    shapefile.dispose()
    newDS
  }

  def featuresToDataStoreViaParams(features: SimpleFeatureCollection,
                                   params: JMap[String, Serializable],
                                   featureName: String): DataStore = {
    val ds = DataStoreFinder.getDataStore(params)
    featuresToDataStore(features, ds, featureName)
  }

  def featuresToDataStore(features: SimpleFeatureCollection,
                          ds: DataStore,
                          featureName: String): DataStore = {
    // Add the ability to rename this FT
    val featureType: SimpleFeatureType =
      if(featureName != null) {   // Is this line right?
        val originalFeatureType = features.getSchema
        val sftBuilder = new SimpleFeatureTypeBuilder()
        sftBuilder.init(originalFeatureType)
        sftBuilder.setName(featureName)
        sftBuilder.buildFeatureType()
      } else
        features.getSchema

    val featureTypeName = featureType.getName.getLocalPart

    ds.createSchema(featureType)

    val newType = ds.getSchema(featureTypeName)

    val reTypedSFC = new TypeUpdatingFeatureCollection(features, newType)

    val fs: FeatureStore[SimpleFeatureType, SimpleFeature] =
      ds.getFeatureSource(featureTypeName).asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]

    fs.addFeatures(reTypedSFC)
    ds
  }
}


