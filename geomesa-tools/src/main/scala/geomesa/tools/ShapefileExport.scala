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
package geomesa.tools

import java.io.File

import org.geotools.data.DataUtilities
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureStore}
import org.opengis.feature.simple.SimpleFeatureType

class ShapefileExport {

  def write(file: String, feature: String, collection: SimpleFeatureCollection, schema: SimpleFeatureType) {

    val url = DataUtilities.fileToURL(new File(file))

    // create a new shapfile data store
    val factory = new ShapefileDataStoreFactory()
    val newShapeFile = factory.createDataStore(url).asInstanceOf[ShapefileDataStore]

    // create a schema in the new datastore
    newShapeFile.createSchema(schema)

    val store = newShapeFile.getFeatureSource.asInstanceOf[SimpleFeatureStore]

    store.addFeatures(collection)
  }
}
