/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.plugin.process

import java.{util => ju}

import org.geoserver.catalog.{Keyword, Catalog, CatalogBuilder, DataStoreInfo}
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.plugin.wps.GeomesaProcess

import scala.collection.JavaConversions._

@DescribeProcess(
  title = "Geomesa Bulk Import",
  description = "Bulk Import data into Geomesa from another process with no transformations of data"
)
class ImportProcess(val catalog: Catalog) extends GeomesaProcess {

  val DEFAULT_MAX_SHARD = 3 // 4 shards

  @DescribeResult(name = "layerName", description = "Name of the new featuretype, with workspace")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "Input feature collection")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "workspace",
                 description = "Target workspace")
               workspace: String,

               @DescribeParameter(
                 name = "store",
                 description = "Target store")
               store: String,

               @DescribeParameter(
                 name = "name",
                 description = "Name of the new featureType or layer name")
               name: String,

               @DescribeParameter(
                 name = "keywords",
                 min = 0,
                 collectionType = classOf[String],
                 description = "List of (comma-separated) keywords for layer")
               keywordStrs: ju.List[String],

               @DescribeParameter(
                 name = "numShards",
                 min = 0,
                 max= 1,
                 description = "Number of shards to store for this table (defaults to 4)")
               numShards: Integer,

               @DescribeParameter(
                 name = "securityLevel",
                 min = 0,
                 max = 1,
                 description = "The level of security to apply to this import")
               securityLevel: String
              ) = {

    val workspaceInfo = Option(catalog.getWorkspaceByName(workspace)).getOrElse {
      throw new ProcessException(s"Unable to find workspace $workspace")
    }

    val catalogBuilder = new CatalogBuilder(catalog)
    catalogBuilder.setWorkspace(workspaceInfo)

    val storeInfo = Option(catalog.getDataStoreByName(workspaceInfo.getName, store)).getOrElse {
      throw new ProcessException(s"Unable to find store $store in workspace $workspace")
    }

    val maxShard = Option(numShards).map { n => if(n > 1) n-1 else DEFAULT_MAX_SHARD }.getOrElse(DEFAULT_MAX_SHARD)

    val targetType = importIntoStore(features, name, storeInfo, maxShard, Option(securityLevel))

    // import the layer into geoserver
    catalogBuilder.setStore(storeInfo)
    val typeInfo = catalogBuilder.buildFeatureType(targetType.getName)
    
    val kws = for {
      ks <- Option(keywordStrs).getOrElse(new ju.ArrayList[String]())
      kw <- ks.split(",").map(_.trim)
    } yield new Keyword(kw)
    typeInfo.getKeywords.addAll(kws)
    catalogBuilder.setupBounds(typeInfo)

    val layerInfo = catalogBuilder.buildLayer(typeInfo)

    catalog.add(typeInfo)
    catalog.add(layerInfo)

    // return layer name
    layerInfo.prefixedName
  }

  def importIntoStore(features: SimpleFeatureCollection,
                      name: String,
                      storeInfo: DataStoreInfo,
                      maxShard: Int,
                      visibility: Option[String]) = {
    val ds = storeInfo.getDataStore(null)
    if(!ds.isInstanceOf[AccumuloDataStore]) {
      throw new ProcessException(s"Cannot import into non-AccumuloDataStore of type ${ds.getClass.getName}")
    }
    val accumuloDS = ds.asInstanceOf[AccumuloDataStore]

    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.init(features.getSchema)
    sftBuilder.setName(name)
    val sft = sftBuilder.buildFeatureType
    accumuloDS.createSchema(sft, maxShard)

    // query the actual SFT stored by the source
    val storedSft = accumuloDS.getSchema(sft.getName)

    // verify the layer doesn't already exist
    val layerName = s"${storeInfo.getWorkspace.getName}:${storedSft.getTypeName}"
    val layer = catalog.getLayerByName(layerName)
    if(layer != null) throw new ProcessException(s"Target layer $layerName already exists in the catalog")

    val fs = accumuloDS.getFeatureSource(storedSft.getName).asInstanceOf[AccumuloFeatureStore]
    fs.addFeatures(features, visibility)
    storedSft
  }

}
