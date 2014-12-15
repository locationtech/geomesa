package org.locationtech.geomesa.plugin.process

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
                 description = "List of (comma-separated) keywords for layer")
               keywordStr: String,

               @DescribeParameter(
                 name = "numShards",
                 min = 0,
                 max= 1,
                 description = "Number of shards to store for this table (defaults to 4)")
               numShards: Integer
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

    val targetType = importIntoStore(features, name, storeInfo, maxShard)

    // import the layer into geoserver
    catalogBuilder.setStore(storeInfo)
    val typeInfo = catalogBuilder.buildFeatureType(targetType.getName)
    // this is Very Ugly but FeatureTypeInfo exposes no other access to Keywords
    typeInfo.getKeywords.addAll(keywordStr.split(",").map(new Keyword(_)).toSeq)
    catalogBuilder.setupBounds(typeInfo)

    val layerInfo = catalogBuilder.buildLayer(typeInfo)

    catalog.add(typeInfo)
    catalog.add(layerInfo)

    // return layer name
    layerInfo.prefixedName
  }

  def importIntoStore(features: SimpleFeatureCollection, name: String, storeInfo: DataStoreInfo, maxShard: Int) = {
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
    fs.addFeatures(features)
    storedSft
  }

}
