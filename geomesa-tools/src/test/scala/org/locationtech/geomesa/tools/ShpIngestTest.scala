package org.locationtech.geomesa.tools

import java.io.File
import java.util.Date

import com.google.common.io.Files
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShpIngestTest extends Specification {

  import org.locationtech.geomesa.utils.geotools.Conversions._

import scala.collection.JavaConversions._

  "ShpIngest" >> {
    val geomBuilder = JTSFactoryFinder.getGeometryFactory

    val shpStoreFactory = new ShapefileDataStoreFactory
    val shpFile = new File(Files.createTempDir(), "shpingest.shp")
    val shpUrl = shpFile.toURI.toURL
    val params = Map("url" -> shpUrl)
    val shpStore = shpStoreFactory.createNewDataStore(params)
    val schema = SimpleFeatureTypes.createType("shpingest", "age:Integer,dtg:Date,*geom:Point:srid=4326")
    shpStore.createSchema(schema)
    val data =
      List(
        ("1", 1, new Date(), (10.0, 10.0)),
        ("1", 2, new Date(), (20.0, 20.0))
      )
    val writer = shpStore.getFeatureWriterAppend("shpingest", Transaction.AUTO_COMMIT)
    data.foreach { case (id, age, dtg, (lat, lon)) =>
      val f = writer.next()
      f.setAttribute("age", age)
      f.setAttribute("dtg", dtg)
      val pt = geomBuilder.createPoint(new Coordinate(lat, lon))
      f.setDefaultGeometry(pt)
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      f.getUserData.put(Hints.PROVIDED_FID, id)
      writer.write()
    }
    writer.flush()
    writer.close()

    val dsConf =     Map(
      "instanceId"      -> "mycloud",
      "zookeepers"      -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"            -> "myuser",
      "password"        -> "mypassword",
      "tableName"       -> "testshpingestcatalog",
      "useMock"         -> "true")

    val ds = DataStoreFinder.getDataStore(dsConf)

    "should properly ingest a shapefile" >> {
      val ingestConf = IngestArguments(file = shpFile.getAbsolutePath)
      ShpIngest.doIngest(ingestConf, dsConf)

      val fs = ds.getFeatureSource("shpingest")
      val result = fs.getFeatures.features().toList
      result.length must beEqualTo(2)
    }

    "should support renaming the feature type" >> {
      val ingestConf = IngestArguments(file = shpFile.getAbsolutePath, featureName = "changed")
      ShpIngest.doIngest(ingestConf, dsConf)

      val fs = ds.getFeatureSource("changed")
      val result = fs.getFeatures.features().toList
      result.length must beEqualTo(2)

    }
  }
}
