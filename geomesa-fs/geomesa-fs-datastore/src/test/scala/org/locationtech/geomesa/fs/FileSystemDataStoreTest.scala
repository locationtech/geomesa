package org.locationtech.geomesa.fs

import java.nio.file.{Files, Paths}

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

/**
  * Created by anthony on 5/28/17.
  */

@RunWith(classOf[JUnitRunner])
class FileSystemDataStoreTest extends Specification with AllExpectations {

  "FileSystemDataStore" should {
    "connect to a data store" >> {
      sequential
      import scala.collection.JavaConversions._
      val gf = JTSFactoryFinder.getGeometryFactory
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

      val sf = new ScalaSimpleFeature("1", sft, Array("test", Integer.valueOf(100), new java.util.Date, gf.createPoint(new Coordinate(10, 10))))

      val dir = Files.createTempDirectory(Paths.get("/tmp"), "fsds")
      val params = Map("fs.path" -> dir.toAbsolutePath.toString, "fs.encoding" -> "parquet")
      val ds = DataStoreFinder.getDataStore(params)

      ds.createSchema(sft)
      val fw = ds.getFeatureWriterAppend("test", Transaction.AUTO_COMMIT)
      val s = fw.next()
      s.setAttributes(sf.getAttributes)
      s.getIdentifier.asInstanceOf[FeatureIdImpl].setID("foo")

      fw.write()
      fw.close()

      "fw must not be null" >> { fw must not beNull }


      "ds must not be null" >> {
        ds.getTypeNames must have size 1
      }


      val fs = ds.getFeatureSource("test")

      "fs must not be null" >> {
        fs must not beNull
      }

      import org.locationtech.geomesa.utils.geotools.Conversions._
      val features = fs.getFeatures(Query.ALL).features().toList

      "feature count must be 1" >> {
        features.length must be equalTo 1
      }

    }
  }
}
