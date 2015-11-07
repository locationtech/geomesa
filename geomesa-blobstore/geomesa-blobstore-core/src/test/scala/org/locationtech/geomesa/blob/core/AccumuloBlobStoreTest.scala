package org.locationtech.geomesa.blob.core

import java.io.File

import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloBlobStoreTest extends Specification {
  val dsParams = Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
    "user"              -> "myuser",
    "password"          -> "mypassword",
    "tableName"         -> "geomesa",
    "useMock"           -> "true",
    "featureEncoding"   -> "avro")
  val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

  val bstore = new AccumuloBlobStore(ds)

  "AccumuloBlobStore" should {
    "be able able to store and retrieve a file" in {

      val file = new File(getClass().getClassLoader.getResource("testFile.txt").getFile)
      val params = Map("wkt" -> "POINT(0,0)")

      val storeId = bstore.put(file, params)
      val returnFile = bstore.get(storeId)





    }
  }



}
