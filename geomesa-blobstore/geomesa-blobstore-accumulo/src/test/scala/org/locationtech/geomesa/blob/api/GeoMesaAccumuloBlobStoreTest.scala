/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api

import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.blob.accumulo.GeoMesaAccumuloBlobStore
import org.opengis.filter._
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoMesaAccumuloBlobStoreTest extends TestWithDataStore {

  sequential

  lazy val bstore = GeoMesaAccumuloBlobStore(ds)

  val testfile1 = "testFile.txt"
  val testfile2 = "testFile2.txt"
  val testfile3 = "testFile3.txt"
  var testFile1Id = ""

  "AccumuloBlobStore" should {

    "be able to store and deleteBlob a file" in {
      val (storeId, file) = ingestFile(testfile3, "POINT(10 10)")

      bstore.delete(storeId)

      // test if blobstore get is failing to return anything
      val ret = bstore.get(storeId)
      ret must beNull

      // test if geomesa feature table is empty
      val ids = bstore.getIds(Filter.INCLUDE).toList
      ids must beEmpty
    }

    "be able to store and retrieve a file" in {
      val (storeId, file) = ingestFile(testfile1, "POINT(0 0)")

      val ret = bstore.get(storeId)

      val is = new FileInputStream(file)
      val inputStream = try { IOUtils.toByteArray(is) } finally { is.close() }

      ret.getLocalName mustEqual testfile1
      inputStream mustEqual ret.getPayload
    }

    "query for ids and then retrieve a file" in {
      // Since the test is sequential, the first file should be in the store.
      val ids = bstore.getIds(Filter.INCLUDE).toList

      ids.size mustEqual 1
      val id = ids.head

      val ret = bstore.get(id)

      ret.getPayload must not be null
      ret.getLocalName mustEqual testfile1
    }

    "insert a second file and then be able to query for both" in {
      val (storeId2, _) = ingestFile(testfile2, "POINT(50 50)")

      val ids2 = bstore.getIds(Filter.INCLUDE).toList
      ids2.size mustEqual 2

      val ret = bstore.get(storeId2)

      ret.getLocalName mustEqual testfile2

      val filter = ECQL.toFilter("BBOX(geom, -10,-10,10,10)")

      val filteredIds = bstore.getIds(filter).toList

      filteredIds.size mustEqual 1
    }
  }

  def ingestFile(fileName: String, wkt: String): (String, File) = {
    val file = new File(getClass.getClassLoader.getResource(fileName).getFile)
    val params = Map("wkt" -> wkt)
    val storeId = bstore.put(file, params)
    (storeId, file)
  }
}
