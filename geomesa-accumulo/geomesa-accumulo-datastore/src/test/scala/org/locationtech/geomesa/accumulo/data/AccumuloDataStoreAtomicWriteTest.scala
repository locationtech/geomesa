/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.ConditionalWriter
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException.ConditionalWriteStatus
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.AtomicWriteTransaction
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAtomicWriteTest extends Specification with TestWithMultipleSfts {

  import scala.collection.JavaConverters._

  val spec = "name:String:index=true,dtg:Date,geom:Point:srid=4326"

  def features(sft: SimpleFeatureType): Seq[SimpleFeature] = Seq.tabulate(5) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"2024-02-15T0$i:00:01.000Z", s"POINT (0 $i)")
  }

  def update1(sft: SimpleFeatureType): SimpleFeature =
    ScalaSimpleFeature.create(sft, "4", "name4", "2024-02-15T04:00:02.000Z", "POINT (1 4)")
  def update2(sft: SimpleFeatureType): SimpleFeature =
    ScalaSimpleFeature.create(sft, "4", "name4", "2024-02-15T04:00:03.000Z", "POINT (2 4)")

  val filters = Seq(
    s"IN(${Seq.tabulate(10)(i => i).mkString("'", "','", "'")})", // id index
    "bbox(geom, -1, -1, 10, 10)", // z2
    "bbox(geom, -1, -1, 10, 10) AND dtg during 2024-02-15T00:00:00.000Z/2024-02-15T12:00:00.000Z", // z3
    s"name IN(${Seq.tabulate(10)(i => s"name$i").mkString("'", "','", "'")})" // attribute
  ).map(ECQL.toFilter)

  def query(sft: SimpleFeatureType, filter: Filter): Seq[SimpleFeature] = {
    val query = new Query(sft.getTypeName, filter)
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
  }

  "AccumuloDataStore atomic writer" should {
    "append features" in {
      val sft = createNewSchema(spec)
      val feats = features(sft)
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, AtomicWriteTransaction.INSTANCE)) { writer =>
        feats.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
      foreach(filters) { filter =>
        query(sft, filter) mustEqual feats
      }
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, AtomicWriteTransaction.INSTANCE)) { writer =>
        foreach(feats) { f =>
          FeatureUtils.write(writer, f, useProvidedFid = true) must throwA[ConditionalWriteException].like {
            case e: ConditionalWriteException =>
              e.getRejections.asScala must containTheSameElementsAs(
                Seq(
                  ConditionalWriteStatus("id", "insert", ConditionalWriter.Status.REJECTED),
                  ConditionalWriteStatus("z2:geom", "insert", ConditionalWriter.Status.REJECTED),
                  ConditionalWriteStatus("z3:geom:dtg", "insert", ConditionalWriter.Status.REJECTED),
                  ConditionalWriteStatus("attr:name:geom:dtg", "insert", ConditionalWriter.Status.REJECTED),
                )
              )
          }
        }
      }
      foreach(filters) { filter =>
        query(sft, filter) mustEqual feats
      }
    }
    "update features" in {
      val sft = createNewSchema(spec)
      val feats = features(sft)
      addFeatures(feats)

      val up = update1(sft)
      WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN ('4')"), AtomicWriteTransaction.INSTANCE)) { writer =>
        writer.hasNext must beTrue
        val update = writer.next
        update.setAttribute("geom", up.getAttribute("geom"))
        update.setAttribute("dtg", up.getAttribute("dtg"))
        writer.write()
      }
      foreach(filters) { filter =>
        query(sft, filter) mustEqual feats.take(4) ++ Seq(up)
      }
    }
    "append and update with auths" in {
      val sft = createNewSchema(spec)
      val feats = features(sft)
      feats.foreach(f => SecurityUtils.setFeatureVisibility(f, "user"))
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, AtomicWriteTransaction.INSTANCE)) { writer =>
        feats.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
      foreach(filters) { filter =>
        val res = query(sft, filter)
        res mustEqual feats
        res.map(SecurityUtils.getVisibility) mustEqual Seq.fill(5)("user")
      }

      val up = update1(sft)
      WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN ('4')"), AtomicWriteTransaction.INSTANCE)) { writer =>
        writer.hasNext must beTrue
        val update = writer.next
        update.setAttribute("geom", up.getAttribute("geom"))
        update.setAttribute("dtg", up.getAttribute("dtg"))
        SecurityUtils.setFeatureVisibility(update, "admin")
        writer.write()
      }
      foreach(filters) { filter =>
        val res = query(sft, filter)
        res mustEqual feats.take(4) ++ Seq(up)
        res.map(SecurityUtils.getVisibility) mustEqual Seq.fill(4)("user") :+ "admin"
      }
    }
    "throw exception and roll-back if atomic update is violated" in {
      val sft = createNewSchema(spec)
      val feats = features(sft)
      addFeatures(feats)

      val up1 = update1(sft)
      val up2 = update2(sft)
      WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN ('4')"), AtomicWriteTransaction.INSTANCE)) { writer =>
        writer.hasNext must beTrue
        val update = writer.next
        update.setAttribute("geom", up1.getAttribute("geom"))
        update.setAttribute("dtg", up1.getAttribute("dtg"))
        // after getting the read, go in and make an edit
        WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN ('4')"), AtomicWriteTransaction.INSTANCE)) { writer =>
          writer.hasNext must beTrue
          val update = writer.next
          update.setAttribute("geom", up2.getAttribute("geom"))
          update.setAttribute("dtg", up2.getAttribute("dtg"))
          writer.write()
        }
        foreach(filters) { filter =>
          query(sft, filter) mustEqual feats.take(4) ++ Seq(up2)
        }
        writer.write() must throwA[ConditionalWriteException].like {
          case e: ConditionalWriteException =>
            e.getFeatureId mustEqual "4"
            e.getRejections.asScala must containTheSameElementsAs(
              Seq(
                ConditionalWriteStatus("id", "update", ConditionalWriter.Status.REJECTED),
                ConditionalWriteStatus("z2:geom", "delete", ConditionalWriter.Status.REJECTED),
                ConditionalWriteStatus("z3:geom:dtg", "delete", ConditionalWriter.Status.REJECTED),
                ConditionalWriteStatus("attr:name:geom:dtg", "delete", ConditionalWriter.Status.REJECTED),
              )
            )
        }
      }
      // verify update was rolled back correctly
      foreach(filters) { filter =>
        query(sft, filter) mustEqual feats.take(4) ++ Seq(up2)
      }
    }
    "throw exception and roll-back if atomic delete is violated" in {
      val sft = createNewSchema(spec)
      val feats = features(sft)
      addFeatures(feats)

      val up1 = update1(sft)
      WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN ('4')"), AtomicWriteTransaction.INSTANCE)) { writer =>
        writer.hasNext must beTrue
        writer.next
        // after getting the read, go in and make an edit
        WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN ('4')"), AtomicWriteTransaction.INSTANCE)) { writer =>
          writer.hasNext must beTrue
          val update = writer.next
          update.setAttribute("geom", up1.getAttribute("geom"))
          update.setAttribute("dtg", up1.getAttribute("dtg"))
          writer.write()
        }
        foreach(filters) { filter =>
          query(sft, filter) mustEqual feats.take(4) ++ Seq(up1)
        }
        writer.remove() must throwA[ConditionalWriteException].like {
          case e: ConditionalWriteException =>
            e.getFeatureId mustEqual "4"
            e.getRejections.asScala must containTheSameElementsAs(
              Seq(
                ConditionalWriteStatus("id", "delete", ConditionalWriter.Status.REJECTED),
                ConditionalWriteStatus("z2:geom", "delete", ConditionalWriter.Status.REJECTED),
                ConditionalWriteStatus("z3:geom:dtg", "delete", ConditionalWriter.Status.REJECTED),
                ConditionalWriteStatus("attr:name:geom:dtg", "delete", ConditionalWriter.Status.REJECTED),
              )
            )
        }
      }
      // verify update was rolled back correctly
      foreach(filters) { filter =>
        query(sft, filter) mustEqual feats.take(4) ++ Seq(up1)
      }
    }
    "make direct updates with a low-level atomic writer" in {
      val sft = createNewSchema(spec)
      val feats = features(sft)
      addFeatures(feats)

      val indices = ds.manager.indices(sft, mode = IndexMode.Write)
      WithClose(ds.adapter.createWriter(sft, indices, None, atomic = true)) { writer =>
        val up1 = update1(sft)
        writer.update(up1, feats(4))
        foreach(filters) { filter =>
          query(sft, filter) mustEqual feats.take(4) ++ Seq(up1)
        }
        val up2 = update2(sft)
        writer.update(up2, up1)
        foreach(filters) { filter =>
          query(sft, filter) mustEqual feats.take(4) ++ Seq(up2)
        }
        // verify incorrect updates are rejected without changes
        writer.update(feats(4), up1) must throwA[ConditionalWriteException]
        foreach(filters) { filter =>
          query(sft, filter) mustEqual feats.take(4) ++ Seq(up2)
        }
      }
    }
  }
}
