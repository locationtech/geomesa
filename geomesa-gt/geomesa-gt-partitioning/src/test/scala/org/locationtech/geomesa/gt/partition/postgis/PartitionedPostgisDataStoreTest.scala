/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.postgis.PostGISPSDialect
import org.geotools.data.{DataStoreFinder, DefaultTransaction, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.jdbc.JDBCDataStore
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.annotation.tailrec
import scala.util.control.NonFatal

@RunWith(classOf[JUnitRunner])
class PartitionedPostgisDataStoreTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  val hours = 1
  val spec =
    "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;" +
        Seq(
          s"pg.partitions.interval.hours=$hours",
          "pg.partitions.cron.minute=0"/*,
          "pg.partitions.max=2",
          "pg.partitions.tablespace.wa=wa",
          "pg.partitions.tablespace.wa-partitions=wa_partition",
          "pg.partitions.tablespace.main=partition"*/
        ).mkString(",")

  val methods =
    Methods(
      create = false,
      recreate = false,
      write = false,
      update = false,
      delete = false,
      remove = false
    )

  lazy val sft = SimpleFeatureTypes.createType(s"test", spec)

  "PartitionedPostgisDataStore" should {
    "work" in {
      skipped("requires postgis instance")
      val params =
        Map(
          "dbtype"   -> PartitionedPostgisDataStoreParams.DbType.sample,
          "host"     -> "localhost",
          "port"     -> "5432",
          "schema"   -> "public",
          "database" -> "geodata",
          "user"     -> "postgres",
          "passwd"   -> "gimmee",
          "Batch insert size"  -> "10",
          "Commit size"        -> "20",
          "preparedStatements" -> "true"
        )

      val ds = DataStoreFinder.getDataStore(params.asJava)

      try {
        ds must not(beNull)
        ds must beAnInstanceOf[JDBCDataStore]

        logger.info(s"Existing type names: ${ds.getTypeNames.mkString(", ")}")

        if (methods.create) {
          ds.createSchema(sft)
        } else if (methods.recreate) {
          WithClose(ds.asInstanceOf[JDBCDataStore].getConnection(Transaction.AUTO_COMMIT)) { cx =>
            val dialect = ds.asInstanceOf[JDBCDataStore].dialect match {
              case p: PartitionedPostgisDialect => p
              case p: PostGISPSDialect =>
                @tailrec
                def unwrap(c: Class[_]): Class[_] =
                  if (c == classOf[PostGISPSDialect]) { c } else { unwrap(c.getSuperclass) }
                val m = unwrap(p.getClass).getDeclaredMethod("getDelegate")
                m.setAccessible(true)
                m.invoke(p).asInstanceOf[PartitionedPostgisDialect]
            }
            dialect.recreateFunctions("public", sft, cx)
          }
        }

        val userData = ds.getSchema(sft.getTypeName).getUserData.asScala
        userData must containAllOf(sft.getUserData.asScala.toSeq)

        val now = System.currentTimeMillis()

        if (methods.write) {
          WithClose(new DefaultTransaction()) { tx =>
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
              (1 to 10).foreach { i =>
                val next = writer.next()
                next.setAttribute("name", s"name$i")
                next.setAttribute("age", i)
                next.setAttribute("dtg", new java.util.Date(now - (i * 20 * 60 * 1000))) // 20 minutes
                next.setAttribute("geom", WKTUtils.read(s"POINT(0 $i)"))
                next.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
                next.getIdentifier.asInstanceOf[FeatureIdImpl].setID(s"fid$i")
                writer.write()
              }
            }
            tx.commit()
          }
        }

        if (methods.update) {
          (1 to 10).foreach { i =>
            WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter(s"IN('fid$i')"), Transaction.AUTO_COMMIT)) { writer =>
              if (writer.hasNext) {
                val next = writer.next()
                next.setAttribute("dtg", new java.util.Date(now - (i * 5 * 60 * 1000)))
                writer.write()
              } else {
                logger.warn(s"No entry found for update fid$i")
              }
            }
          }
        }

        if (methods.delete) {
          (1 to 10).foreach { i =>
            WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter(s"IN('fid$i')"), Transaction.AUTO_COMMIT)) { writer =>
              if (writer.hasNext) {
                writer.next()
                writer.remove()
              } else {
                logger.warn(s"No entry found for delete fid$i")
              }
            }
          }
        }

        if (methods.remove) {
          ds.removeSchema(sft.getTypeName)
        }
      } catch {
        case NonFatal(e) => e.printStackTrace(); ko(e.toString)
      } finally {
        if (ds != null) {
          ds.dispose()
        }
      }
      ok
    }
  }

  case class Methods(create: Boolean, recreate: Boolean, write: Boolean, update: Boolean, delete: Boolean, remove: Boolean)
}
