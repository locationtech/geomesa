/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect.functions

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2.BasicDataSource
import org.locationtech.geomesa.gt.partition.postgis.PostgisContainer
import org.locationtech.geomesa.gt.partition.postgis.dialect.ExecutionContext
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll

class PgVisTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  private val container = new PostgisContainer()
  private val ds = new BasicDataSource()

  override def beforeAll(): Unit = {
    if (logger.underlying.isTraceEnabled()) {
      container.withLogAllStatements()
    }
    container.start()
    ds.setUrl(s"jdbc:postgresql://${container.getHost}:${container.getMappedPort(5432)}/postgres")
    ds.setUsername("postgres")
    ds.setPassword(container.password)
    WithClose(ds.getConnection) { cx =>
      WithClose(new ExecutionContext(cx)) { ec =>
        PgVis.create(null)(ec)
      }
    }
  }

  override def afterAll(): Unit = CloseWithLogging(Seq(ds, container))

  "pg_vis" should {
    "correctly evaluate visibilities" in {
      val tests = Seq(
        StandaloneTestCase("user", Array("user", "admin", "test"), visible = true),
        StandaloneTestCase("user", Array("user"), visible = true),
        StandaloneTestCase("user", Array("admin", "test"), visible = false),
        StandaloneTestCase("user", Array.empty, visible = false),
        StandaloneTestCase("user&admin&test", Array("user", "admin", "test"), visible = true),
        StandaloneTestCase("user&admin&test", Array("user", "admin"), visible = false),
        StandaloneTestCase("user&admin&test", Array("test"), visible = false),
        StandaloneTestCase("user&admin&test", Array.empty, visible = false),
        StandaloneTestCase("user|admin|test", Array("user", "admin", "test"), visible = true),
        StandaloneTestCase("user|admin|test", Array("user", "admin"), visible = true),
        StandaloneTestCase("user|admin|test", Array("test"), visible = true),
        StandaloneTestCase("user|admin|test", Array.empty, visible = false),
        StandaloneTestCase("(user&admin)|test", Array("user", "admin", "test"), visible = true),
        StandaloneTestCase("(user&admin)|test", Array("test"), visible = true),
        StandaloneTestCase("(user&admin)|test", Array("user", "admin"), visible = true),
        StandaloneTestCase("(user&admin)|test", Array("admin"), visible = false),
        StandaloneTestCase("(user&admin)|test", Array.empty, visible = false),
        StandaloneTestCase("'user'&admin", Array("user", "admin"), visible = true),
        StandaloneTestCase("'u\\'ser'&admin", Array("user", "admin"), visible = false),
        StandaloneTestCase("'u\\'ser'&admin", Array("u'ser", "admin"), visible = true),
        StandaloneTestCase("'u\\\\\\\\ser'&admin", Array("u\\\\ser", "admin"), visible = true),
        StandaloneTestCase("A.B-C+D", Array("A", "B", "C", "D"), visible = false),
      )
      WithClose(ds.getConnection) { cx =>
        WithClose(cx.prepareStatement("select pg_vis(?,?);")) { ps =>
          foreach(tests) { test =>
            ps.setString(1, test.vis)
            ps.setArray(2, cx.createArrayOf("varchar", test.auths))
            WithClose(ps.executeQuery()) { rs =>
              rs.next must beTrue
              rs.getBoolean(1) mustEqual test.visible
              rs.next must beFalse
            }
          }
        }
      }
    }

    "provide row-level filtering" in {
      val testData = Seq(
        TestData(0, "everyone", "user|admin"),
        TestData(1, "admin only", "admin"),
        TestData(2, "test users", "(user|admin)&test"),
      )
      val testCases = Seq(
        TableTestCase(Array[AnyRef]("admin"), Seq(0, 1)),
        TableTestCase(Array[AnyRef]("user"), Seq(0)),
        TableTestCase(Array[AnyRef]("user", "test"), Seq(0, 2)),
        TableTestCase(Array[AnyRef]("admin", "test"), Seq(0, 1, 2)),
      )
      WithClose(ds.getConnection) { cx =>
        WithClose(cx.createStatement()) { st =>
          st.executeUpdate("CREATE TABLE test(id integer primary key, name varchar, visibilities varchar);")
        }
        WithClose(cx.prepareStatement("INSERT INTO test(id, name, visibilities) VALUES (?, ?, ?);")) { ps =>
          testData.foreach { d =>
            ps.setInt(1, d.id)
            ps.setString(2, d.name)
            ps.setString(3, d.vis)
            ps.executeUpdate()
          }
        }
        WithClose(cx.prepareStatement("select id from test where pg_vis(visibilities,?) = true;")) { ps =>
          foreach(testCases) { test =>
            ps.setArray(1, ps.getConnection.createArrayOf("varchar", test.auths))
            val result = WithClose(ps.executeQuery()) { rs =>
              Iterator.continually(rs).takeWhile(_.next).map(_.getInt(1)).toList
            }
            result mustEqual test.ids
          }
        }
      }
    }
  }

  private case class StandaloneTestCase(vis: String, auths: Array[AnyRef], visible: Boolean)
  private case class TestData(id: Int, name: String, vis: String)
  private case class TableTestCase(auths: Array[AnyRef], ids: Seq[Int])
}
