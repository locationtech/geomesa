/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect.functions

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2.BasicDataSource
import org.locationtech.geomesa.gt.partition.postgis.PostgisContainer
import org.locationtech.geomesa.gt.partition.postgis.dialect.ColumnName
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.MainView
import org.locationtech.geomesa.security.VisibilityUtils
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer

class PgVisTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  private val container = new PostgisContainer()
  private val ds = new BasicDataSource()

  private val tests = Seq(
    StandaloneTestCase("user", Seq("user", "admin", "test"), visible = true),
    StandaloneTestCase("user", Seq("user"), visible = true),
    StandaloneTestCase("user", Seq("admin", "test"), visible = false),
    StandaloneTestCase("user", Seq.empty, visible = false),
    StandaloneTestCase("user&admin&test", Seq("user", "admin", "test"), visible = true),
    StandaloneTestCase("user&admin&test", Seq("user", "admin"), visible = false),
    StandaloneTestCase("user&admin&test", Seq("test"), visible = false),
    StandaloneTestCase("user&admin&test", Seq.empty, visible = false),
    StandaloneTestCase("user|admin|test", Seq("user", "admin", "test"), visible = true),
    StandaloneTestCase("user|admin|test", Seq("user", "admin"), visible = true),
    StandaloneTestCase("user|admin|test", Seq("test"), visible = true),
    StandaloneTestCase("user|admin|test", Seq.empty, visible = false),
    StandaloneTestCase("(user&admin)|test", Seq("user", "admin", "test"), visible = true),
    StandaloneTestCase("(user&admin)|test", Seq("test"), visible = true),
    StandaloneTestCase("(user&admin)|test", Seq("user", "admin"), visible = true),
    StandaloneTestCase("(user&admin)|test", Seq("admin"), visible = false),
    StandaloneTestCase("(user&admin)|test", Seq.empty, visible = false),
    StandaloneTestCase(""""user"&admin""", Seq("user", "admin"), visible = true),
    StandaloneTestCase(""""u\"ser"&admin""", Seq("user", "admin"), visible = false),
    StandaloneTestCase(""""u\"ser"&admin""", Seq("u\"ser", "admin"), visible = true),
    StandaloneTestCase(""""u\\\\ser"&admin""", Seq("u\\\\ser", "admin"), visible = true),
    StandaloneTestCase("ops-team", Seq("ops-team"), visible = true),
    StandaloneTestCase("geo:admin", Seq("geo:admin"), visible = true),
    StandaloneTestCase("path/read", Seq("path/read"), visible = true),
    StandaloneTestCase("\"user name\"", Seq("user name"), visible = true),
    StandaloneTestCase("\"A&B\"", Seq("A&B"), visible = true),
    StandaloneTestCase("\"O'Reilly\"", Seq("O'Reilly"), visible = true),
    StandaloneTestCase("(\"user name\"|ops-team)&geo:admin", Seq("ops-team", "geo:admin"), visible = true),
    StandaloneTestCase("(\"user name\"|ops-team)&geo:admin", Seq("ops-team"), visible = false),
    // invalid syntax tests
    StandaloneTestCase("A.B-C+D", Seq("A", "B", "C", "D"), visible = false, invalid = true),
    StandaloneTestCase("A|B&C", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("A&B|C", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("A|!B", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("A=B", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("A|B|", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("A||B", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("A(B", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("A&|B", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("()", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase(")", Seq("A", "B", "C"), visible = false, invalid = true),
    StandaloneTestCase("'A'", Seq("A", "B", "C"), visible = false, invalid = true),
  )

  override def beforeAll(): Unit = {
    if (logger.underlying.isTraceEnabled()) {
      container
        .withLogAllStatements()
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgis")))
    }
    container.start()
    ds.setUrl(s"jdbc:postgresql://${container.getHost}:${container.getMappedPort(5432)}/postgres")
    ds.setUsername("postgres")
    ds.setPassword(container.password)
  }

  override def afterAll(): Unit = CloseWithLogging(Seq(ds, container))

  "pg_vis" should {
    "correctly evaluate visibilities" in {
      val sql =
        s"""WITH user_auths AS (SELECT array_to_tsvector(string_to_array(?, ',')) AS vis)
           |SELECT user_auths.vis @@ ?::tsquery FROM user_auths""".stripMargin
      WithClose(ds.getConnection) { cx =>
        WithClose(cx.prepareStatement(sql)) { ps =>
          foreach(tests) { test =>
            if (test.invalid) {
              PgVis.toPostgisVisibility(test.vis) must throwAn[IllegalArgumentException]
            } else {
              ps.setString(1, test.auths.mkString(","))
              ps.setString(2, PgVis.toPostgisVisibility(test.vis))
              WithClose(ps.executeQuery()) { rs =>
                rs.next must beTrue
                rs.getBoolean(1) mustEqual test.visible
                rs.next must beFalse
              }
            }
          }
        }
      }
    }

    "correctly convert from visibilities to tsvector syntax and back" in {
      foreach(tests.filterNot(_.invalid)) { test =>
        VisibilityUtils.normalize(PgVis.fromPostgisVisibility(PgVis.toPostgisVisibility(test.vis))) mustEqual
          VisibilityUtils.normalize(test.vis)
      }
    }

    "provide row-level filtering" in {
      val testData = Seq(
        TestData(0, "everyone", "user|admin"),
        TestData(1, "admin only", "admin"),
        TestData(2, "test users", "(user|admin)&test"),
      )
      val testCases = Seq(
        TableTestCase(Seq("admin"), Seq(0, 1)),
        TableTestCase(Seq("user"), Seq(0)),
        TableTestCase(Seq("user", "test"), Seq(0, 2)),
        TableTestCase(Seq("admin", "test"), Seq(0, 1, 2)),
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
        val sql =
          s"""WITH user_auths AS (SELECT array_to_tsvector(string_to_array(?, ',')) AS vis)
             |SELECT id FROM test CROSS JOIN user_auths WHERE ${MainView.whereVisible("test", ColumnName("visibilities"))}""".stripMargin
        WithClose(cx.prepareStatement(sql)) { ps =>
          foreach(testCases) { test =>
            ps.setString(1, test.auths.mkString(","))
            val result = WithClose(ps.executeQuery()) { rs =>
              Iterator.continually(rs).takeWhile(_.next).map(_.getInt(1)).toList
            }
            result mustEqual test.ids
          }
        }
      }
    }
  }

  private case class StandaloneTestCase(vis: String, auths: Seq[String], visible: Boolean, invalid: Boolean = false)
  private case class TestData(id: Int, name: String, vis: String)
  private case class TableTestCase(auths: Seq[String], ids: Seq[Int])
}
