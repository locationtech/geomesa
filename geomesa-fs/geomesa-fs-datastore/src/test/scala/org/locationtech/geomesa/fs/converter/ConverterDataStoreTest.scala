/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.converter

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.{CreateFlag, FileContext, Path}
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.filter.Filter
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

import java.io.{BufferedOutputStream, ByteArrayInputStream}
import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class ConverterDataStoreTest extends Specification with BeforeAfterAll {

  import scala.collection.JavaConverters._

  sequential

  var minio: MinIOContainer = _
  val bucket = "geomesa"

  override def beforeAll(): Unit = {
    minio =
      new MinIOContainer(
        DockerImageName.parse("minio/minio").withTag(sys.props.getOrElse("minio.docker.tag", "RELEASE.2024-10-29T16-01-48Z")))
    minio.start()
    minio.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("minio")))
    minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName, minio.getPassword)
    minio.execInContainer("mc", "mb", s"localhost/$bucket")
  }

  override def afterAll(): Unit = {
    if (minio != null) {
      minio.close()
    }
  }

  def fsConfig(converter: String, path: String): String = {
    val props = Seq(
      prop("fs.options.converter.path", path),
      prop("fs.partition-scheme.name", "datetime"),
      prop("fs.partition-scheme.opts.datetime-format", "yyyy/DDD/HH/mm"),
      prop("fs.partition-scheme.opts.step-unit", "MINUTES"),
      prop("fs.partition-scheme.opts.step", "15"),
      prop("fs.partition-scheme.opts.dtg-attribute", "dtg"),
      prop("fs.options.leaf-storage", "true"),
    )
    s"""<configuration>
      |$converter
      |${props.mkString("\n")}
      |</configuration>
      |""".stripMargin
  }

  def sftByName(name: String): String = {
    Seq(
      prop("fs.options.sft.name", name),
      prop("fs.options.converter.name", name),
    ).mkString("\n")
  }

  def sftByConf(conf: String): String = {
    Seq(
      prop("fs.options.sft.conf", conf),
      prop("fs.options.converter.conf", conf),
    ).mkString("\n")
  }

  def prop(key: String, value: String): String = s"  <property><name>$key</name><value>$value</value></property>"

  "ConverterDataStore" should {
    "work with one datastore" >> {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path"       -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding"   -> "converter",
        "fs.config.xml" -> fsConfig(sftByName("fs-test"), "datastore1")
      ).asJava)
      ds must not(beNull)

      val types = ds.getTypeNames
      types must haveSize(1)
      types.head mustEqual "fs-test"

      val q = new Query("fs-test", Filter.INCLUDE)
      val feats = CloseableIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
      feats must haveLength(4)
    }

    "work with something else" >> {
      val params = Map(
        "fs.path"       -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding"   -> "converter",
        "fs.config.xml" -> fsConfig(sftByName("fs-test"), "datastore2")
      )
      WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        ds must not(beNull)

        val types = ds.getTypeNames
        types must haveSize(1)
        types.head mustEqual "fs-test"

        val q = new Query("fs-test", Filter.INCLUDE)
        val feats = CloseableIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        feats must haveLength(4)
      }
    }

    "read tar.gz files from s3 storage" >> {
      val bucket = s"s3a://${this.bucket}/"
      val config = {
        val props = Seq(
          sftByName("fs-test"),
          prop("fs.s3a.endpoint", minio.getS3URL),
          prop("fs.s3a.access.key", minio.getUserName),
          prop("fs.s3a.secret.key", minio.getPassword),
          prop("fs.s3a.path.style.access", "true"),
          prop("dfs.client.use.datanode.hostname", "true"),
          prop("fs.s3a.connection.maximum", "20") // reduce connection pool size to show resource leaks quickly
        ).mkString("\n")
        fsConfig(props, "datastore1")
      }
      val fc = {
        val conf = new Configuration()
        conf.addResource(new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8)))
        FileContext.getFileContext(conf)
      }
      // number of times to write the sample files into our tgz
      // note: we need fairly large files to trigger GEOMESA-3411
      val multiplier = 177156
      Seq("00", "15", "30", "45").foreach { file =>
        val path = s"datastore1/2017/001/01/$file"
        val contents = WithClose(getClass.getClassLoader.getResourceAsStream(s"example/$path"))(IOUtils.toByteArray)
        writePlain(fc, s"$bucket$path", contents, multiplier)
        writePlainGz(fc, s"$bucket$path", contents, multiplier)
        writeTarGz(fc, s"$bucket$path", contents, multiplier)
      }

      foreach(Seq(("100 millis", true), ("5 minutes", false))) { case (timeout, expectTimeout) =>
        val params = Map(
          "fs.path"               -> bucket,
          "fs.encoding"           -> "converter",
          "fs.config.xml"         -> config,
          "fs.read-threads"       -> "12",
          "geomesa.query.timeout" -> timeout,
        )
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          ds must not(beNull)

          val types = ds.getTypeNames
          types must haveSize(1)
          types.head mustEqual "fs-test"

          foreach(Range(0, 5)) { _ =>
            val count = try {
              val q = new Query("fs-test", Filter.INCLUDE)
              WithClose(CloseableIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)))(_.length)
            } catch {
              case _: RuntimeException => -1
            }
            if (expectTimeout) {
              count mustEqual -1
            } else {
              count mustEqual multiplier * 12
            }
          }
        }
      }
    }

    "load sft as a string" >> {

      val conf = ConfigFactory.parseString(
        """
          |geomesa {
          |  sfts {
          |    "fs-test" = {
          |      attributes = [
          |        { name = "name", type = "String", index = true                              }
          |        { name = "dtg",  type = "Date",   index = false                             }
          |        { name = "geom", type = "Point",  index = true, srid = 4326, default = true }
          |      ]
          |    }
          |  }
          |  converters {
          |    "fs-test" {
          |      type   = "delimited-text",
          |      format = "CSV",
          |      options {
          |        skip-lines = 0
          |      },
          |      id-field = "toString($name)",
          |      fields = [
          |        { name = "name", transform = "$1::string"   }
          |        { name = "dtg",  transform = "dateTime($2)" }
          |        { name = "geom", transform = "point($3)"    }
          |      ]
          |    }
          |
          |  }
          |}
        """.stripMargin
      ).root().render(ConfigRenderOptions.concise)

      val params = Map(
        "fs.path"       -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding"   -> "converter",
        "fs.config.xml" -> fsConfig(sftByConf(conf), "datastore1")
      )
      WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        ds must not(beNull)

        val types = ds.getTypeNames
        types must haveSize(1)
        types.head mustEqual "fs-test"

        val q = new Query("fs-test", Filter.INCLUDE)
        val feats = CloseableIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
        feats must haveSize(4)
      }
    }
  }

  private def writePlain(fc: FileContext, path: String, contents: Array[Byte], multiplier: Int): Unit = {
    WithClose(fc.create(new Path(path), java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent())) { os =>
      WithClose(new BufferedOutputStream(os)) { buf =>
        var i = 0
        while (i < multiplier) {
          buf.write(contents)
          i += 1
        }
      }
    }
  }

  private def writePlainGz(fc: FileContext, path: String, contents: Array[Byte], multiplier: Int): Unit = {
    WithClose(fc.create(new Path(s"$path.gz"), java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent())) { os =>
      WithClose(new BufferedOutputStream(os)) { buf =>
        WithClose(new GzipCompressorOutputStream(buf)) { gz =>
          var i = 0
          while (i < multiplier) {
            gz.write(contents)
            i += 1
          }
        }
      }
    }
  }

  private def writeTarGz(fc: FileContext, path: String, contents: Array[Byte], multiplier: Int): Unit = {
    WithClose(fc.create(new Path(s"$path.tgz"), java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent())) { os =>
      WithClose(new BufferedOutputStream(os)) { buf =>
        WithClose(new GzipCompressorOutputStream(buf)) { gz =>
          WithClose(new TarArchiveOutputStream(gz)) { tar =>
            val entry = new TarArchiveEntry("file")
            entry.setSize(contents.length * multiplier)
            tar.putArchiveEntry(entry)
            var i = 0
            while (i < multiplier) {
              tar.write(contents)
              i += 1
            }
            tar.closeArchiveEntry()
            tar.finish()
          }
        }
      }
    }
  }
}
