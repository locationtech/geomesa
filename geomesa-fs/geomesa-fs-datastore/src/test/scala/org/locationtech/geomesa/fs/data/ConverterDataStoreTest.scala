/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.io.IOUtils
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.FileSystemContext
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

import java.io.{BufferedOutputStream, StringReader}
import java.net.URI
import java.util.Properties

class ConverterDataStoreTest extends SpecificationWithJUnit with BeforeAfterAll {

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
      s"fs.options.converter.path=$path",
      "fs.partition-scheme.name=datetime",
      "fs.partition-scheme.opts.datetime-format=yyyy/DDD/HH/mm",
      "fs.partition-scheme.opts.step-unit=MINUTES",
      "fs.partition-scheme.opts.step=15",
      "fs.partition-scheme.opts.dtg-attribute=dtg",
      "fs.options.leaf-storage=true",
    )
    s"$converter\n${props.mkString("\n")}".stripMargin
  }

  def sftByName(name: String): String = {
    Seq(
      s"fs.options.sft.name=$name",
      s"fs.options.converter.name=$name",
    ).mkString("\n")
  }

  def sftByConf(conf: String): String = {
    Seq(
      s"fs.options.sft.conf=$conf",
      s"fs.options.converter.conf=$conf",
    ).mkString("\n")
  }

  "ConverterDataStore" should {
    "work with one datastore" in {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path"              -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding"          -> "converter",
        "fs.config.properties" -> fsConfig(sftByName("fs-test"), "datastore1")
      ).asJava)
      ds must not(beNull)

      val types = ds.getTypeNames
      types must haveSize(1)
      types.head mustEqual "fs-test"

      val q = new Query("fs-test", Filter.INCLUDE)
      val feats = CloseableIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
      feats must haveLength(4)
    }

    "work with something else" in {
      val params = Map(
        "fs.path"              -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding"          -> "converter",
        "fs.config.properties" -> fsConfig(sftByName("fs-test"), "datastore2")
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

    "read tar.gz files from s3 storage" in {
      val bucket = s"s3a://${this.bucket}/"
      val config = {
        val props = Seq(
          sftByName("fs-test"),
          "fs.s3a.endpoint.region=us-east-1",
          s"fs.s3a.endpoint=${minio.getS3URL}",
          s"fs.s3a.access.key=${minio.getUserName}",
          s"fs.s3a.secret.key=${minio.getPassword}",
          "fs.s3a.path.style.access=true",
          "dfs.client.use.datanode.hostname=true",
          "fs.s3a.connection.maximum=20" // reduce connection pool size to show resource leaks quickly
        ).mkString("\n")
        fsConfig(props, "datastore1")
      }
      val props = {
        val properties = new Properties()
        properties.load(new StringReader(config))
        properties.asScala.toMap
      }
      // number of times to write the sample files into our tgz
      // note: we need fairly large files to trigger GEOMESA-3411
      val multiplier = 177156
      val fsc = FileSystemContext.create(new URI(bucket), props)
      WithClose(ObjectStore(fsc)) { fs =>
        Seq("00", "15", "30", "45").foreach { file =>
          val path = s"datastore1/2017/001/01/$file"
          val contents = WithClose(getClass.getClassLoader.getResourceAsStream(s"example/$path"))(IOUtils.toByteArray)
          writePlain(fs, s"$bucket$path", contents, multiplier)
          writePlainGz(fs, s"$bucket$path", contents, multiplier)
          writeTarGz(fs, s"$bucket$path", contents, multiplier)
        }
      }

      foreach(Seq(("100 millis", true), ("5 minutes", false))) { case (timeout, expectTimeout) =>
        val params = Map(
          "fs.path"               -> bucket,
          "fs.encoding"           -> "converter",
          "fs.config.properties"  -> config,
          "fs.read-threads"       -> "12",
          "geomesa.query.timeout" -> timeout,
        )
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          ds must not(beNull)

          val types = ds.getTypeNames
          types must haveSize(1)
          types.head mustEqual "fs-test"

          foreach(Range(0, 5)) { _ =>
            val q = new Query("fs-test", Filter.INCLUDE)
            if (expectTimeout) {
              WithClose(CloseableIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)))(_.length) must throwAn[Exception]
            } else {
              WithClose(CloseableIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)))(_.length) mustEqual multiplier * 12
            }
          }
        }
      }
    }

    "load sft as a string" in {
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
        "fs.path"              -> this.getClass.getClassLoader.getResource("example").getFile,
        "fs.encoding"          -> "converter",
        "fs.config.properties" -> fsConfig(sftByConf(conf), "datastore1")
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

  private def writePlain(fs: ObjectStore, path: String, contents: Array[Byte], multiplier: Int): Unit = {
    WithClose(fs.overwrite(new URI(s"$path.csv"))) { os =>
      WithClose(new BufferedOutputStream(os)) { buf =>
        var i = 0
        while (i < multiplier) {
          buf.write(contents)
          i += 1
        }
      }
    }
  }

  private def writePlainGz(fs: ObjectStore, path: String, contents: Array[Byte], multiplier: Int): Unit = {
    WithClose(fs.overwrite(new URI(s"$path.gz"))) { os =>
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

  private def writeTarGz(fs: ObjectStore, path: String, contents: Array[Byte], multiplier: Int): Unit = {
    WithClose(fs.overwrite(new URI(s"$path.tgz"))) { os =>
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
