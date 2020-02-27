/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Base64

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{SetObjectTaggingRequest, Tag}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs._
import org.apache.hadoop.util.Progressable
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class S3VisibilityObserverTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("s3", "dtg:Date,*geom:Point:srid=4326")

  def feature(id: Int, vis: String): SimpleFeature = {
    val sf = ScalaSimpleFeature.create(sft, s"$id", "2020-01-01T00:00:00.000Z", "POINT (45 55)")
    SecurityUtils.setFeatureVisibility(sf, vis)
    sf
  }

  // noinspection NotImplementedCode
  class MockFileSystem extends FileSystem{

    val s3: AmazonS3 = mock[AmazonS3]

    override def getUri: URI = ???
    override def open(path: Path, i: Int): FSDataInputStream = ???
    override def create(path: Path, fsp: FsPermission, b: Boolean, i: Int, i1: Short, l: Long, p: Progressable): FSDataOutputStream = ???
    override def append(path: Path, i: Int, p: Progressable): FSDataOutputStream = ???
    override def rename(path: Path, path1: Path): Boolean = ???
    override def delete(path: Path, b: Boolean): Boolean = ???
    override def listStatus(path: Path): Array[FileStatus] = ???
    override def setWorkingDirectory(path: Path): Unit = ???
    override def getWorkingDirectory: Path = ???
    override def mkdirs(path: Path, fsp: FsPermission): Boolean = ???
    override def getFileStatus(path: Path): FileStatus = ???
  }

  "S3VisibilityObserver" should {

    "initialize factory correctly" >> {
      // mimic construction through reflection
      WithClose(classOf[S3VisibilityObserverFactory].newInstance()) { factory =>
        val root = mock[Path]
        root.getFileSystem(ArgumentMatchers.any()) returns new MockFileSystem()
        factory.init(new Configuration(), root, sft) must not(throwAn[Exception])
      }
    }

    "tag a single visibility label" >> {
      WithClose(new S3VisibilityObserverFactory) { factory =>
        val fs = new MockFileSystem()
        val root = mock[Path]
        root.getFileSystem(ArgumentMatchers.any()) returns fs
        factory.init(new Configuration(), root, sft)
        val observer = factory.apply(new Path("s3a://foo/bar/baz.json"))
        observer.write(feature(0, "user"))
        observer.close()

        val captor: ArgumentCaptor[SetObjectTaggingRequest] = ArgumentCaptor.forClass(classOf[SetObjectTaggingRequest])
        there was one(fs.s3).setObjectTagging(captor.capture())
        val request = captor.getValue
        request.getBucketName mustEqual "foo"
        request.getKey mustEqual "bar/baz.json"
        val encoded = Base64.getEncoder.encodeToString("user".getBytes(StandardCharsets.UTF_8))
        request.getTagging.getTagSet.asScala mustEqual Seq(new Tag(S3VisibilityObserverFactory.DefaultTag, encoded))
      }
    }

    "tag multiple visibility labels" >> {
      WithClose(new S3VisibilityObserverFactory) { factory =>
        val fs = new MockFileSystem()
        val root = mock[Path]
        root.getFileSystem(ArgumentMatchers.any()) returns fs
        factory.init(new Configuration(), root, sft)
        val observer = factory.apply(new Path("s3a://foo/bar/baz.json"))
        observer.write(feature(0, "user"))
        observer.write(feature(1, "admin"))
        observer.write(feature(2, "user"))
        observer.close()

        val captor: ArgumentCaptor[SetObjectTaggingRequest] = ArgumentCaptor.forClass(classOf[SetObjectTaggingRequest])
        there was one(fs.s3).setObjectTagging(captor.capture())
        val request = captor.getValue
        request.getBucketName mustEqual "foo"
        request.getKey mustEqual "bar/baz.json"
        // since the vis are kept in a set, the order is not defined
        val encodedFront = Base64.getEncoder.encodeToString("user&admin".getBytes(StandardCharsets.UTF_8))
        val encodedBack = Base64.getEncoder.encodeToString("admin&user".getBytes(StandardCharsets.UTF_8))
        request.getTagging.getTagSet.asScala must beOneOf(
          Seq(new Tag(S3VisibilityObserverFactory.DefaultTag, encodedFront)),
          Seq(new Tag(S3VisibilityObserverFactory.DefaultTag, encodedBack))
        )
      }
    }

    "simplify tag expressions" >> {
      WithClose(new S3VisibilityObserverFactory) { factory =>
        val fs = new MockFileSystem()
        val root = mock[Path]
        root.getFileSystem(ArgumentMatchers.any()) returns fs
        factory.init(new Configuration(), root, sft)
        val observer = factory.apply(new Path("s3a://foo/bar/baz.json"))
        observer.write(feature(0, "user&admin"))
        observer.write(feature(1, "admin"))
        observer.write(feature(2, "user"))
        observer.close()

        val captor: ArgumentCaptor[SetObjectTaggingRequest] = ArgumentCaptor.forClass(classOf[SetObjectTaggingRequest])
        there was one(fs.s3).setObjectTagging(captor.capture())
        val request = captor.getValue
        request.getBucketName mustEqual "foo"
        request.getKey mustEqual "bar/baz.json"
        // since the vis are kept in a set, the order is not defined
        val encodedFront = Base64.getEncoder.encodeToString("user&admin".getBytes(StandardCharsets.UTF_8))
        val encodedBack = Base64.getEncoder.encodeToString("admin&user".getBytes(StandardCharsets.UTF_8))
        request.getTagging.getTagSet.asScala must beOneOf(
          Seq(new Tag(S3VisibilityObserverFactory.DefaultTag, encodedFront)),
          Seq(new Tag(S3VisibilityObserverFactory.DefaultTag, encodedBack))
        )
      }
    }
  }
}
