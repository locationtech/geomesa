/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.s3

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.s3a.{S3AFileSystem, S3AInternals}
import org.geotools.api.feature.simple.SimpleFeature
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectTaggingRequest, Tag}

import java.nio.charset.StandardCharsets
import java.util.Base64

@RunWith(classOf[JUnitRunner])
class S3VisibilityObserverTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("s3", "dtg:Date,*geom:Point:srid=4326")

  def feature(id: Int, vis: String): SimpleFeature = {
    val sf = ScalaSimpleFeature.create(sft, s"$id", "2020-01-01T00:00:00.000Z", "POINT (45 55)")
    SecurityUtils.setFeatureVisibility(sf, vis)
    sf
  }

  def mockS3(factory: S3VisibilityObserverFactory): S3Client = {
    val root = mock[Path]
    val s3Internals = mock[S3AInternals]
    val s3 = mock[S3Client]
    val fs = new S3AFileSystem() {
      override def getS3AInternals: S3AInternals = s3Internals
    }

    root.getFileSystem(ArgumentMatchers.any()) returns fs
    s3Internals.getAmazonS3Client(ArgumentMatchers.any()) returns s3
    factory.init(new Configuration(), root, sft)
    s3
  }

  "S3VisibilityObserver" should {

    "initialize factory correctly" >> {
      // mimic construction through reflection
      WithClose(classOf[S3VisibilityObserverFactory].getDeclaredConstructor().newInstance()) { factory =>
        mockS3(factory) must not(throwAn[Exception])
      }
    }

    "tag a single visibility label" >> {
      WithClose(new S3VisibilityObserverFactory) { factory =>
        val s3 = mockS3(factory)
        val observer = factory.apply(new Path("s3a://foo/bar/baz.json"))
        observer.write(feature(0, "user"))
        observer.close()

        val captor: ArgumentCaptor[PutObjectTaggingRequest] = ArgumentCaptor.forClass(classOf[PutObjectTaggingRequest])
        there was one(s3).putObjectTagging(captor.capture())
        val request = captor.getValue
        request.bucket mustEqual "foo"
        request.key mustEqual "bar/baz.json"
        val encoded = Base64.getEncoder.encodeToString("user".getBytes(StandardCharsets.UTF_8))
        request.tagging.tagSet.asScala mustEqual
          Seq(Tag.builder.key(S3VisibilityObserverFactory.DefaultTag).value(encoded).build())
      }
    }

    "tag multiple visibility labels" >> {
      WithClose(new S3VisibilityObserverFactory) { factory =>
        val s3 = mockS3(factory)
        val observer = factory.apply(new Path("s3a://foo/bar/baz.json"))
        observer.write(feature(0, "user"))
        observer.write(feature(1, "admin"))
        observer.write(feature(2, "user"))
        observer.close()

        val captor: ArgumentCaptor[PutObjectTaggingRequest] = ArgumentCaptor.forClass(classOf[PutObjectTaggingRequest])
        there was one(s3).putObjectTagging(captor.capture())
        val request = captor.getValue
        request.bucket mustEqual "foo"
        request.key mustEqual "bar/baz.json"
        // since the vis are kept in a set, the order is not defined
        val encodedFront = Base64.getEncoder.encodeToString("user&admin".getBytes(StandardCharsets.UTF_8))
        val encodedBack = Base64.getEncoder.encodeToString("admin&user".getBytes(StandardCharsets.UTF_8))
        request.tagging.tagSet.asScala must beOneOf(
          Seq(Tag.builder.key(S3VisibilityObserverFactory.DefaultTag).value(encodedFront).build()),
          Seq(Tag.builder.key(S3VisibilityObserverFactory.DefaultTag).value(encodedBack).build())
        )
      }
    }

    "simplify tag expressions" >> {
      WithClose(new S3VisibilityObserverFactory) { factory =>
        val s3 = mockS3(factory)
        val observer = factory.apply(new Path("s3a://foo/bar/baz.json"))
        observer.write(feature(0, "user&admin"))
        observer.write(feature(1, "admin"))
        observer.write(feature(2, "user"))
        observer.close()

        val captor: ArgumentCaptor[PutObjectTaggingRequest] = ArgumentCaptor.forClass(classOf[PutObjectTaggingRequest])
        there was one(s3).putObjectTagging(captor.capture())
        val request = captor.getValue
        request.bucket mustEqual "foo"
        request.key mustEqual "bar/baz.json"
        // since the vis are kept in a set, the order is not defined
        val encodedFront = Base64.getEncoder.encodeToString("user&admin".getBytes(StandardCharsets.UTF_8))
        val encodedBack = Base64.getEncoder.encodeToString("admin&user".getBytes(StandardCharsets.UTF_8))
        request.tagging.tagSet.asScala must beOneOf(
          Seq(Tag.builder.key(S3VisibilityObserverFactory.DefaultTag).value(encodedFront).build()),
          Seq(Tag.builder.key(S3VisibilityObserverFactory.DefaultTag).value(encodedBack).build())
        )
      }
    }
  }
}
