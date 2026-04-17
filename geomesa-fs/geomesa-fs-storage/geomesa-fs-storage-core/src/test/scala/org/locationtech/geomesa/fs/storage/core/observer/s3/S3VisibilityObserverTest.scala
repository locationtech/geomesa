/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.observer
package s3

import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemContext
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.ArgumentCaptor
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{PutObjectTaggingRequest, PutObjectTaggingResponse, Tag}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.CompletableFuture

class S3VisibilityObserverTest extends SpecificationWithJUnit with Mockito {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("s3", "dtg:Date,*geom:Point:srid=4326")

  def feature(id: Int, vis: String): SimpleFeature = {
    val sf = ScalaSimpleFeature.create(sft, s"$id", "2020-01-01T00:00:00.000Z", "POINT (45 55)")
    SecurityUtils.setFeatureVisibility(sf, vis)
    sf
  }

  def mockS3(factory: S3VisibilityObserverFactory): S3AsyncClient = {
    val s3 = mock[S3AsyncClient]
    s3.putObjectTagging(any[PutObjectTaggingRequest]()) returns CompletableFuture.completedFuture(null: PutObjectTaggingResponse)
    val fs = new S3ObjectStore(s3)
    factory.init(FileSystemContext(fs, URI.create("s3a://foo/"), Map.empty), sft)
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
        val observer = factory.apply(new URI("s3a://foo/bar/baz.json"))
        observer(feature(0, "user"))
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
        val observer = factory.apply(new URI("s3a://foo/bar/baz.json"))
        observer(feature(0, "user"))
        observer(feature(1, "admin"))
        observer(feature(2, "user"))
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
        val observer = factory.apply(new URI("s3a://foo/bar/baz.json"))
        observer(feature(0, "user&admin"))
        observer(feature(1, "admin"))
        observer(feature(2, "user"))
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
