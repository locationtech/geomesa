/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.fs

import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.fs.storage.core.FileSystemContext
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

import java.net.URI
import java.nio.charset.StandardCharsets

class S3ObjectStoreTest extends SpecificationWithJUnit with BeforeAfterAll {

  var minio: MinIOContainer = _

  lazy val conf = Map(
    "fs.s3.region" -> "us-east-1",
    "fs.s3.endpoint" -> minio.getS3URL,
    "fs.s3.access-key-id" -> minio.getUserName,
    "fs.s3.secret-access-key" -> minio.getPassword,
    "fs.s3.force-path-style" -> "true",
  )
  lazy val context = FileSystemContext(new URI("s3://geomesa/fs/"), conf, None)

  override def beforeAll(): Unit = {
    minio =
      new MinIOContainer(
        DockerImageName.parse("minio/minio").withTag(sys.props.getOrElse("minio.docker.tag", "RELEASE.2024-10-29T16-01-48Z")))
    minio.start()
    minio.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("minio")))
    minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName, minio.getPassword)
    minio.execInContainer("mc", "mb", "localhost/geomesa")
  }

  override def afterAll(): Unit = {
    if (minio != null) {
      minio.close()
    }
  }

  "S3ObjectStore" should {
    "prevent overwriting existing files in create" in {
      WithClose(ObjectStore(context)) { fs =>
        val file = new URI("s3://geomesa/fs/tmp.txt")
        val first = fs.create(file).orNull
        first must not(beNull)
        val second = fs.create(file).orNull
        second must not(beNull) // object doesn't exist yet as first request is still open
        first.write("foo".getBytes(StandardCharsets.UTF_8))
        second.write("bar".getBytes(StandardCharsets.UTF_8))
        second.close() must not(throwAn[Exception])
        first.close() must throwAn[Exception]
        WithClose(fs.read(file)) { opt =>
          opt must beSome
          IOUtils.toString(opt.get, StandardCharsets.UTF_8) mustEqual "bar"
        }
        fs.create(file) must beNone
      }
    }
  }
}
