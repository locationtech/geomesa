/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

public class IcebergRestContainer extends GenericContainer<IcebergRestContainer> {
    public IcebergRestContainer(String s3AccessKeyId, String s3SecretAccessKey) {
        super(DockerImageName.parse("apache/iceberg-rest-fixture").withTag(System.getProperty("iceberg.rest.docker.tag")));
        setExposedPorts(List.of(8181));
        addEnv("CATALOG_WAREHOUSE", "s3://geomesa/iceberg/");
        addEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO");
        addEnv("CATALOG_S3_ENDPOINT", "http://minio:9000");
        addEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true");
        addEnv("AWS_REGION", "us-east-1");
        addEnv("AWS_ACCESS_KEY_ID", s3AccessKeyId);
        addEnv("AWS_SECRET_ACCESS_KEY", s3SecretAccessKey);
    }
}
