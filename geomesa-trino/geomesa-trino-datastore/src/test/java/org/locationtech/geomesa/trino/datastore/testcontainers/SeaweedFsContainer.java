/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

public class SeaweedFsContainer extends GenericContainer<SeaweedFsContainer> {
    public SeaweedFsContainer(String s3AccessKeyId, String s3SecretAccessKey) {
        super(DockerImageName.parse("chrislusf/seaweedfs").withTag(System.getProperty("seaweed.docker.tag")));
         setCommand("mini", "-dir=/tmp/data");
        setExposedPorts(List.of(8333));
        setWaitStrategy(
            Wait.forHttp("/status")
                   .forPort(8333)
                   .forStatusCode(200)
        );
        addEnv("S3_BUCKET", "geomesa");
        addEnv("AWS_ACCESS_KEY_ID", s3AccessKeyId);
        addEnv("AWS_SECRET_ACCESS_KEY", s3SecretAccessKey);
    }

    public String getS3URL() {
        return "http://" + getHost() + ":" + getFirstMappedPort() + "/";
    }
}
