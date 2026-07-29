/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class GeoMesaTrinoContainer extends TrinoContainer {

    public static final String TRINO_PLUGIN_PROPS = "geomesa-trino-plugin.properties";

    public static final DockerImageName IMAGE =
            DockerImageName.parse("trinodb/trino").withTag(System.getProperty("trino.docker.tag"));

    private static final Logger logger = LoggerFactory.getLogger(GeoMesaTrinoContainer.class);

    public GeoMesaTrinoContainer() {
        super(IMAGE);
    }

    public GeoMesaTrinoContainer withGeoMesaPlugin() {
        return withGeoMesaPlugin(findDistributedRuntime());
    }

    public GeoMesaTrinoContainer withGeoMesaPlugin(String jarHostPath) {
        logger.info("Binding to host path {}", jarHostPath);
        return (GeoMesaTrinoContainer) withFileSystemBind(jarHostPath,
                "/usr/lib/trino/plugin/iceberg-spatial/geomesa-trino-plugin.jar",
                BindMode.READ_ONLY);
    }

    private static String findDistributedRuntime() {
        String path = null;
        try {
            URL url = GeoMesaTrinoContainer.class.getClassLoader().getResource(TRINO_PLUGIN_PROPS);
            URI uri = url == null ? null : url.toURI();
            logger.debug("Trino plugin lookup: {}", uri);
            if (uri != null && uri.toString().endsWith("/target/classes/" + TRINO_PLUGIN_PROPS)) {
                // running through an IDE
                File targetDir = Paths.get(uri).toFile().getParentFile().getParentFile();
                File[] names = targetDir.listFiles((dir, name) ->
                        name.startsWith("geomesa-trino-plugin_") &&
                                (name.endsWith("-SNAPSHOT.jar") || name.matches(
                                        ".*-[0-9]+\\.[0-9]+\\.[0-9]+\\.jar")));
                if (names != null && names.length == 1) {
                    path = names[0].getAbsolutePath();
                }
            } else if (uri != null && "jar".equals(uri.getScheme())) {
                // running through maven
                String jar = uri.toString().substring(4).replaceAll("\\.jar!.*", ".jar");
                path = Paths.get(URI.create(jar)).toFile().getAbsolutePath();
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException("Could not load geomesa-accumulo-distributed-runtime JAR from classpath", e);
        }
        if (path == null) {
            throw new RuntimeException(
                    "Could not load geomesa-accumulo-distributed-runtime JAR from classpath");
        }
        return path;
    }
}
