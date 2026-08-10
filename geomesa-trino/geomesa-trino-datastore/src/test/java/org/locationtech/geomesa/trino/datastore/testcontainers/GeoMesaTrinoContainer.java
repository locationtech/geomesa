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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class GeoMesaTrinoContainer extends TrinoContainer {

    public static final DockerImageName IMAGE =
            DockerImageName.parse("trinodb/trino").withTag(System.getProperty("trino.docker.tag"));

    private static final Logger logger = LoggerFactory.getLogger(GeoMesaTrinoContainer.class);

    public GeoMesaTrinoContainer() {
        super(IMAGE);
    }

    public GeoMesaTrinoContainer withGeoMesaPlugin() {
        var pluginZip = System.getProperty("trino.plugin.path.2.12");
        if (pluginZip == null || pluginZip.isEmpty()) {
            pluginZip = System.getProperty("trino.plugin.path.2.13");
        }
        if (pluginZip == null) {
            throw new RuntimeException(
                    "Could not load 'trino.plugin.path' from sys properties - is surefire configured correctly?");
        }

        try {
            // create temp directory for extracted plugin files
            var tempPluginDir = Files.createTempDirectory("geomesa-trino-plugin-");
            logger.debug("Extracting plugin zip {} to {}", pluginZip, tempPluginDir);

            // extract zip file
            try (var zis = new ZipInputStream(Files.newInputStream(Paths.get(pluginZip)))) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (!entry.isDirectory()) {
                        Path targetPath = tempPluginDir.resolve(entry.getName());
                        Files.createDirectories(targetPath.getParent());
                        Files.copy(zis, targetPath);
                        targetPath.toFile().deleteOnExit();

                        // strip first directory component for container path
                        String entryName = entry.getName();
                        String fileName = entryName.contains("/") ?
                            entryName.substring(entryName.indexOf('/') + 1) : entryName;
                        String containerPath = "/usr/lib/trino/plugin/iceberg-spatial/" + fileName;
                        logger.debug("Mounting {} to {}", targetPath, containerPath);
                        // noinspection resource
                        withFileSystemBind(targetPath.toString(), containerPath, BindMode.READ_ONLY);
                    }
                    zis.closeEntry();
                }
            }

            // delete temp directory on exit (after its contents)
            tempPluginDir.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract plugin zip", e);
        }

        return this;
    }
}
