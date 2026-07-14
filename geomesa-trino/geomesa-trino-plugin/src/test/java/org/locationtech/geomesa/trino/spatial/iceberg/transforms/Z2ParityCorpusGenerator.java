/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.transforms;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.geomesa.curve.interop.SpaceFillingCurves;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;

/**
 * One-shot corpus generator for the Python Z2 / XZ2 parity tests.
 *
 * <p>Run on demand (skipped in normal CI):
 * <pre>
 *   mvn -pl iceberg-spatial-tools test \
 *       -Dtest=Z2ParityCorpusGenerator \
 *       -Dcorpus.regenerate=true
 * </pre>
 *
 * <p>Output files (sibling, relative to repo root, resolved via {@code ../} from
 * the module working directory):
 * <ul>
 *   <li>{@code tools/tests/data/z2_parity_corpus.json}</li>
 *   <li>{@code tools/tests/data/xz2_parity_corpus.json}</li>
 * </ul>
 *
 * <p>Both corpora are emitted from the SAME geometry sequence; the Z2 corpus
 * keeps only the Point entries (Z2 rejects extended geometries) while the XZ2
 * corpus keeps everything. Values are sourced directly from the upstream
 * GeoMesa SFCs via {@link SpaceFillingCurves} so the corpus is the ground truth.
 * Each entry includes the raw SFC Long, the left-aligned hex encoding
 * ({@code Z2SFC.hexEncode}/{@code XZ2SFC.hexEncode} via {@link SpaceFillingCurves}),
 * and the WKB hex.
 */
class Z2ParityCorpusGenerator {

    @Test
    @EnabledIfSystemProperty(named = "corpus.regenerate", matches = "true")
    void generate() throws Exception {
        GeometryFactory gf = new GeometryFactory();
        WKBWriter wkbW = new WKBWriter();
        Geometry[] geoms = new Geometry[]{
            gf.createPoint(new Coordinate(0, 0)),
            gf.createPoint(new Coordinate(-180, -90)),
            gf.createPoint(new Coordinate(179.999, 89.999)),
            gf.createPoint(new Coordinate(-77.5, 38.9)),     // DC
            gf.createPoint(new Coordinate(139.7, 35.7)),     // Tokyo
            gf.createPolygon(new Coordinate[]{
                new Coordinate(-80, 35), new Coordinate(-70, 35),
                new Coordinate(-70, 45), new Coordinate(-80, 45),
                new Coordinate(-80, 35)}),
            gf.createPoint(new Coordinate(180.0, 0.0)),
            gf.createPoint(new Coordinate(0.0, 90.0)),
            gf.createPoint(new Coordinate(-157.5, 0.0)),
            // Centroids of the polygons below (Z2 corpus is point-only).
            gf.createPoint(new Coordinate(-75.0, 40.0)),
            gf.createPoint(new Coordinate(116.0005, 39.0005)),
            gf.createPoint(new Coordinate(115.0, 35.0)),
            polygonWkbGeom(gf, 116.0, 39.0, 116.001, 39.001),
            polygonWkbGeom(gf, 110.0, 30.0, 120.0, 40.0),
            polygonWkbGeom(gf, -170.0, -80.0, 170.0, 80.0),
            polygonWkbGeom(gf, -1.0, -1.0, 1.0, 1.0),
        };

        List<Geometry> all = new ArrayList<>(List.of(geoms));

        // Domain corners approached from inside at two epsilon scales.
        for (double eps : new double[]{1e-7, 1e-12}) {
            for (double lon : new double[]{-180 + eps, 180 - eps}) {
                for (double lat : new double[]{-90 + eps, 90 - eps}) {
                    all.add(gf.createPoint(new Coordinate(lon, lat)));
                }
            }
        }

        // Slightly out-of-bounds: pins clamping parity.
        all.add(gf.createPoint(new Coordinate(180.0000001, 39.0)));
        all.add(gf.createPoint(new Coordinate(-180.0000001, -39.0)));
        all.add(gf.createPoint(new Coordinate(116.0, 90.0000001)));
        all.add(gf.createPoint(new Coordinate(-77.0, -90.0000001)));
        all.add(polygonWkbGeom(gf, -180.0000001, 39.0, -179.0, 90.0000001));

        // Envelopes at/above/below 0.5^k stress the XZ2 l1 vs l1+1 code-length branch.
        for (int k = 1; k <= 12; k++) {
            double w = 360.0 * Math.pow(0.5, k);
            double h = 180.0 * Math.pow(0.5, k);
            all.add(polygonWkbGeom(gf, -10.0, -5.0, -10.0 + w, -5.0 + h));
            all.add(polygonWkbGeom(gf, -10.0, -5.0, -10.0 + Math.nextUp(w), -5.0 + Math.nextUp(h)));
            all.add(polygonWkbGeom(gf, 0.0, 0.0, w * 0.999, h * 0.999));
        }

        // Seeded random sweep.
        Random rnd = new Random(42L);
        for (int i = 0; i < 150; i++) {
            all.add(gf.createPoint(new Coordinate(
                -180 + 360 * rnd.nextDouble(), -90 + 180 * rnd.nextDouble())));
        }
        for (int i = 0; i < 150; i++) {
            double cx = -180 + 360 * rnd.nextDouble();
            double cy = -90 + 180 * rnd.nextDouble();
            double w = Math.pow(10, -6 + 8 * rnd.nextDouble());  // 1e-6 .. 1e2 deg
            double h = Math.pow(10, -6 + 8 * rnd.nextDouble());
            all.add(polygonWkbGeom(gf,
                Math.max(-180, cx - w / 2), Math.max(-90, cy - h / 2),
                Math.min(180, cx + w / 2), Math.min(90, cy + h / 2)));
        }
        geoms = all.toArray(new Geometry[0]);

        java.nio.file.Path z2Out = java.nio.file.Paths.get("../tools/tests/data/z2_parity_corpus.json")
            .toAbsolutePath().normalize();
        java.nio.file.Path xz2Out = z2Out.resolveSibling("xz2_parity_corpus.json");
        java.nio.file.Files.createDirectories(z2Out.getParent());

        int z2Count = 0;
        try (PrintWriter pw = new PrintWriter(z2Out.toFile())) {
            pw.println("[");
            boolean first = true;
            for (Geometry g : geoms) {
                // Z2 corpus is point-only; polygons stay for the XZ2 corpus below.
                if (!(g instanceof org.locationtech.jts.geom.Point pt)) continue;
                byte[] wkb = wkbW.write(g);
                String wkbHex = HexFormat.of().formatHex(wkb);
                long z2 = SpaceFillingCurves.z2Index(pt.getX(), pt.getY());
                String z2Hex = SpaceFillingCurves.z2Hex(pt.getX(), pt.getY());
                if (!first) pw.println(",");
                pw.printf("  {\"wkt\": \"%s\", \"wkb_hex\": \"%s\", \"z2\": %d, \"z2_hex\": \"%s\"}",
                    g.toText(), wkbHex, z2, z2Hex);
                first = false;
                z2Count++;
            }
            pw.println();
            pw.println("]");
        }
        System.out.println("Wrote " + z2Count + " entries to " + z2Out);

        int xz2Count = 0;
        try (PrintWriter pw = new PrintWriter(xz2Out.toFile())) {
            pw.println("[");
            boolean first = true;
            for (Geometry g : geoms) {
                byte[] wkb = wkbW.write(g);
                String wkbHex = HexFormat.of().formatHex(wkb);
                Envelope env = g.getEnvelopeInternal();
                long xz2 = SpaceFillingCurves.xz2Index(
                    env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY(), SpatialIndexRanges.G);
                String xz2Hex = SpaceFillingCurves.xz2Hex(
                    env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY(), SpatialIndexRanges.G);
                if (!first) pw.println(",");
                pw.printf("  {\"wkt\": \"%s\", \"wkb_hex\": \"%s\", \"xz2\": %d, \"xz2_hex\": \"%s\"}",
                    g.toText(), wkbHex, xz2, xz2Hex);
                first = false;
                xz2Count++;
            }
            pw.println();
            pw.println("]");
        }
        System.out.println("Wrote " + xz2Count + " entries to " + xz2Out);
    }

    private static Polygon polygonWkbGeom(GeometryFactory gf,
                                          double xmin, double ymin,
                                          double xmax, double ymax) {
        return gf.createPolygon(new Coordinate[]{
            new Coordinate(xmin, ymin), new Coordinate(xmax, ymin),
            new Coordinate(xmax, ymax), new Coordinate(xmin, ymax),
            new Coordinate(xmin, ymin)
        });
    }
}
