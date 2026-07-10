/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.filter.Id;
import org.geotools.api.filter.identity.Identifier;
import org.geotools.api.filter.spatial.BBOX;
import org.geotools.api.filter.spatial.Beyond;
import org.geotools.api.filter.spatial.BinarySpatialOperator;
import org.geotools.api.filter.spatial.Contains;
import org.geotools.api.filter.spatial.Crosses;
import org.geotools.api.filter.spatial.Disjoint;
import org.geotools.api.filter.spatial.DWithin;
import org.geotools.api.filter.spatial.Equals;
import org.geotools.api.filter.spatial.Intersects;
import org.geotools.api.filter.spatial.Overlaps;
import org.geotools.api.filter.spatial.Touches;
import org.geotools.api.filter.spatial.Within;
import org.geotools.api.filter.temporal.During;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.Literal;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.api.temporal.Period;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

/**
 * Translates GeoTools filters to Trino SQL WHERE clauses against tables that follow
 * this project's spatial-column convention: {@code geom VARBINARY} (raw WKB) plus
 * companions {@code __geom_bbox__ row(xmin, ymin, xmax, ymax)} and
 * {@code __geom_z2__ varchar} / {@code __geom_xz2__ varchar}. If using the spatial
 * catalog, applies bbox/Z2-stat pushdown on top of the same SQL via
 * {@code SpatialConnectorMetadata.applyFilter}.
 *
 * <p>Geom column references are always wrapped in {@code ST_GeomFromBinary(geom)}
 * before being handed to Trino's stock spatial functions.
 */
public class TrinoFilterToSQL extends FilterToSQL {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoFilterToSQL.class);

    /**
     * Creates a filter translator that double-quotes identifiers.
     */
    public TrinoFilterToSQL() {
        setSqlNameEscape("\"");  // quote identifiers in base-class comparisons too
    }

    /** Double-quote an identifier, doubling embedded quotes. */
    private static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /** {@code ST_GeometryFromText('<wkt>')} for a geometry, with any single quote in the WKT
     *  doubled. {@link WKTWriter} emits only numerics/keywords/punctuation today (no quotes),
     *  so this is defense-in-depth — it keeps the geometry literal on the same escaping
     *  discipline as identifiers ({@link #quoteIdent}), feature ids, and the visibility literal. */
    private String geomFromText(Geometry geom) {
        return "ST_GeometryFromText('" + wkt.write(geom).replace("'", "''") + "')";
    }

    /** Per-instance, NOT static: {@link WKTWriter} is not thread-safe, and a
     *  {@link TrinoFilterToSQL} is created fresh per query (see
     *  {@code TrinoFeatureSource.encodeFilterSql}), so an instance field is
     *  thread-confined. A shared static writer would corrupt output under
     *  concurrent queries. */
    private final WKTWriter wkt = new WKTWriter();

    /** Approximate meters per degree of latitude (and of longitude at the equator). */
    private static final double METERS_PER_DEGREE = 111_111.0;

    /** Outward margin on the DWITHIN outer bbox, absorbing flat-vs-spherical
     *  projection error so rows whose true distance is just under d aren't excluded. */
    private static final double OUTER_SAFETY_MARGIN = 1.1;

    /** Inward margin on the DWITHIN inner inscribed rectangle, applied conservatively
     *  so the bbox-contained shortcut never produces a false TRUE. */
    private static final double INNER_SAFETY_MARGIN = 0.9;

    /** Half-diagonal-to-half-side factor for a square inscribed in the d-circle. */
    private static final double INSCRIBED_FACTOR = 1.0 / Math.sqrt(2.0);

    /** Latitude (deg) beyond which the flat cos(lat) longitude scaling degenerates: as
     *  {@code cos(lat) -> 0} the per-degree-longitude distance vanishes, so a within-d region
     *  spans (nearly) all longitudes. Past this we use a full-longitude outer band (so the
     *  prefilter never drops matching rows) and drop the inscribed-rectangle shortcut (whose
     *  flat approximation is unsound near the poles), falling through to the exact spherical
     *  distance check. Also avoids the division-by-zero at exactly ±90°. */
    private static final double NEAR_POLE_LAT = 85.0;

    // ── Spatial ───────────────────────────────────────────────────────────────
    //
    // All binary spatial operators funnel through the base class's dispatcher: the
    // public visit(*) methods delegate to visitBinarySpatialOperator, which splits
    // the operands into the property/literal pair before calling the hook below. BBOX
    // keeps a dedicated visit() override because its literal is an envelope, not a
    // Geometry.

    /** Single translation point for every binary spatial operator. {@code swapped}
     *  means the geometry literal was expression1 (e.g. {@code INTERSECTS(POLYGON,
     *  geom)}) — irrelevant for symmetric operators, semantics-reversing for
     *  Within/Contains (which are each other's complement: {@code contains(a, b) ⇔
     *  within(b, a)}). Disjoint and Beyond get NO bbox prefilter */
    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal literal, boolean swapped, Object extraData) {
        String col = property.getPropertyName();
        if (!(literal.getValue() instanceof Geometry geom)) {
            throw new IllegalArgumentException(
                "Unsupported spatial filter literal (expected a geometry): " + filter);
        }
        if (filter instanceof Intersects) {                  // symmetric
            writeIntersects(col, geom);
        } else if (filter instanceof Within) {
            if (swapped) {
                writeLiteralWithinColumn(col, geom);
            } else {
                writeWithin(col, geom);
            }
        } else if (filter instanceof Contains) {
            if (swapped) {
                writeWithin(col, geom);                      // CONTAINS(lit, geom) ⇔ WITHIN(geom, lit)
            } else {
                writeColumnContainsLiteral(col, geom);
            }
        } else if (filter instanceof DWithin d) {            // symmetric
            writeDWithin(col, geom, convertToMeters(d.getDistance(), d.getDistanceUnits()));
        } else if (filter instanceof Beyond b) {             // symmetric
            writeBeyond(col, geom, convertToMeters(b.getDistance(), b.getDistanceUnits()));
        } else if (filter instanceof Crosses) {              // symmetric
            writeIntersectionImplyingOp("ST_Crosses", col, geom);
        } else if (filter instanceof Touches) {              // symmetric
            writeIntersectionImplyingOp("ST_Touches", col, geom);
        } else if (filter instanceof Overlaps) {             // symmetric
            writeIntersectionImplyingOp("ST_Overlaps", col, geom);
        } else if (filter instanceof Equals) {               // symmetric
            writeIntersectionImplyingOp("ST_Equals", col, geom);
        } else if (filter instanceof Disjoint) {             // symmetric
            write("ST_Disjoint(ST_GeomFromBinary(" + quoteIdent(col) + "),"
                + " " + geomFromText(geom) + ")");
        } else {
            throw new IllegalArgumentException("Unsupported spatial operator: " + filter);
        }
        return extraData;
    }

    /** Other operand shapes the base dispatcher couldn't split into a property/literal pair
     *  are unsupported. */
    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            Expression e1, Expression e2, Object extraData) {
        throw new IllegalArgumentException(
            "Unsupported spatial filter operands (expected one geometry literal and one property name): " + filter);
    }

    /**
     * Translates a BBOX filter into a bbox-overlap predicate.
     *
     * @param filter bbox filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(BBOX filter, Object extraData) {
        String col = filter.getExpression1() instanceof PropertyName pn
            ? pn.getPropertyName() : defaultGeomCol();
        BoundingBox b = filter.getBounds();
        // BBOX(geom, env) ⇔ INTERSECTS(geom, envelope-rectangle) — route through the
        // rectangle intersects emission (overlap prefilter + shrunk-contained shortcut
        // + exact ST_Intersects fallback). A bare float32 bbox-overlap is NOT exact:
        // the stored bbox is rounded to nearest, so it admits rows up to ½ ulp outside
        // the envelope (boundary rows made this visible in the filter-parity suite).
        Envelope env = new Envelope(b.getMinX(), b.getMaxX(), b.getMinY(), b.getMaxY());
        writeIntersects(col, new org.locationtech.jts.geom.GeometryFactory().toGeometry(env));
        return extraData;
    }

    /** WITHIN(geom, literal): rectangular query polygons take the shrunk-contained
     *  shortcut with an exact ST_Within fallback (see {@link #writeWithinRectangle});
     *  anything else emits bbox-overlap (Z2 pushdown) AND exact ST_Within at row
     *  level. Also serves swapped Contains
     *  ({@code CONTAINS(lit, geom) ⇔ WITHIN(geom, lit)}). */
    private void writeWithin(String col, Geometry geom) {
        if (geom instanceof Polygon p && p.isRectangle()) {
            writeWithinRectangle(col, p);
        } else {
            writeWithinNonRectangle(col, geom);
        }
    }

    /** DWithin translation: outer/inner bbox bounds plus an exact spherical distance check. */
    private void writeDWithin(String col, Geometry refGeom, double distanceMeters) {

        // The reference envelope. For a point this degenerates to the point itself; for an
        // extended reference (linestring, polygon) the within-d region surrounds the WHOLE
        // geometry, so the outer box must be the envelope expanded by d — a radius-d box
        // around the centroid would wrongly prune rows near the reference's far ends.
        Envelope ref = refGeom.getEnvelopeInternal();

        // OUTER bbox: bounding box of "every point within d of ref" — necessary-overlap
        // prefilter. The safety margin absorbs flat-vs-spherical projection error so we
        // don't accidentally exclude rows whose true distance is just under d.
        // Longitude scaling: degrees of longitude shrink toward the poles, so cos() is
        // evaluated at the POLEWARD EDGE of the expanded latitude band (not its center) —
        // the narrowest point, where the required longitude span is widest; sizing by the
        // center latitude under-covers for large radii at high latitudes and drops rows
        // (e.g. lat 60°, d=1000 km: cos(60°)/cos(69.9°) ≈ 1.46, past the 1.1 margin).
        // The near-pole gate is likewise evaluated at the band edge. Near a pole (or when
        // the span would wrap the full circle) the within-d region covers all longitudes,
        // so we use a full-longitude band rather than a bounded (row-dropping) span.
        double outerDegLat = (distanceMeters / METERS_PER_DEGREE) * OUTER_SAFETY_MARGIN;
        double polewardLat = Math.min(Math.max(Math.abs(ref.getMinY() - outerDegLat),
                                               Math.abs(ref.getMaxY() + outerDegLat)), 90.0);
        boolean nearPole = polewardLat >= NEAR_POLE_LAT;
        double outerDegLon = nearPole ? Double.POSITIVE_INFINITY
            : (distanceMeters / (METERS_PER_DEGREE * Math.cos(Math.toRadians(polewardLat)))) * OUTER_SAFETY_MARGIN;
        Envelope outer = (outerDegLon >= 180.0 || ref.getWidth() + 2 * outerDegLon >= 360.0)
            ? new Envelope(-180.0, 180.0, ref.getMinY() - outerDegLat, ref.getMaxY() + outerDegLat)
            : new Envelope(ref.getMinX() - outerDegLon, ref.getMaxX() + outerDegLon,
                           ref.getMinY() - outerDegLat, ref.getMaxY() + outerDegLat);

        String ptWkt = wkt.write(refGeom).replace("'", "''");
        String bboxCol = bboxColName(col);
        // Exact spherical distance — always correct, used as the fallback (and, for
        // non-point references and near the poles, the sole) row-level check.
        String distanceCheck = String.format(Locale.ROOT, "%s <= %.0f",
            sphericalDistanceSql(col, refGeom, ptWkt), distanceMeters);

        if (nearPole || !(refGeom instanceof Point)) {
            // No inscribed-rectangle shortcut here. Near the poles the flat approximation
            // is unsound where longitude lines converge. For an extended reference the
            // rectangle would sit on the envelope centroid — a point that need not lie ON
            // the geometry, so rectangle containment proves nothing about distance to it.
            // Every candidate row falls through to the exact distance check.
            write("(" + bboxOverlapSql(bboxCol, outer) + ") AND " + distanceCheck);
        } else {
            double lon = ((Point) refGeom).getX();
            double lat = ((Point) refGeom).getY();
            // INNER inscribed rectangle (point references only): half-sides (d ×
            // INNER_SAFETY_MARGIN × INSCRIBED_FACTOR) scaled to lat/lon. Corners land at
            // distance ≤ INNER_SAFETY_MARGIN × d from ref, so any bbox(geom) ⊆ this
            // rectangle ⇒ every point of geom is within d ⇒ TRUE.
            // Longitude scaling mirrors the outer box in the conservative direction: cos()
            // is evaluated at the EQUATORWARD EDGE of the band (its physically widest
            // point), so the rectangle's real half-width never exceeds the target anywhere
            // in the band — sizing by the center latitude lets the equatorward corners land
            // beyond d (e.g. lat 70°, d=1000 km: corner ≈ 1.03 d), wrongly including rows.
            double innerDegLat = (distanceMeters / METERS_PER_DEGREE) * INSCRIBED_FACTOR * INNER_SAFETY_MARGIN;
            double equatorwardLat = Math.max(Math.abs(lat) - innerDegLat, 0.0);
            double innerDegLon = (distanceMeters / (METERS_PER_DEGREE * Math.cos(Math.toRadians(equatorwardLat))))
                * INSCRIBED_FACTOR * INNER_SAFETY_MARGIN;
            Envelope inner = new Envelope(lon - innerDegLon, lon + innerDegLon,
                                          lat - innerDegLat, lat + innerDegLat);
            // Outer bbox-overlap (file/Z2 pruning) AND CASE WHEN inner-rectangle-contained
            // (sufficient for distance ≤ d) THEN TRUE ELSE exact spherical distance check.
            write("(" + bboxOverlapSql(bboxCol, outer) + ") AND "
                + "CASE WHEN " + bboxContainedSql(bboxCol, inner) + " THEN TRUE"
                + " ELSE " + distanceCheck + " END");
        }
    }

    // ── Other binary spatial operators ────────────────────────────────────────
    //
    // Crosses/Touches/Overlaps/Equals all imply a non-empty intersection, so each
    // gets the pushable bbox-overlap prefilter (Z2/file pruning) plus the exact
    // row-level ST_* test. No CASE WHEN shortcuts: envelope containment proves
    // nothing for these predicates (see the Intersects rectangle gate).

    /** {@code CONTAINS(geom, literal)} — the row geometry contains the literal, so
     *  the pushable prefilter is bbox-COVERS (the row bbox must contain the literal's
     *  envelope) plus exact {@code ST_Contains}. The literal-first form is handled in
     *  the dispatcher as {@code WITHIN(geom, literal)}. */
    private void writeColumnContainsLiteral(String col, Geometry geom) {
        String q = quoteIdent(bboxColName(col));
        Envelope env = geom.getEnvelopeInternal();
        // Float32-aligned bounds keep the covers-prefilter necessary against the
        // nearest-rounded stored bbox; see bboxOverlapSql.
        String bboxCovers = String.format(
            "%s.xmin <= %s AND %s.xmax >= %s AND %s.ymin <= %s AND %s.ymax >= %s",
            q, ceilF32(env.getMinX()), q, floorF32(env.getMaxX()),
            q, ceilF32(env.getMinY()), q, floorF32(env.getMaxY()));
        write("(" + bboxCovers + ")"
            + " AND ST_Contains(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(geom) + ")");
    }

    /** Beyond — DWithin's complement: exact spherical distance check, no prefilter. */
    private void writeBeyond(String col, Geometry refGeom, double distanceMeters) {
        String refWkt = wkt.write(refGeom).replace("'", "''");
        write(String.format(Locale.ROOT, "%s > %.0f",
            sphericalDistanceSql(col, refGeom, refWkt), distanceMeters));
    }

    /**
     * Spherical distance (meters) between the row geometry and a reference geometry.
     * Trino's spherical {@code ST_Distance} only accepts POINT inputs, so for an
     * extended reference the planar nearest pair is found first
     * ({@code geometry_nearest_points}) and the spherical distance is measured
     * between those two points — exact for point references, and a close
     * approximation for lines/polygons (the planar nearest point can deviate
     * slightly from the geodesic nearest at mid latitudes).
     */
    private String sphericalDistanceSql(String col, Geometry refGeom, String refWkt) {
        String geomExpr = "ST_GeomFromBinary(" + quoteIdent(col) + ")";
        String refExpr = "ST_GeometryFromText('" + refWkt + "')";
        if (refGeom instanceof Point) {
            return "ST_Distance(to_spherical_geography(" + geomExpr + "),"
                + " to_spherical_geography(" + refExpr + "))";
        }
        String np = "geometry_nearest_points(" + geomExpr + ", " + refExpr + ")";
        return "ST_Distance(to_spherical_geography(" + np + "[1]),"
            + " to_spherical_geography(" + np + "[2]))";
    }

    /** Shared shape for the intersection-implying operators (Crosses, Touches,
     *  Overlaps, Equals): the pushable bbox-overlap prefilter (necessary — each of
     *  these predicates implies a non-empty intersection, which implies overlapping
     *  envelopes) AND the exact row-level test. */
    private void writeIntersectionImplyingOp(String function, String col, Geometry geom) {
        write("(" + bboxOverlapSql(bboxColName(col), geom.getEnvelopeInternal()) + ")"
            + " AND " + function + "(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(geom) + ")");
    }

    // ── Temporal ──────────────────────────────────────────────────────────────

    /**
     * Translates a During filter into a timestamp range predicate.
     *
     * @param filter temporal during filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(During filter, Object extraData) {
        String col = ((PropertyName) filter.getExpression1()).getPropertyName();
        Period period = (Period) ((Literal) filter.getExpression2()).getValue();
        String begin = formatTimestamp(period.getBeginning().getPosition().getDate());
        String end   = formatTimestamp(period.getEnding().getPosition().getDate());
        write(String.format(
            "%s > TIMESTAMP '%s' AND %s < TIMESTAMP '%s'",
            quoteIdent(col), begin, quoteIdent(col), end));
        return extraData;
    }

    /**
     * Temporal literals must be TIMESTAMP-typed in the emitted SQL: the base class
     * writes {@link Date} values as quoted strings, which Trino cannot compare to a
     * timestamp-with-time-zone column. Covers BEFORE/AFTER, BETWEEN, and plain
     * comparison operators on date attributes (DURING has its own visit).
     *
     * @param literal the literal value to write
     */
    @Override
    protected void writeLiteral(Object literal) throws IOException {
        if (literal instanceof Date date) {
            out.write("TIMESTAMP '" + formatTimestamp(date) + "'");
        } else {
            super.writeLiteral(literal);
        }
    }

    // ── Feature ID ────────────────────────────────────────────────────────────

    /**
     * Translates an Id filter into a feature-id IN predicate.
     *
     * @param filter id filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(Id filter, Object extraData) {
        String ids = filter.getIdentifiers().stream()
            .map(Identifier::getID)
            .map(id -> "'" + id.toString().replace("'", "''") + "'")
            .collect(Collectors.joining(", "));
        write("\"__fid__\" IN (" + ids + ")");
        return extraData;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Emit the 4-clause necessary-overlap predicate against {@code __<col>_bbox__}.
     * Single source of truth for the bbox-overlap shape that SI's
     * {@code tryExtractBboxEnvelope} matches for file-level Z2 / bbox-stat pruning.
     */
    private void writeBboxOverlap(String col, Envelope env) {
        write(bboxOverlapSql(bboxColName(col), env));
    }

    /**
     * The 4-clause "bbox(geom) overlaps env" fragment. Necessary condition for any
     * geom/env interaction, and the exact shape SI's connector reads to push Z2
     * partition pruning + per-file bbox-stat pruning.
     *
     * <p>Bounds are emitted float32-aligned ({@link #floorF32}/{@link #ceilF32}):
     * the stored bbox values are float32 rounded to NEAREST, so comparing them
     * against the raw double bound can drop a row whose true coordinate is within
     * ~½ ulp inside the envelope edge (the stored value rounds across it). Float
     * rounding is monotone, so relaxing each bound to the nearest float in the
     * admitting direction keeps the prefilter necessary without admitting more
     * than one extra float32 quantum per side.
     */
    private static String bboxOverlapSql(String bboxCol, Envelope env) {
        String q = quoteIdent(bboxCol);
        return String.format(
            "%s.xmax >= %s AND %s.xmin <= %s" +
            " AND %s.ymax >= %s AND %s.ymin <= %s",
            q, floorF32(env.getMinX()), q, ceilF32(env.getMaxX()),
            q, floorF32(env.getMinY()), q, ceilF32(env.getMaxY()));
    }

    /** Largest float32 value ≤ v (as a double literal); see {@link #bboxOverlapSql}.
     *  Mirrors the connector's {@code pruneSafeLowerBound}. Exactly-representable
     *  values pass through unchanged. */
    private static double floorF32(double v) {
        float f = (float) v;
        return f > v ? Math.nextDown(f) : f;
    }

    /** Smallest float32 value ≥ v (as a double literal); see {@link #floorF32}. */
    private static double ceilF32(double v) {
        float f = (float) v;
        return f < v ? Math.nextUp(f) : f;
    }

    /** The 4-clause "bbox(geom) fully contained in env" fragment. */
    private static String bboxContainedSql(String bboxCol, Envelope env) {
        String q = quoteIdent(bboxCol);
        return String.format(
            "%s.xmin >= %s AND %s.xmax <= %s" +
            " AND %s.ymin >= %s AND %s.ymax <= %s",
            q, env.getMinX(), q, env.getMaxX(),
            q, env.getMinY(), q, env.getMaxY());
    }

    /** Returns the synthetic bbox struct column name for a given geometry column. */
    private static String bboxColName(String col) {
        return "__" + col + "_bbox__";
    }

    /** Fallback geometry column name when no PropertyName is available. */
    private String defaultGeomCol() {
        return featureType != null && featureType.getGeometryDescriptor() != null
            ? featureType.getGeometryDescriptor().getLocalName()
            : "geom";
    }

    /**
     * For an axis-aligned rectangular query polygon: bbox-overlap (necessary; lets
     * SI reconstruct the envelope and push Z2/bbox pruning) AND CASE WHEN the bbox
     * is contained in a slightly-SHRUNK rectangle THEN TRUE ELSE the exact
     * {@code ST_Within} test.
     *
     * <p>The shortcut cannot use the rectangle itself: WITHIN is boundary-exclusive
     * (a geometry touching the rectangle's edge is NOT within it), while the
     * inclusive bbox-contained comparison would admit it — boundary-snapped GPS
     * points made exactly that mismatch visible in the filter-parity suite. The
     * stored bbox is also float32 (rounded to nearest, up to ½ ulp off the true
     * bounds). Shrinking each side by two float ulps makes containment prove the
     * geometry lies strictly inside the rectangle; anything on or near the boundary
     * falls through to the exact row-level test.
     */
    private void writeWithinRectangle(String col, Polygon rect) {
        String bboxCol = bboxColName(col);
        Envelope env = rect.getEnvelopeInternal();
        Envelope shrunk = new Envelope(
            shrinkLo(env.getMinX()), shrinkHi(env.getMaxX()),
            shrinkLo(env.getMinY()), shrinkHi(env.getMaxY()));
        write("(" + bboxOverlapSql(bboxCol, env) + ") AND "
            + "CASE WHEN " + bboxContainedSql(bboxCol, shrunk) + " THEN TRUE"
            + " ELSE ST_Within(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(rect) + ") END");
    }

    /** A lower bound moved two float32 ulps inward; see {@link #writeWithinRectangle}. */
    private static double shrinkLo(double v) {
        return Math.nextUp(Math.nextUp((float) v));
    }

    /** An upper bound moved two float32 ulps inward; see {@link #writeWithinRectangle}. */
    private static double shrinkHi(double v) {
        return Math.nextDown(Math.nextDown((float) v));
    }

    /**
     * Non-rectangular polygons require the exact ST_Within row-level test —
     * bbox⊆env(polygon) doesn't prove geom⊆polygon since the polygon is a strict
     * subset of its envelope. We still emit bbox-overlap as a leading conjunct so
     * SI's connector can push Z2 partition pruning + bbox file-stat pruning.
     */
    private void writeWithinNonRectangle(String col, Geometry queryGeom) {
        String bboxCol = bboxColName(col);
        Envelope env = queryGeom.getEnvelopeInternal();
        write("(" + bboxOverlapSql(bboxCol, env) + ")"
            + " AND ST_Within(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(queryGeom) + ")");
    }

    /**
     * {@code WITHIN(literal, geom)} — the literal contained in the row geometry. The
     * necessary pushable prefilter is bbox-COVERS: the row bbox must contain the
     * literal's envelope (literal ⊆ geom ⇒ env(literal) ⊆ env(geom) = bbox). The exact
     * test is {@code ST_Within(literal, geom)} with the operands in the original order.
     */
    private void writeLiteralWithinColumn(String col, Geometry literalGeom) {
        String q = quoteIdent(bboxColName(col));
        Envelope env = literalGeom.getEnvelopeInternal();
        // Float32-aligned bounds keep the covers-prefilter necessary against the
        // nearest-rounded stored bbox; see bboxOverlapSql.
        String bboxCovers = String.format(
            "%s.xmin <= %s AND %s.xmax >= %s AND %s.ymin <= %s AND %s.ymax >= %s",
            q, ceilF32(env.getMinX()), q, floorF32(env.getMaxX()),
            q, ceilF32(env.getMinY()), q, floorF32(env.getMaxY()));
        write("(" + bboxCovers + ")"
            + " AND ST_Within(" + geomFromText(literalGeom) + ","
            + " ST_GeomFromBinary(" + quoteIdent(col) + "))");
    }

    /**
     * Emits a two-part predicate that gives both file-level Z2/bbox pruning AND
     * row-level short-circuiting:
     *
     * <pre>
     * (bbox-overlap)  AND  CASE WHEN bbox-contained THEN TRUE ELSE ST_Intersects(...) END
     * </pre>
     *
     * <p>The leading bbox-overlap conjunct is the same shape that
     * {@code SpatialConnectorMetadata.tryExtractBboxEnvelope} already recognizes —
     * the SI connector extracts the envelope from it and pushes Z2 partition
     * pruning + per-file bbox-stat pruning. The CASE WHEN tail survives Trino's
     * optimizer intact and short-circuits the WKB decode + ST_Intersects test
     * for any row whose bbox is fully inside the envelope.
     *
     * <p><b>The CASE WHEN shortcut applies ONLY to axis-aligned rectangular
     * query polygons</b> (where the polygon equals its envelope). For any other query
     * geometry, bbox-containment in the ENVELOPE does not imply intersection with the
     * GEOMETRY (examples, a polygon with a hole, a crescent, etc. contain
     * envelope regions outside the polygon), so the exact {@code ST_Intersects} runs for
     * every bbox-overlapping row instead. For rectangles:
     * <ul>
     *   <li>bbox-overlap=FALSE ⇒ ST_Intersects=FALSE, AND short-circuits, row excluded ✓</li>
     *   <li>bbox-overlap=TRUE, bbox-contained=TRUE ⇒ CASE returns TRUE, row included
     *       (bbox ⊆ rect ⇒ geom ⊆ rect ⇒ intersects) ✓</li>
     *   <li>bbox-overlap=TRUE, bbox-contained=FALSE ⇒ CASE returns exact ST_Intersects ✓</li>
     * </ul>
     *
     * <p><b>Why CASE not OR:</b> Trino's optimizer distributes OR over AND
     * ({@code (A AND B AND C AND D) OR X} → {@code (A OR X) AND (B OR X) AND (C OR X) AND (D OR X)}),
     * causing ST_Intersects to evaluate up to 4× per row (3.3× slowdown measured).
     * CASE WHEN is opaque to that rewrite.
     */
    private void writeIntersects(String col, Geometry geom) {
        String bboxCol = bboxColName(col);
        Envelope env = geom.getEnvelopeInternal();
        String exact = "ST_Intersects(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(geom) + ")";
        if (geom instanceof Polygon p && p.isRectangle()) {
            // Rectangle: bbox-overlap (necessary; pushable to file-level pruning) AND
            // CASE WHEN bbox-contained (sufficient — the polygon IS its envelope) THEN TRUE
            // ELSE exact ST_Intersects. CASE WHEN (not OR) survives Trino's optimizer intact.
            // The shortcut rectangle is shrunk by two float ulps per side: the stored
            // bbox is float32 rounded to nearest (up to ½ ulp off the true bounds), so
            // containment in the UNSHRUNK rectangle could return TRUE for a geometry a
            // hair outside it; shrunk containment proves true containment, and the
            // boundary shell falls through to the exact test.
            Envelope shrunk = new Envelope(
                shrinkLo(env.getMinX()), shrinkHi(env.getMaxX()),
                shrinkLo(env.getMinY()), shrinkHi(env.getMaxY()));
            write("(" + bboxOverlapSql(bboxCol, env) + ") AND "
                + "CASE WHEN " + bboxContainedSql(bboxCol, shrunk) + " THEN TRUE"
                + " ELSE " + exact + " END");
        } else {
            // Non-rectangular query: envelope containment alone doesn't imply intersection
            // with the geometry itself, so no row-level shortcut — just the
            // pushable bbox-overlap prefilter and the exact test.
            write("(" + bboxOverlapSql(bboxCol, env) + ") AND " + exact);
        }
    }

    private static String formatTimestamp(Date date) {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return fmt.format(date);
    }

    private static double convertToMeters(double distance, String units) {
        if (units == null) return distance;  // GeoTools default: treat as meters.
        return switch (units.toLowerCase(Locale.ROOT)) {
            case "meters", "m", "meter"     -> distance;
            case "kilometers", "km"         -> distance * 1000.0;
            case "feet", "ft"               -> distance * 0.3048;
            case "miles", "mi"              -> distance * 1609.344;
            case "nautical miles", "nm"     -> distance * 1852.0;
            default -> {
                LOG.warn("Unrecognized DWITHIN distance unit '" + units + "'; treating distance as meters.");
                yield distance;
            }
        };
    }

    private void write(String sql) {
        try {
            out.write(sql);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write SQL", e);
        }
    }

    /**
     * Override encodeToString to initialize {@code out} before visiting and
     * bypass the default capabilities check in {@link FilterToSQL#encode}.
     *
     * @param filter filter to translate
     * @return the SQL WHERE-clause fragment
     */
    @Override
    public String encodeToString(org.geotools.api.filter.Filter filter) throws FilterToSQLException {
        StringWriter sw = new StringWriter();
        out = sw;
        filter.accept(this, null);
        return sw.toString();
    }
}
