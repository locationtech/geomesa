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
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
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
 * Translates GeoTools filters to Trino SQL WHERE clauses of pure row-level
 * {@code ST_*} function calls (plus temporal/attribute predicates). Geom column
 * references are always wrapped in {@code ST_GeomFromBinary(geom)} and geometry
 * literals in {@code ST_GeometryFromText('<wkt>')} — matches the shape recognized
 * by the {@code spatial_iceberg} connector's {@code SpatialConnectorMetadata.applyFilter}.
 *
 * <p>Swapped operand forms are normalized column-first via the OGC complement
 * ({@code within(a, b) ⇔ contains(b, a)}) so the connector's
 * {@code ST_*(ST_GeomFromBinary(col), literal)} recognizer always matches.
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

    /** For envelope→rectangle conversion (BBOX filters, DWithin outer bounds). */
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    /** Approximate meters per degree of latitude (and of longitude at the equator). */
    private static final double METERS_PER_DEGREE = 111_111.0;

    /** Outward margin on the DWITHIN outer envelope, absorbing flat-vs-spherical
     *  projection error so rows whose true distance is just under d aren't excluded. */
    private static final double OUTER_SAFETY_MARGIN = 1.1;

    /** Latitude (deg) beyond which the flat cos(lat) longitude scaling degenerates: as
     *  {@code cos(lat) -> 0} the per-degree-longitude distance vanishes, so a within-d region
     *  spans (nearly) all longitudes. Past this we use a full-longitude outer band (so the
     *  prefilter never drops matching rows). Also avoids the division-by-zero at exactly ±90°. */
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
     *  Within/Contains, which are normalized column-first via their OGC complement
     *  ({@code within(a, b) ⇔ contains(b, a)}). */
    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal literal, boolean swapped, Object extraData) {
        String col = property.getPropertyName();
        if (!(literal.getValue() instanceof Geometry geom)) {
            throw new IllegalArgumentException(
                "Unsupported spatial filter literal (expected a geometry): " + filter);
        }
        if (filter instanceof Intersects) {                  // symmetric
            writeSpatial("ST_Intersects", col, geom);
        } else if (filter instanceof Within) {
            writeSpatial(swapped ? "ST_Contains" : "ST_Within", col, geom);
        } else if (filter instanceof Contains) {
            writeSpatial(swapped ? "ST_Within" : "ST_Contains", col, geom);
        } else if (filter instanceof DWithin d) {            // symmetric
            writeDWithin(col, geom, convertToMeters(d.getDistance(), d.getDistanceUnits()));
        } else if (filter instanceof Beyond b) {             // symmetric
            writeBeyond(col, geom, convertToMeters(b.getDistance(), b.getDistanceUnits()));
        } else if (filter instanceof Crosses) {              // symmetric
            writeSpatial("ST_Crosses", col, geom);
        } else if (filter instanceof Touches) {              // symmetric
            writeSpatial("ST_Touches", col, geom);
        } else if (filter instanceof Overlaps) {             // symmetric
            writeSpatial("ST_Overlaps", col, geom);
        } else if (filter instanceof Equals) {               // symmetric
            writeSpatial("ST_Equals", col, geom);
        } else if (filter instanceof Disjoint) {             // symmetric
            writeSpatial("ST_Disjoint", col, geom);
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
     * Translates a BBOX filter into {@code ST_Intersects} against the envelope
     * rectangle — {@code BBOX(geom, env) ⇔ INTERSECTS(geom, envelope-rectangle)}.
     * The connector derives bbox/Z2 pruning from the call; the exact row-level test
     * keeps the predicate correct against the float32-rounded stored bboxes.
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
        Envelope env = new Envelope(b.getMinX(), b.getMaxX(), b.getMinY(), b.getMaxY());
        writeSpatial("ST_Intersects", col, GEOMETRY_FACTORY.toGeometry(env));
        return extraData;
    }

    /** The single spatial emission shape: {@code ST_Fn(ST_GeomFromBinary(col),
     *  ST_GeometryFromText('<wkt>'))}, column-first — exactly what the connector's
     *  {@code extractGeomColumnName}/{@code tryExtractEnvelope} recognize for
     *  bbox/Z2/XZ2 pushdown (ST_Disjoint is deliberately never pushed there). */
    private void writeSpatial(String function, String col, Geometry geom) {
        write(function + "(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(geom) + ")");
    }

    /**
     * DWithin translation: a necessary {@code ST_Intersects} against the outer
     * envelope rectangle (every geometry within d of the reference overlaps it, so
     * the connector derives bbox/Z2 pruning from the call) AND the exact spherical
     * distance check at row level.
     */
    private void writeDWithin(String col, Geometry refGeom, double distanceMeters) {

        // The reference envelope. For a point this degenerates to the point itself; for an
        // extended reference (linestring, polygon) the within-d region surrounds the WHOLE
        // geometry, so the outer box must be the envelope expanded by d — a radius-d box
        // around the centroid would wrongly prune rows near the reference's far ends.
        Envelope ref = refGeom.getEnvelopeInternal();

        // OUTER envelope: bounding box of "every point within d of ref". The safety margin
        // absorbs flat-vs-spherical projection error so we don't accidentally exclude rows
        // whose true distance is just under d.
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

        String refWkt = wkt.write(refGeom).replace("'", "''");
        String distanceCheck = String.format(Locale.ROOT, "%s <= %.0f",
            sphericalDistanceSql(col, refGeom, refWkt), distanceMeters);
        write("ST_Intersects(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(GEOMETRY_FACTORY.toGeometry(outer)) + ")"
            + " AND " + distanceCheck);
    }

    /** Beyond — DWithin's complement: exact spherical distance check, no prefilter
     *  (the matching rows are OUTSIDE the neighborhood, like ST_Disjoint's). */
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

    /** Fallback geometry column name when no PropertyName is available. */
    private String defaultGeomCol() {
        return featureType != null && featureType.getGeometryDescriptor() != null
            ? featureType.getGeometryDescriptor().getLocalName()
            : "geom";
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
