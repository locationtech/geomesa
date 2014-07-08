package geomesa.core.filter

import geomesa.core.filter.FilterUtils._
import geomesa.core.index.IndexSchema
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, Interval}
import org.opengis.filter.Filter
import org.opengis.filter._
import scala.collection.JavaConversions._

object TestFilters {

  val baseFilters: Seq[Filter] =
    Seq(
      "INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
      "INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
      "NOT (INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))))",
      "NOT (INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))",
      "attr56 = val56",
      "geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z'",
      "geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z'"
    )

  val oneLevelAndFilters: Seq[Filter] =
    Seq(
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) AND attr17 = val17)",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))))",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND attr81 = val81)",
      "(attr15 = val15 AND INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))))"
    )

  val oneLevelOrFilters: Seq[Filter] =
    Seq(
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))))",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR attr4 = val4)",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) OR attr20 = val20)",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR attr36 = val36)",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) OR attr24 = val24)",
      "(attr100 = val100 OR INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR attr54 = val54)",
      "(attr19 = val19 OR attr75 = val75 OR attr72 = val72)",
      "(attr37 = val37 OR attr19 = val19)",
      "(attr44 = val44 OR INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) OR attr32 = val32)",
      "(attr95 = val95 OR INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))))"
    )

  val simpleNotFilters: Seq[Filter] =
    Seq(
      "NOT (INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))))",
      "NOT (INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))))",
      "NOT (INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))",
      "NOT (attr23 = val23)",
      "NOT (attr89 = val89)",
      "NOT (geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
      "NOT (geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z')"
    )

  val andsOrsFilters: Seq[Filter] =
    Seq(
      "((INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))) AND (geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR attr22 = val22 OR attr86 = val86) AND (geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z'))",
      "((geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))) OR (attr85 = val85 OR geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z') OR (INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) AND geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z'))",
      "(geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z' OR attr31 = val31 OR geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z' OR geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
      "((attr32 = val32 AND geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z') AND (INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) OR attr82 = val82 OR INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))))",
      "((attr44 = val44 AND INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))) OR (INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR attr2 = val2))",
      "(geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR attr51 = val51 OR attr39 = val39)"
    )

  val oneGeomFilters: Seq[Filter] =
    Seq(
      "((geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z') OR (geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))))",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z')",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) AND geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) AND geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z')",
      "(INTERSECTS(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
      "(attr84 = val84 AND geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))",
      "(geomesa_index_start_time BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z')",
      "(geomesa_index_start_time BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z' AND attr92 = val92 AND INTERSECTS(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))"
    )


}
