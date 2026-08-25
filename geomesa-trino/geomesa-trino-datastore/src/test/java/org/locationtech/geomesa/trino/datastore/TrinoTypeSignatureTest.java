/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.locationtech.geomesa.trino.datastore.TrinoTypeSignature.arrayElement;
import static org.locationtech.geomesa.trino.datastore.TrinoTypeSignature.isRowOrVariant;
import static org.locationtech.geomesa.trino.datastore.TrinoTypeSignature.isStructural;
import static org.locationtech.geomesa.trino.datastore.TrinoTypeSignature.mapKeyValue;
import static org.locationtech.geomesa.trino.datastore.TrinoTypeSignature.normalize;
import static org.locationtech.geomesa.trino.datastore.TrinoTypeSignature.scalarBinding;

/**
 * A hand-written reader of someone else's grammar earns its own tests. The last group
 * is the important one: it pins the safety property the class claims — corruption of
 * paren depth by a quoted identifier can only fail toward "not a typed collection",
 * never toward a typed binding the value would not satisfy.
 */
class TrinoTypeSignatureTest {

    @Test
    void normalizeHandlesNullAndCase() {
        assertThat(normalize(null)).isEmpty();
        assertThat(normalize("  ARRAY(VARCHAR) ")).isEqualTo("array(varchar)");
    }

    @Test
    void arrayElementIsReturnedRaw() {
        assertThat(arrayElement("array(varchar)")).isEqualTo("varchar");
        assertThat(arrayElement("array(row(a int))")).isEqualTo("row(a int)");
        assertThat(arrayElement("array(array(varchar))")).isEqualTo("array(varchar)");
    }

    @Test
    void arrayElementRejectsNonArrays() {
        assertThat(arrayElement("map(varchar,varchar)")).isNull();
        assertThat(arrayElement("varchar")).isNull();
        assertThat(arrayElement("")).isNull();
        assertThat(arrayElement("array(varchar")).isNull();   // unterminated
    }

    @Test
    void mapKeyValueSplitsAtTheTopLevelComma() {
        assertThat(mapKeyValue("map(varchar,varchar)")).containsExactly("varchar", "varchar");
        assertThat(mapKeyValue("map(varchar, double)")).containsExactly("varchar", " double");
    }

    @Test
    void mapKeyValueIgnoresCommasNestedInParens() {
        assertThat(mapKeyValue("map(varchar,row(a int, b int))"))
                .containsExactly("varchar", "row(a int, b int)");
        assertThat(mapKeyValue("map(row(a int, b int),varchar)"))
                .containsExactly("row(a int, b int)", "varchar");
        assertThat(mapKeyValue("map(varchar,decimal(10,2))"))
                .containsExactly("varchar", "decimal(10,2)");
    }

    @Test
    void mapKeyValueRejectsNonMapsAndDegenerateInput() {
        assertThat(mapKeyValue("array(varchar)")).isNull();
        assertThat(mapKeyValue("map(varchar)")).isNull();     // no comma
        assertThat(mapKeyValue("map(varchar,)")).isNull();    // trailing comma
        assertThat(mapKeyValue("map(,varchar)")).isNull();    // leading comma
    }

    @Test
    void structuralTypesAreRecognized() {
        assertThat(isStructural("array(varchar)")).isTrue();
        assertThat(isStructural("map(varchar,varchar)")).isTrue();
        assertThat(isStructural("row(a int)")).isTrue();
        assertThat(isStructural("variant")).isTrue();
        assertThat(isStructural("varchar")).isFalse();
        assertThat(isRowOrVariant("row(a int)")).isTrue();
        assertThat(isRowOrVariant("array(row(a int))")).isFalse();
    }

    @Test
    void scalarBindingsCoverWhatObjectTypeCanCarry() {
        assertThat(scalarBinding("varchar")).isEqualTo(String.class);
        assertThat(scalarBinding("varchar(64)")).isEqualTo(String.class);
        assertThat(scalarBinding("bigint")).isEqualTo(Long.class);
        assertThat(scalarBinding("integer")).isEqualTo(Integer.class);
        assertThat(scalarBinding("double")).isEqualTo(Double.class);
        assertThat(scalarBinding("real")).isEqualTo(Float.class);
        assertThat(scalarBinding("boolean")).isEqualTo(Boolean.class);
        assertThat(scalarBinding("uuid")).isEqualTo(UUID.class);
        assertThat(scalarBinding("varbinary")).isEqualTo(byte[].class);
        assertThat(scalarBinding("date")).isEqualTo(Date.class);
        assertThat(scalarBinding("timestamp(6)")).isEqualTo(Date.class);
        assertThat(scalarBinding("timestamp(6) with time zone")).isEqualTo(Date.class);
    }

    @Test
    void scalarBindingRefusesWhatGeoMesaCannotCarry() {
        assertThat(scalarBinding("decimal(10,2)")).isNull();   // no ObjectType for it
        assertThat(scalarBinding("row(a int)")).isNull();
        assertThat(scalarBinding("array(varchar)")).isNull();
        assertThat(scalarBinding("map(varchar,varchar)")).isNull();
        assertThat(scalarBinding("variant")).isNull();
        assertThat(scalarBinding("json")).isNull();
    }

    // ── quoted identifiers ───────────────────────────────────────────────
    //
    // Depth tracking skips double-quoted identifiers, so parens and commas inside a row
    // field name no longer corrupt the count and the split is correct. The mapper's end
    // state is unchanged: a row on either side is not scalar, so the column still goes
    // to JSON. Getting the split right is what frees the safety argument from having to
    // reason about where quotes can appear.

    @Test
    void quotedCloseParenDoesNotCorruptTheDepthCount() {
        assertThat(mapKeyValue("map(row(\"a)b\" int),varchar)"))
                .containsExactly("row(\"a)b\" int)", "varchar");
    }

    @Test
    void quotedOpenParenDoesNotCorruptTheDepthCount() {
        assertThat(mapKeyValue("map(row(\"x(\" int),varchar)"))
                .containsExactly("row(\"x(\" int)", "varchar");
    }

    @Test
    void quotedCommaIsNotTreatedAsTheSeparator() {
        assertThat(mapKeyValue("map(row(\"a,b\" int),varchar)"))
                .containsExactly("row(\"a,b\" int)", "varchar");
        assertThat(mapKeyValue("map(varchar,row(\"a,b\" int))"))
                .containsExactly("varchar", "row(\"a,b\" int)");
    }

    @Test
    void doubledQuoteEscapeNeedsNoSpecialCase() {
        // "a""b" is one identifier containing a quote; the two toggles cancel out.
        assertThat(mapKeyValue("map(row(\"a\"\"b\" int),varchar)"))
                .containsExactly("row(\"a\"\"b\" int)", "varchar");
    }

    @Test
    void aQuotedFieldNameStillNeverYieldsATypedBinding() {
        // However the split lands, a row side is not scalar, so the column goes to JSON.
        assertThat(scalarBinding("row(\"a)b\" int)")).isNull();
        assertThat(scalarBinding(arrayElement("array(row(\"weird(name\" int))"))).isNull();
    }
}
