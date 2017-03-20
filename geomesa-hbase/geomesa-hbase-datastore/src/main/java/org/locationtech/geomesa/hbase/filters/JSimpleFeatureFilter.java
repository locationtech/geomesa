/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *************************************************************************/

package org.locationtech.geomesa.hbase.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;

public class JSimpleFeatureFilter extends FilterBase {
    private String sftString;
    private SimpleFeatureType sft;
    private KryoFeatureSerializer serializer;

    private org.opengis.filter.Filter filter;
    private String filterString;

    public JSimpleFeatureFilter(String sftString, String filterString) {
        this.sftString = sftString;
        sft = SimpleFeatureTypes.createType("", sftString);
        serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId());

        this.filterString = filterString;
        if (filterString != null && filterString != "") {
            try {
                this.filter = ECQL.toFilter(this.filterString);
            } catch (CQLException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public JSimpleFeatureFilter(SimpleFeatureType sft, org.opengis.filter.Filter filter) {
        this.sft = sft;
        this.filter = filter;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        this.filterString = ECQL.toCQL(filter);
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (filter == null) {
            return ReturnCode.INCLUDE;
        } else {
            SimpleFeature sf = serializer.deserialize(v.getValueArray(), v.getValueOffset(), v.getValueLength());
            if (filter.evaluate(sf)) {
                return ReturnCode.INCLUDE;
            } else {
                return ReturnCode.SKIP;
            }
        }
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        return super.transformCell(v);
    }

    // TODO: Add static method to compute byte array from SFT and Filter.
    @Override
    public byte[] toByteArray() throws IOException {
      return Bytes.add(getLengthArray(sftString), getLengthArray(filterString));
    }

    private byte[] getLengthArray(String s) {
        int len = getLen(s);
        if (len == 0) {
            return Bytes.toBytes(0);
        } else {
            return Bytes.add(Bytes.toBytes(len), s.getBytes());
        }
    }

    private int getLen(String s) {
        if (s != null) {
            return s.length();
        } else {
            return 0;
        }
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        int sftLen =  Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(Bytes.copy(pbBytes, 4, sftLen));

        int filterLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        String filterString = new String(Bytes.copy(pbBytes, sftLen + 8, filterLen));

        return new JSimpleFeatureFilter(sftString, filterString);
    }

}
