/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode; // note: don't remove, needed for javadocs
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature;
import org.locationtech.geomesa.filter.factory.FastFilterFactory;
import org.locationtech.geomesa.index.iterators.IteratorCache;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// Replaced with CqlTransformFilter
@Deprecated
public class JSimpleFeatureFilter extends FilterBase {

    public static int Priority = 30;

    private static Logger logger = LoggerFactory.getLogger(JSimpleFeatureFilter.class);

    private final Filter localFilter;
    private final KryoBufferSimpleFeature reusable;
    private final Transformer transformer;
    private String sftString;
    protected SimpleFeatureType sft;

    protected org.opengis.filter.Filter filter;
    private String filterString;

    private String transform;
    private String transformSchema;

    private interface Filter {
        ReturnCode filterKeyValue(Cell v) throws IOException;
        void setReusableSF(KryoBufferSimpleFeature reusableSF);
    }

    private static abstract class AbstractFilter implements JSimpleFeatureFilter.Filter {
        KryoBufferSimpleFeature sf;

        @Override
        public void setReusableSF(KryoBufferSimpleFeature reusableSF) {
            this.sf = reusableSF;
        }
    }

    private static class IncludeFilter extends JSimpleFeatureFilter.AbstractFilter {
        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            return ReturnCode.INCLUDE;
        }
    }

    private static class CQLFilter extends JSimpleFeatureFilter.AbstractFilter {
        private org.opengis.filter.Filter filter;
        private static Logger log = LoggerFactory.getLogger(CQLFilter.class);

        CQLFilter(org.opengis.filter.Filter filter) {
            this.filter = filter;
        }

        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            try {
                log.trace("Evaluating filter against SimpleFeature");
                if (filter.evaluate(sf)) {
                    return ReturnCode.INCLUDE;
                } else {
                    return ReturnCode.SKIP;
                }
            } catch(Exception e) {
                log.error("Exception thrown while scanning, skipping", e);
                return ReturnCode.SKIP;
            }

        }
    }

    private interface Transformer {
        Cell transformCell(Cell v) throws IOException;
        void setReusableSF(KryoBufferSimpleFeature reusableSF);
    }

    private static abstract class AbstractTransformer implements Transformer {

        KryoBufferSimpleFeature sf;
        public void setReusableSF(KryoBufferSimpleFeature reusableSF) {
            this.sf = reusableSF;
        }

    }

    private static class NoTransform extends AbstractTransformer {

        @Override
        public Cell transformCell(Cell v) throws IOException {
            return v;
        }
    }

    private static class CQLTransfomer extends AbstractTransformer {
        private final String transform;
        private final SimpleFeatureType schema;

        CQLTransfomer(String transform, SimpleFeatureType schema) {
            this.transform = transform;
            this.schema = schema;
        }

        @Override
        public void setReusableSF(KryoBufferSimpleFeature reusableSF) {
            super.setReusableSF(reusableSF);
            sf.setTransforms(transform, schema);
        }

        @Override
        public Cell transformCell(Cell c) throws IOException {
            byte[] newval = sf.transform();
            return new KeyValue(c.getRowArray(), c.getRowOffset(), c.getRowLength(),
                    c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength(),
                    c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(),
                    c.getTimestamp(),
                    KeyValue.Type.Put,
                    newval, 0, newval.length);
        }
    }

    private JSimpleFeatureFilter(String sftString,
                                 String filterString,
                                 String transformString,
                                 String transformSchemaString) throws CQLException {
        this.sftString = sftString;
        this.sft = IteratorCache.sft(sftString);
        this.reusable = IteratorCache.serializer(sftString, SerializationOptions.withoutId()).getReusableFeature();

        this.filterString = filterString;

        this.transformSchema = transformSchemaString;
        this.transform = transformString;

        this.localFilter = buildFilter(sft, filterString);
        this.transformer = buildTransformer(transform, transformSchema);

        this.localFilter.setReusableSF(reusable);
        this.transformer.setReusableSF(reusable);

        // TODO GEOMESA-1803 pass index type into filter from client and set featureID
    }

    public JSimpleFeatureFilter(SimpleFeatureType sft,
                                org.opengis.filter.Filter filter,
                                String transform,
                                String transformSchema) throws CQLException {
        this.sft = sft;
        this.filter = filter;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        this.filterString = ECQL.toCQL(filter);

        this.transformSchema = transformSchema;
        this.transform = transform;

        this.localFilter = buildFilter(sft, filterString);
        this.transformer = buildTransformer(transform, transformSchema);

        reusable = IteratorCache.serializer(sftString, SerializationOptions.withoutId()).getReusableFeature();

        this.localFilter.setReusableSF(reusable);
        this.transformer.setReusableSF(reusable);

        // TODO GEOMESA-1803 pass index type into filter from client and set featureID
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        // TODO GEOMESA-1804 is visibility filter first in the FilterList?
        // NOTE: the reusable sf buffer is set here and the filter and transformer depend on it
        reusable.setBuffer(v.getValueArray(), v.getValueOffset(), v.getValueLength());
        // TODO GEOMESA-1803 we need to set the id properly
        return localFilter.filterKeyValue(v);
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        return transformer.transformCell(v);
    }

    // TODO: Add static method to compute byte array from SFT and Filter.
    @Override
    public byte[] toByteArray() throws IOException {
        byte[][] arrays = {getLengthArray(sftString), getLengthArray(filterString), getLengthArray(transform), getLengthArray(transformSchema)};
        return Bytes.add(arrays);
    }

    @Override
    public String toString() {
        return "JSimpleFeatureFilter[filter=" + filterString + ",transform=" + transform + "]";
    }

    public static byte[] toByteArray(String sftString, String filterString, String transform, String transformSchema) throws IOException {
        byte[][] arrays = {getLengthArray(sftString), getLengthArray(filterString), getLengthArray(transform), getLengthArray(transformSchema)};
        return Bytes.add(arrays);
    }

    private static byte[] getLengthArray(String s) {
        int len = getLen(s);
        if (len == 0) {
            return Bytes.toBytes(0);
        } else {
            return Bytes.add(Bytes.toBytes(len), s.getBytes());
        }
    }

    private static int getLen(String s) {
        if (s != null) {
            return s.length();
        } else {
            return 0;
        }
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        logger.trace("Parsing filter of length: " + pbBytes.length);
        int sftLen =  Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(pbBytes, 4, sftLen);

        int filterLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        String filterString = new String(pbBytes, sftLen + 8, filterLen);

        int transformLen = Bytes.readAsInt(pbBytes, sftLen + filterLen + 8, 4);
        String transformString = new String(pbBytes, sftLen + filterLen + 12, transformLen);

        int transformSchemaLen = Bytes.readAsInt(pbBytes, sftLen + filterLen + transformLen + 12, 4);
        String transformSchemaString = new String(pbBytes, sftLen + filterLen + transformLen + 16, transformSchemaLen);


        try {
            return new JSimpleFeatureFilter(sftString, filterString, transformString, transformSchemaString);
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }

    private static JSimpleFeatureFilter.Filter buildFilter(SimpleFeatureType sft, String filterString) throws CQLException {
        if(!"".equals(filterString)) {
            return new CQLFilter(FastFilterFactory.toFilter(sft, filterString));
        } else {
            return new IncludeFilter();
        }
    }

    private static JSimpleFeatureFilter.Transformer buildTransformer(String transformString, String transformSchemaString) {
        if(Strings.isEmpty(transformString)) {
            return new NoTransform();
        } else {
            return new CQLTransfomer(transformString, SimpleFeatureTypes.createType("", transformSchemaString));
        }
    }

}
