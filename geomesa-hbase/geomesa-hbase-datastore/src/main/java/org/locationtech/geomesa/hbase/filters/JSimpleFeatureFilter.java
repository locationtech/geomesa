package org.locationtech.geomesa.hbase.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.locationtech.geomesa.features.interop.SerializationOptions;

import java.io.IOException;

public class JSimpleFeatureFilter extends FilterBase {
    String sftString;
    SimpleFeatureType sft;
    KryoFeatureSerializer serializer;

    org.opengis.filter.Filter filter;
    String filterString;

    public JSimpleFeatureFilter(String sftString, String filterString) {
        this.sftString = sftString;
        configureSFT();
        this.filterString = filterString;
        configureFilter();
    }

    public JSimpleFeatureFilter(SimpleFeatureType sft, org.opengis.filter.Filter filter) {
        this.sft = sft;
        this.filter = filter;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        this.filterString = ECQL.toCQL(filter);
    }

    private void configureSFT() {
        sft = SimpleFeatureTypes.createType("QuickStart", sftString);
        serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId());
    }

    private void configureFilter() {
        if (filterString != null && filterString != "") {
            try {
                this.filter = ECQL.toFilter(this.filterString);
            } catch (CQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        byte[] encodedSF = CellUtil.cloneValue(v);
        SimpleFeature sf = serializer.deserialize(encodedSF);
        if (filter == null || filter.evaluate(sf)) {
            // Accept if we have no filter or if the filter passes
            return ReturnCode.INCLUDE;
        } else {
            return ReturnCode.SKIP;
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
        if (s != null) return s.length();
        else           return 0;
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        int sftLen =  Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(Bytes.copy(pbBytes, 4, sftLen));

        int filterLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        String filterString = new String(Bytes.copy(pbBytes, sftLen + 8, filterLen));

        return new JSimpleFeatureFilter(sftString, filterString);
    }

}