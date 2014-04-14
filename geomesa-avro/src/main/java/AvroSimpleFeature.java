import com.vividsolutions.jts.geom.Geometry;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.geotools.data.DataUtilities;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.identity.FeatureId;
import org.opengis.geometry.BoundingBox;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroSimpleFeature implements SimpleFeature {

    final protected SimpleFeatureType sft;
    final String[] names;
    final Object[] values;
    FeatureId id;
    final HashMap<String, Integer> nameIndex = new HashMap<>();
    final HashMap<Object, Object> userData = new HashMap<>();
    final Schema schema;
    public static final String FEATURE_ID_FIELD_NAME = "__fid__";

    public AvroSimpleFeature(FeatureId id, SimpleFeatureType sft) {
        this.id = id;
        this.values = new Object[sft.getAttributeCount()];
        this.names = new String[sft.getAttributeCount()];
        this.sft = sft;

        int i = 0;
        for (String name : DataUtilities.attributeNames(sft)) {
            names[i++] = name;
            nameIndex.put(name, sft.indexOf(name));
        }

        this.schema = AvroSimpleFeature.generateSchema(sft);
    }

    public static Schema generateSchema(SimpleFeatureType sft) {
        SchemaBuilder.FieldAssembler assembler = SchemaBuilder
                .record(sft.getTypeName())
                .namespace("org.geotools").fields()
                .name(FEATURE_ID_FIELD_NAME).type().stringType().noDefault();
        for (String name : DataUtilities.attributeNames(sft)) {
            assembler = assembler.name(name).type().stringType().noDefault();
        }
        return (Schema) assembler.endRecord();
    }

//    public static Schema generateSchema(String typeName, List<AttributeDescriptor> attributeDescriptorList){
//        SchemaBuilder.FieldAssembler assembler = SchemaBuilder
//                .record(typeName)
//                .namespace("org.geotools").fields()
//                .name("__id__").type().stringType().noDefault();
//        for(AttributeDescriptor descriptor: attributeDescriptorList){
//            final String attributeName = descriptor.getType().getName().toString();
//            //TODO determine type properly
//            final SchemaBuilder.FieldTypeBuilder fieldTypeBuilder = assembler.name(attributeName).type();
//            if(descriptor.isNillable()){
//                assembler = fieldTypeBuilder.nullable().stringType().noDefault();
//            }
//            else{
//                assembler = fieldTypeBuilder.stringType().noDefault();
//            }
//        }
//        return  (Schema) assembler.endRecord();
//    }

    public void write(OutputStream os) throws IOException {
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
        final GenericRecord me = new GenericData.Record(this.schema);
        me.put(FEATURE_ID_FIELD_NAME, this.getID());
        for (int i = 0; i < values.length; i++) {
            me.put(names[i], values[i]);
        }
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(this.schema);
        datumWriter.write(me, encoder);
        encoder.flush();
    }


    public SimpleFeatureType getFeatureType() {
        return sft;
    }

    public SimpleFeatureType getType() {
        return sft;
    }

    public void setID(String id) {
        ((FeatureIdImpl) this.id).setID(id);
    }

    public FeatureId getIdentifier() {
        return id;
    }

    public String getID() {
        return id.getID();
    }

    public Object getAttribute(String name) {
        return getAttribute(nameIndex.get(name));
    }

    public Object getAttribute(Name name) {
        return getAttribute(name.getLocalPart());
    }

    public Object getAttribute(int index) throws IndexOutOfBoundsException {
        return this.values[index];
    }

    public void setAttribute(String name, Object value) {
        int i = nameIndex.get(name);
        setAttribute(i, value);
    }

    public void setAttribute(Name name, Object value) {
        setAttribute(name.getLocalPart(), value);
    }

    public void setAttribute(int index, Object value)
            throws IndexOutOfBoundsException {
        values[index] = value;
    }

    public void setAttributes(List<Object> values) {
        for (int i = 0; i < values.size(); i++) {
            setAttribute(i, values.get(i));
        }
    }

    public int getAttributeCount() {
        return values.length;
    }

    public List<Object> getAttributes() {
        throw new UnsupportedOperationException();
    }

    public Object getDefaultGeometry() {
        GeometryDescriptor defaultGeometry = sft.getGeometryDescriptor();
        return defaultGeometry != null ? getAttribute(defaultGeometry.getName()) : null;
    }

    public void setAttributes(Object[] object) {
        throw new UnsupportedOperationException();
    }

    public void setDefaultGeometry(Object defaultGeometry) {
        GeometryDescriptor descriptor = sft.getGeometryDescriptor();
        setAttribute(descriptor.getName(), defaultGeometry);
    }

    public BoundingBox getBounds() {
        Object obj = getDefaultGeometry();
        if (obj instanceof Geometry) {
            Geometry geometry = (Geometry) obj;
            return new ReferencedEnvelope(geometry.getEnvelopeInternal(), sft.getCoordinateReferenceSystem());
        }
        return new ReferencedEnvelope(sft.getCoordinateReferenceSystem());
    }

    public GeometryAttribute getDefaultGeometryProperty() {
        throw new UnsupportedOperationException();
    }

    public void setDefaultGeometryProperty(GeometryAttribute defaultGeometry) {
        throw new UnsupportedOperationException();
    }

    public Collection<Property> getProperties() {
        throw new UnsupportedOperationException();
    }

    public Collection<Property> getProperties(Name name) {
        throw new UnsupportedOperationException();
    }

    public Collection<Property> getProperties(String name) {
        throw new UnsupportedOperationException();
    }

    public Property getProperty(Name name) {
        throw new UnsupportedOperationException();
    }

    public Property getProperty(String name) {
        throw new UnsupportedOperationException();
    }

    public Collection<? extends Property> getValue() {
        throw new UnsupportedOperationException();
    }

    public void setValue(Collection<Property> value) {
        throw new UnsupportedOperationException();
    }

    public AttributeDescriptor getDescriptor() {
        throw new UnsupportedOperationException();
    }

    public Name getName() {
        throw new UnsupportedOperationException();
    }

    public Map<Object, Object> getUserData() {
        return userData;
    }

    public boolean isNillable() {
        throw new UnsupportedOperationException();
    }

    public void setValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public void validate() {
    }

    /**
     * override of equals.  Returns if the passed in object is equal to this.
     *
     * @param obj the Object to test for equality.
     * @return <code>true</code> if the object is equal, <code>false</code>
     *         otherwise.
     */
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (!(obj instanceof AvroSimpleFeature)) {
            return false;
        }

        AvroSimpleFeature feat = (AvroSimpleFeature) obj;

        // this check shouldn't exist, by contract,
        //all features should have an ID.
        if (id == null) {
            if (feat.getIdentifier() != null) {
                return false;
            }
        }

        if (!id.equals(feat.getIdentifier())) {
            return false;
        }

        if (!feat.getFeatureType().equals(sft)) {
            return false;
        }

        for (int i = 0, ii = values.length; i < ii; i++) {
            Object otherAtt = feat.getAttribute(i);

            if (values[i] == null) {
                if (otherAtt != null) {
                    return false;
                }
            } else {
                if (!values[i].equals(otherAtt)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * returns a unique code for this feature
     *
     * @return A unique int
     */
    public int hashCode() {
        return id.hashCode() * sft.hashCode();
    }
}
