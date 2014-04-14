import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.geotools.data.DataUtilities;
import org.geotools.filter.identity.FeatureIdImpl;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.identity.FeatureId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class FeatureSpecificReader implements DatumReader<AvroSimpleFeature> {

    final HashSet<String> fieldsDesired;
    final Map<String, Class<?>> typeMap = new HashMap<>();
    final List<String> dataFields;
    final SimpleFeatureType newType;
    Schema oldSchema;
    final Schema newSchema;

    /**
     * Read all fields of a feature of the given type
     *
     * @param type
     */
    public FeatureSpecificReader(final SimpleFeatureType type) {
        this(type, type);
    }

    /**
     * Read records of oldType and conver them into newType
     *
     * @param oldType
     * @param newType
     */
    public FeatureSpecificReader(final SimpleFeatureType oldType, final SimpleFeatureType newType) {
        this.fieldsDesired = new HashSet<>(Arrays.asList(DataUtilities.attributeNames(newType)));
        this.oldSchema = AvroSimpleFeature.generateSchema(oldType);
        this.newSchema = AvroSimpleFeature.generateSchema(newType);
        this.newType = newType;

        this.dataFields = new ArrayList<>();
        for (final Schema.Field field : oldSchema.getFields()) {
            final String name = field.name();
            if (!name.equals(AvroSimpleFeature.FEATURE_ID_AVRO_FIELD_NAME)
                    && !name.equals(AvroSimpleFeature.AVRO_SIMPLE_FEATURE_VERSION)) {
                this.dataFields.add(name);
            }
        }

        for (final AttributeDescriptor attributeDescriptor : oldType.getAttributeDescriptors()) {
            final String name = attributeDescriptor.getLocalName();
            final Class<?> clazz = attributeDescriptor.getType().getBinding();
            typeMap.put(name, clazz);
        }
    }

    //TODO constructor with fieldIndexes
    //TODO read() using field IDs instead of just field names

    @Override
    public void setSchema(Schema schema) {
        this.oldSchema = schema;
    }

    @Override
    /**
     * returns ID + values in list (length attrs + 1 for id)
     **/
    public AvroSimpleFeature read(AvroSimpleFeature reuse, final Decoder in) throws IOException {
        if (reuse != null) {
            //TODO clear and reuse instead of creating new AvroSimpleFeature
        }

        // read the version first - since its 1st version throw it away
        in.readInt();

        //read the id always
        final FeatureId id = new FeatureIdImpl(in.readString());
        //TODO object reuse of FeatureId ?

        reuse = new AvroSimpleFeature(id, this.newType);

        if (this.dataFields.size() != this.fieldsDesired.size()) {
            // Rad ALL data fields keeping desired fields and skipping all others - make sure to consume entire record
            // since there may be records concatenated together
            for (final String field : this.dataFields) {
                setOrConsume(reuse, field, in);
            }
        } else {
            // Read all fields and set by index which should be in order
            for (String field : this.dataFields) {
                set(reuse, field, in);
            }
        }
        return reuse;
    }

    protected void set(AvroSimpleFeature reuse, String field, Decoder in) throws IOException {
        final Class<?> clazz = typeMap.get(field);
        if (clazz == String.class) {
            reuse.setAttribute(field, in.readString().toString());
        } else if (clazz == Integer.class) {
            reuse.setAttribute(field, in.readInt());
        } else if (clazz == Long.class) {
            reuse.setAttribute(field, in.readLong());
        } else if (clazz == Double.class) {
            reuse.setAttribute(field, in.readDouble());
        } else if (clazz == Float.class) {
            reuse.setAttribute(field, in.readFloat());
        } else if (clazz == Boolean.class) {
            reuse.setAttribute(field, in.readBoolean());
        } else if (clazz == UUID.class) {
            ByteBuffer bb = in.readBytes(null);
            //TODO convert to UUID
        } else if (clazz == Date.class) {
            // represented as a long as millis
            reuse.setAttribute(field, new Date(in.readLong()));
        } else {
            //TODO handle other things like shapes and points, etc.
        }
    }

    protected void setOrConsume(AvroSimpleFeature reuse, String field, Decoder in) throws IOException {
        final Class<?> clazz = typeMap.get(field);
        if (clazz == String.class) {
            if (fieldsDesired.contains(field)) {
                reuse.setAttribute(field, in.readString().toString());
            } else {
                in.skipString();
            }
        } else if (clazz == Integer.class) {
            if (fieldsDesired.contains(field)) {
                reuse.setAttribute(field, in.readInt());
            } else {
                in.readInt();
            }
        } else if (clazz == Long.class) {
            if (fieldsDesired.contains(field)) {
                reuse.setAttribute(field, in.readLong());
            } else {
                in.readLong();
            }
        } else if (clazz == Double.class) {
            if (fieldsDesired.contains(field)) {
                reuse.setAttribute(field, in.readDouble());
            } else {
                in.readDouble();
            }
        } else if (clazz == Float.class) {
            if (fieldsDesired.contains(field)) {
                reuse.setAttribute(field, in.readFloat());
            } else {
                in.readFloat();
            }
        } else if (clazz == Boolean.class) {
            if (fieldsDesired.contains(field)) {
                reuse.setAttribute(field, in.readBoolean());
            } else {
                in.readBoolean();
            }
        } else if (clazz == UUID.class) {
            if (fieldsDesired.contains(field)) {
                ByteBuffer bb = in.readBytes(null);
                //TODO convert to UUID
            } else {
                in.skipBytes();
            }
        } else if (clazz == Date.class) {
            // represented as a long as millis
            if (fieldsDesired.contains(field)) {
                reuse.setAttribute(field, new Date(in.readLong()));
            } else {
                in.readLong();
            }
        } else {
            //TODO handle other things like shapes and points, etc.
        }
    }

}
