/* Copyright (c) 2001 - 2013 OpenPlans - www.openplans.org. All rights reserved.
* This code is licensed under the GPL 2.0 license, available at the root
* application directory.
*/
package geomesa.core.util;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.DataFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.identity.FeatureId;

import java.io.IOException;
import java.util.*;


/**
* Wraps multiple feature collections into a single.
*
* @author Justin Deoliveira, The Open Planning Project
*
*/
public class CompositeFeatureCollection extends DataFeatureCollection {
    final List<SimpleFeatureCollection> collections;

    public CompositeFeatureCollection(Collection<SimpleFeatureCollection> collections) {
        this.collections = new LinkedList<SimpleFeatureCollection>(collections);
    }

    public boolean add(SimpleFeatureCollection sf ) {
        return collections.add(sf);
    }

    protected Iterator openIterator() throws IOException {
        return new CompositeIterator();
    }

    public SimpleFeatureType getSchema() {
        return collections.iterator().next().getSchema();
    }

    public ReferencedEnvelope getBounds() {
        return DataUtilities.bounds(this);
    }

    public int getCount() throws IOException {
        int count = 0;
        Iterator i = iterator();

        try {
            while (i.hasNext()) {
                i.next();
                count++;
            }
        } finally {
            close(i);
        }

        return count;
    }

    class CompositeIterator implements Iterator {
        final Iterator<SimpleFeatureCollection> collectionIterator = collections.iterator();
        SimpleFeatureIterator iterator;

        public void remove() {
            throw new UnsupportedOperationException("Cannot remove features from composite feature collection");
        }

        public boolean hasNext() {
            //is there a current iterator that has another element
            if ((iterator != null) && iterator.hasNext()) {
                return true;
            }

            //get the next iterator
            while (collectionIterator.hasNext()) {
                //close current before we move to next
                if (iterator != null) {
                    iterator.close();
                }

                //grap next
                iterator = collectionIterator.next().features();

                if (iterator.hasNext()) {
                    return true;
                }
            }

            //no more
            if (iterator != null) {
                //close the last iterator
                iterator.close();
            }

            return false;
        }

        public Object next() {
            return iterator.next();
        }
    }

    public Object[] toArray(Object[] arg0) {
        List list = new ArrayList();

        Iterator it = collections.iterator();
        while(it.hasNext()){
            FeatureCollection col = (FeatureCollection)it.next();
            FeatureIterator it2 = col.features();
            while (it2.hasNext()){
                list.add(it.next());
            }
            it2.close();
        }

        return list.toArray(arg0);
    }

    public FeatureId getIdentifier() {
        throw new RuntimeException("Can't get the id for a composite featurecollection; you need to identify the consituent collections directly.");
    }
}
