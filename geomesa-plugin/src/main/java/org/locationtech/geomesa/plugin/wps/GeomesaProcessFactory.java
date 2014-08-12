package org.locationtech.geomesa.plugin.wps;

import org.apache.log4j.Logger;
import org.geoserver.wps.jts.SpringBeanProcessFactory;

public class GeomesaProcessFactory extends SpringBeanProcessFactory {
    private Logger log = Logger.getLogger(GeomesaProcessFactory.class);

    public GeomesaProcessFactory(String title, String namespace, Class markerInterface) {
        super(title, namespace, markerInterface);
        log.info("Created GeomesaProcessFactory");
    }
}
