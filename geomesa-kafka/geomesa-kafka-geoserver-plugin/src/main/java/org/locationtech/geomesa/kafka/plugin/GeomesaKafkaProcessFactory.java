package org.locationtech.geomesa.kafka.plugin;

import org.geoserver.wps.jts.SpringBeanProcessFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeomesaKafkaProcessFactory extends SpringBeanProcessFactory {
    private Logger log = LoggerFactory.getLogger(GeomesaKafkaProcessFactory.class);

    public GeomesaKafkaProcessFactory(String title, String namespace, Class markerInterface) {
        super(title, namespace, markerInterface);
        log.info("Created GeomesaKafkaProcessFactory");
    }
}
