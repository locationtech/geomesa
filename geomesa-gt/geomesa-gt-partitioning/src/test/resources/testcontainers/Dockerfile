ARG FROM_TAG=15-3.3
FROM postgis/postgis:${FROM_TAG}

# install pg_cron
RUN echo | sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh && \
    apt-get -y install postgresql-15-cron && \
    echo "\nshared_preload_libraries='pg_cron,pg_stat_statements'\n" >> /usr/share/postgresql/postgresql.conf.sample

# configure pg_cron
COPY initdb.sh /docker-entrypoint-initdb.d/initdb.sh
