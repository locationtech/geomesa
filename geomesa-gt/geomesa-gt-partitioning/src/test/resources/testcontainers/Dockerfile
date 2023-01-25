FROM postgis/postgis:15-3.3

# install pg_cron
RUN echo | sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh && \
    apt-get -y install postgresql-15-cron && \
    echo "\nshared_preload_libraries=pg_cron\n" >> /usr/share/postgresql/postgresql.conf.sample

# configure pg_cron
COPY initdb.sh /docker-entrypoint-initdb.d/pg_cron.sh
