# VERSION 1.10.7
# AUTHOR: Matthieu "Puckel_" Roisil
# MODIFIED: Sky McKinley (sgm7@)
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t devel/hsu-etl .
# RUN: docker run --name hsu-etl-dev -d -p 8081:8080 -v /home/sgm7/docker-containers/docker-airflow/dags:/usr/local/airflow/dags devel/hsu-etl
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.7-slim-stretch
LABEL maintainer="Puckel_"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.7
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS="oracle"
ARG PYTHON_DEPS="cx_Oracle pandas"
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
        libaio1 \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq apt-utils \
    && apt-get install -yqq \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        unzip \
        wget \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install 'redis==3.2' \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base \
    && wget -O /tmp/instantclient.zip https://download.oracle.com/otn_software/linux/instantclient/195000/instantclient-basiclite-linux.x64-19.5.0.0.0dbru.zip \
    && mkdir /opt/oracle \
    && cd /opt/oracle \
    && unzip /tmp/instantclient.zip 

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

ENV LD_LIBRARY_PATH /opt/oracle/instantclient_19_5:$LD_LIBRARY_PATH

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

RUN apt-get update
RUN apt-get install libaio1

# Airflow Connections
ENV AIRFLOW_CONN_SFTP_BAY_CLOVER sftp://cloversvc:xsw21qaz@bay.humboldt.edu:22

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
