FROM python:3.13-slim
ENV PYTHONIOENCODING utf-8

COPY /src /code/src/
COPY /tests /code/tests/
COPY /scripts /code/scripts/
COPY requirements.txt /code/requirements.txt
COPY flake8.cfg /code/flake8.cfg
COPY deploy.sh /code/deploy.sh

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
RUN apt-get update && apt-get install -y build-essential && apt-get install -y wget

RUN pip install flake8

RUN pip install -r /code/requirements.txt

# Prepare Oracle drivers
RUN wget -O /tmp/instantclient-basiclite.zip https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-basic-linux.x64-21.8.0.0.0dbru.zip
RUN wget -O /tmp/instantclient-sqlplus-linux.x64-21.8.0.0.0dbru.zip https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-sqlplus-linux.x64-21.8.0.0.0dbru.zip
RUN wget -O /tmp/instantclient-tools-linux.x64-21.8.0.0.0dbru.zip https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-tools-linux.x64-21.8.0.0.0dbru.zip


RUN apt-get update -q \
 && apt-get install -y unzip ssh libmcrypt-dev libaio1t64 wget --no-install-recommends

# Create symbolic link for libaio compatibility
# Oracle Instant Client expects libaio.so.1 but libaio1t64 provides libaio.so.1t64
# This link ensures Oracle components can find the required library
RUN ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1

# Oracle instantclient
#COPY --from=1 /tmp/instantclient-basiclite.zip /tmp/instantclient-basiclite.zip
#COPY --from=1 /tmp/instantclient-sdk.zip /tmp/instantclient-sdk.zip
#COPY --from=1 /tmp/instantclient-sqlplus.zip /tmp/instantclient-sqlplus.zip

RUN unzip /tmp/instantclient-basiclite.zip -d /usr/local/
RUN unzip /tmp/instantclient-sqlplus-linux.x64-21.8.0.0.0dbru.zip -d /usr/local/
RUN unzip /tmp/instantclient-tools-linux.x64-21.8.0.0.0dbru.zip -d /usr/local/

RUN ln -s /usr/local/instantclient_21_8 /usr/local/instantclient
RUN ln -s /usr/local/instantclient/sqldr /usr/local/instantclient/sqldr
RUN ln -s /usr/local/instantclient/sqlplus /usr/bin/sqlplus

# https://stackoverflow.com/questions/48527958/docker-error-getting-username-from-password-database
RUN useradd -u 1000 kbc_dummy

# https://stackoverflow.com/questions/66922967/problems-connecting-oracledb-from-aws-lambda-using-python-38#comment118302837_66922967
ENV LD_LIBRARY_PATH=/usr/local/instantclient
ENV PATH=/usr/local/instantclient:$PATH



WORKDIR /code/


CMD ["python", "-u", "/code/src/component.py"]