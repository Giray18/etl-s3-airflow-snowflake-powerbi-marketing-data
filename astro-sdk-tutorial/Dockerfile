FROM quay.io/astronomer/astro-runtime:10.1.0

RUN pip install apache-airflow-providers-microsoft-mssql 

RUN pip install apache-airflow-providers-common-sql

RUN pip install apache-airflow-providers-odbc

# RUN apt-get update
# RUN apt-get install unixodbc-dev
# RUN pip install pyodbc

USER root
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc

  #Debian 11
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
# optional: for bcp and sqlcmd
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
# optional: for unixODBC development headers
RUN apt-get install -y unixodbc-dev

USER astro
