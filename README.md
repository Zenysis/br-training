# Documentation Contents

1. [Harmony overview](#harmony-overview)

Server/environment setup:

2. [Project setup](#project-initialization)
3. [Local development setup](#local-development-setup)
4. [Production pipeline server setup](#production-pipeline-server-setup)
5. [Production PostgreSQL server setup](#production-postgres-server-setup)
6. [Production web server setup](#production-web-server-setup)
7. [Production Druid server setup](#production-druid-server-setup)

Code base customization

8. [Writing integrations](#writing-integrations)
9. [Contributions](#contributions)

## Harmony overview

Harmony is an analytical platform that consists of two parts:

- A frontend for running queries, constructing dashboards, and performing other analytical duties, and
- A pipeline for ingesting incompatible data streams and transforming them into a standard, joinable format.

Data pipelines tend to run regularly and extract data from all sorts of data systems, ranging from Excel spreadsheets to custom SQL databases. After processing, data is dumped into a [Druid](https://druid.apache.org) database and then queried by the frontend.

## Project initialization

We refer to Harmony setups as "deployments". Each deployment has its own databases, data sources, and is hosted separately. 

The next step in setting up your Harmony project is to configure its deployment. Configurations are created in the `config/` directory. There is a configuration template in `config/template` and an example configuration in `config/br`. Typically, configuration directories are named after two or three-letter project codes. You'll now need to choose a project code and create your own configuration directory.


**Creating a new configuration**

To create a new configuration, copy the `config/template` directory into a new directory named for your project code.

At minimum, the following configuration files must be updated (documentation inside the files provides instructions):
- Define global constants in `global_config.py`
- Customize basic settings (e.g. site name) in `config/<project code>/general.py` and `config/<project code>/ui.py`

After you have written your first data integration (see [writing integrations](#writing-integrations)):
- Create dimensions for querying in `datatypes.py`
- Customize aggregation options in `aggregation.py`
- Add indicators based on the field ids you created in the pipeline step 

We are working on making this customization easier (and configurable from a frontend) in future releases.

**Misc. notes**

- Some of the config variables require your Druid and PostreSQL hosts to be set up. See [Production PostgreSQL server setup](#production-postgres-server-setup) and [Production Druid server setup](#production-druid-server-setup).

- When you run a script or the web server, select a configuration by setting the `ZEN_ENV` environmental variable. This environmental variable maps directly to folder names in `config/`, and will cause the `config` module to export the contents of that particular configuration.

      Say there is configuration directory named `usa`. We can specify that configuration with the following:

      ```bash
      export ZEN_ENV='usa'
      ```

## Local development setup

In order to run a local web server or run data pipeline steps on the command line, you'll need to set up a local development environment. This is distinct from setting up production servers (explained in other sections).

Operating systems supported by this documentation:

- Linux (Ubuntu)
- macOS

### Note: running only the pipeline code locally

If you are exclusively interested in running the pipeline locally, and not the web server, only the following steps are necessary: [system requirements](#system-requirements), [source code](#source-code), [Python dependencies](#python-dependencies), and [Druid setup](#druid-setup).

### System requirements

1. Install python (any version 3.8 - 3.10).
2. Update package managers.
   1. macOS: install [homebrew](https://brew.sh/).
   2. Ubuntu:
      ```
      sudo apt-get update # updates available package version list
      sudo apt-get upgrade # update packages
      sudo apt-get autoremove # remove old packages
      sudo do-release-upgrade # update os version
      ```
3. Install docker.

   1. macOS: Install [docker desktop](https://desktop.docker.com/mac/main/amd64/Docker.dmg?utm_source=docker&utm_medium=webreferral&utm_campaign=dd-smartbutton&utm_location=header) and start it by opening the app.
   2. Ubuntu:
      ```
      sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common
      curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
      sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
      sudo apt-get update
      sudo apt-get install -y docker-ce
      sudo service docker start
      sudo groupadd docker || true
      sudo gpasswd -a $USER docker
      ```

4. On macOS, change the freetds version so python wheels will build correctly.
   ```
   brew unlink freetds
   brew install freetds@0.91
   brew link --force freetds@0.91
   ```

### Source code

Clone repo: `git clone https://github.com/Zenysis/Harmony`. Alternatively, you may want to fork the repo and clone the fork — that way you can use version control for your customization.
​

### Python dependencies

1. Update `PYTHONPATH`. In your bash profile (or z profile, etc.), set the `PYTHONPATH` environment variable to include the path to your clone of Harmony. Run `echo 'export PYTHONPATH="${PYTHONPATH}:<path to repo>"' >> ~/.bash_profile` (or `.bashrc`, `.zshrc`, etc.). Note that anytime you update your bash profile, you either have to restart your terminal or run `source ~/.bash_profile`.

2. Create a python3 virtual environment. Change into the source directory (ie `~/Harmony`). Run the following:
   ```
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip setuptools
   pip install -r requirements.txt
   pip install -r requirements-pipeline.txt
   pip install -r requirements-web.txt
   pip install -r requirements-dev.txt
   deactivate
   ```
   ​
   If you see wheel-related errors here, run `pip install wheel` before iterating over the requirements files.
   ​
3. To enter the virtual environment, run `source venv/bin/activate`.
   ​

### Javascript dependencies

We use [yarn](https://yarnpkg.com/) as a node.js package manager.
​

1. Install yarn.
   ​
   - macOS: `brew install yarn`
   - Ubuntu:
     `curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add - echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list sudo apt update && sudo apt install yarn`
     ​
2. Install node.
   ​
   - macOS: `brew install node`
   - Ubuntu: `sudo apt install nodejs`
     ​
3. `yarn install` will install everything in `package.json`.
   ​

### Druid setup

​
Specify druid host in `global_config.py`: `DEFAULT_DRUID_HOST = '<public production Druid server IP>'`


### PostgreSQL

You will have to set up a [PostgreSQL database](https://www.postgresql.org/) to host relational data and web application state.

1.  Install postgres.

    - macOS: `brew install postgresql`
    - Ubuntu: `sudo apt install postgresql postgresql-contrib`

2.  Start postgres server:

    - macOS: `./scripts/db/postgres/dev/start_postgres.sh`
    - Ubuntu: `sudo systemctl start postgresql.service`

    For Ubuntu, the postgres permissions (the `hba_file` file) will also need to be updated:
    Run `sudo -u postgres psql -c "SHOW hba_file;"` to get the file location. Then, add the following lines to that file:

         ```
         host all all 127.0.0.1/32 trust
         host all all  ::1/0 trust
         ```

    Then restart the Postgres cluster for the changes to take effect. Run `pg_lsclusters` to get the version and name of the cluster. Then run `sudo systemctl restart postgresql@<version>-<name>`
    ​

3.  Enter psql client to check server success: `psql postgres`. If that does not work, try `sudo -u postgres psql postgres`.
    ​
4.  Create a local postgres database: `create db "<ZEN_ENV>-local";` and seed its tables: `./scripts/db/postgres/dev/init_db.py <ZEN_ENV>`

5.  Populate the Data Catalog tables

`./scripts/db/postgres/dev/init_db.py <ZEN_ENV> --populate_indicators`

6. Create a user for your local web app.

```
$ ./scripts/create_user.py -f <first name> -l <last name> -u <email> -p <password> -d <postgresql://postgres:@localhost/{ZEN_ENV}>
```

### Hasura

Start Hasura: `./scripts/db/hasura/dev/start_hasura.sh <ZEN_ENV>-local`

### Run webpack locally

Run command: `webpack-dashboard -- webpack-dev-server --config web/webpack.config.js --mode 'development'`

### Run web server locally

The platform is built on [Flask](http://flask.palletsprojects.com/en/1.1.x/). To run a local development server, run:

`ZEN_ENV=<ZEN_ENV>-local FLASK_ENV=development python ./web/runserver.py`

## Production pipeline server setup

The pipeline server runs the data pipeline to generate datasources (typically, daily). These pipeline server setup instructions were developed for Linux/Ubuntu.

1. Configure your server's users, firewall, etc. Sign in as root.
2. Update system packages. 
      ```
      sudo apt-get update # updates available package version list
      sudo apt-get upgrade # update packages
      sudo apt-get autoremove # remove old packages
      sudo do-release-upgrade # update os version
      ```
4. Install system dependencies.
      ```
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install --no-install-recommends -y \
       build-essential \
       curl \
       dtach \
       freetds-bin \
       freetds-dev \
       git \
       jq \
       libffi-dev \
       libgeos-dev \
       libssl-dev \
       lz4 \
       lzop \
       pigz \
       python3 \
       python3-dev \
       python3-levenshtein \
       python3-lxml \
       python3-venv \
       pypy3 \
       pypy3-dev \
       unzip \
       wget \
       libpq-dev \
       gfortran \
       libopenblas-dev \
       liblapack-dev
      apt-get clean 
      rm -rf /var/lib/apt/lists/* 
      curl \
       -o /usr/local/bin/mc \
       https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2021-11-16T20-37-36Z
      chmod 755 /usr/local/bin/mc
      ```
      (You may not need to install minio depending on your cloud storage choices.)
6. Clone your fork of the Harmony repo.
      `git clone <URL of Harmony clone>`
8. cd into the Harmony source directory and create two Python virtual environments. One is for regular python, one for pypy (which has a faster runtime and may be used for pipelines). 
      ```
      # First set up the normal python3 venv
      python3 -m venv venv
      source venv/bin/activate
      pip install --upgrade pip setuptools
      pip install -r requirements.txt
      pip install -r requirements-pipeline.txt
      pip install -r requirements-web.txt
      pip install -r requirements-dev.txt

      # Second set up the pypy venv
      deactivate
      pypy3 -m venv venv_pypy3
      source venv_pypy3/bin/activate
      pip install --upgrade pip setuptools
      pip install -r requirements.txt
      pip install -r requirements-pipeline.txt
      ```
10. Configure necessary permissions for your cloud storage service. For example, if you're using Minio, you'll need to set up `~/.mc/config` on the server.
11. Optionally, you may want to configure an automated task runner like GitLab, CircleCI, or Jenkins (to automate pipeline runs).

## Production Postgres server setup

Coming soon.

## Production web server setup

Coming soon.

## Production Druid server setup

Druid database is an OLAP database built to handle large analytical queries. It is:

- Column oriented, NoSQL built for analytical workloads
- Distributed and scalable (via Apache Zookeeper)
- Open source and customizable to many types of hardware

A new Druid collection is created every time the pipeline runs. This ensures that all datasets reflect a single common snapshot in time, and makes it possible to inspect historical records for tampering and other inconsistencies.

![](https://static.slab.com/prod/uploads/posts/images/h0kkzOZmq9pOgWwhc5n9T93t.png)

---

On an (Ubuntu) server dedicated to running Druid, follow these instructions:

```
sudo apt-get install -y docker-compose
mkdir druid_setup && cd druid_setup
wget https://raw.githubusercontent.com/apache/druid/0.22.1/distribution/docker/docker-compose.yml
wget https://raw.githubusercontent.com/apache/druid/0.22.1/distribution/docker/environment
docker-compose up -d
```

You should be able to see the Druid router console at http://{SERVER_IP}:8888/

## Writing integrations

This project comes with a built-in ETL pipeline that has been proven to work in a variety of global health contexts.

It provides a general framework for scraping data from any number of data sources or APIs, tools for standardizing this data in a common format (called Zenysis Base Format), and libraries for merging these disparate datasets together in such a way to make them mutually queryable.

![](https://docs.google.com/drawings/u/0/d/s9eL0gIYn0dyR8qhP9lif9w/image?w=624&h=261&rev=2&ac=1&parent=16hZpMQs0FUVEDTEcoXtOPM36sqTqCu8Mvb4vV9PLW1E)

Our data pipeline is based on [Zeus](https://github.com/room77/py77/tree/master/pylib/zeus), an open-source, command-line oriented pipeline runner. Note that Zeus processes files in order of numeric prefix, so given three tasks, `00_fetch_gender`, `00_fetch_sex`, and `03_convert`, Zeus will run 00_fetch_gender and 00_fetch_sex synchronously, then run 03_convert when both are complete.

On a technical level, a "data pipeline" is comprised by three stateless sub-pipelines: Generate → Process → Index.

**Generate**

In short: this pipeline runs queries and collects data from an external source.

The data generation pipeline is used for providing data sets to the process and validate pipelines in a stateless manner. Tasks in the data generation pipeline tend have specific requirements around network access (like running within a specific intranet), task duration (like long running machine learning jobs), or complex source data transformations (like a convoluted excel workbook that is rarely updated and only needs to be cleaned once) that make them unsuitable for running in the other pipelines.

Tasks within the data generation pipeline must handle data persistence themselves by uploading to object storage such as AWS S3 or Minio.

**Process**

In short: this pipeline integrates raw data, cleans it, and standardizes the keys.

This is where the majority of a deployment's data integration work happens. The data processing pipeline is used for transforming and merging multiple different data sets into a single common format that can be stored in a database by the index; pipeline. Dimension value matching and unification across sources will happen in this pipeline.

**Index**

The data indexing pipeline uploads the published data from the process pipeline into the datastores (like Druid and Postgres) used by the frontend for querying and data display.

### Formatting data for ingestion

The easiest way to prepare data for ingestion is by conforming to a flexible predefined CSV format that we refer to as **Zenysis Base Format**.

A `process_csv"` module is included in the repository at `data/pipeline/scripts/process_csv.py`. This module accepts tabular data in a variety of compatible formats and transforms it for fast indexing into the Druid database. In our experience, `process_csv` is flexible enough to handle a significant majority of health data sources in the development sector with little customization or editing.

Before we get into how you should format your data, let's define some terminology:

- **dimension**: A column that you want to group by or filter on. Examples of this are "Region" or "Gender." These columns represent categorical data and all values in these columns are strings.
- **date**: The date associated with this datapoint, as a string formatted YYYY-MM-DD.
- **field**: An indicator or dataset name. Usually it's best to [slugify](https://www.google.com/search?q=slugify&oq=slugify&aqs=chrome..69i57j0l5.503j0j0&sourceid=chrome&ie=UTF-8) these. For example, "Malaria Cases" may become "malaria_cases" or "MalariaCases." The values in these columns are numeric.

The simplest Zenysis Base Format is as follows:

```
dimension1, dimension2, ..., dimensionN, date, field1, field2, ..., fieldN
```

Here's an example set of columns and an example row:

| **RegionName** | **SubregionName** | **DistrictName** | **Date**   | **MalariaCases** | **MeaslesCases** | **NumberOfDoctors** |
| -------------- | ----------------- | ---------------- | ---------- | ---------------- | ---------------- | ------------------- |
| North America  | United States     | District 13      | 2019-01-01 | 150              | 0                | 20                  |
| North America  | Canada            | Albert           | 2019-02-15 | 0                | 2                | 1                   |

If you are integrating a single field, you may use the following format:

```
dimension1, dimension2, ..., dimensionN, date, field, val
```

Here's an example of valid input that follows that format:

| **RegionName** | **SubregionName** | **DistrictName** | **Date**   | **field**     | **val** |
| -------------- | ----------------- | ---------------- | ---------- | ------------- | ------- |
| North America  | United States     | District 13      | 2019-01-01 | population    | 150000  |
| North America  | United States     | District 13      | 2019-02-15 | malaria_cases | 150     |
| North America  | United States     | District 2       | 2019-02-15 | malaria_cases | 22      |

There is more to learn about the CSV processor - it supports a variety of formats, wildcards, and even Python hooks. See `data/pipeline/scripts/process_csv.py` for a README.

When calling process_csv, you must specify the `date` column, the `sourcename` (a label for your datasource), and the `prefix` for all indicators produced (usually the name of your datasource or some other informative tag). You will also have to specify some input and output files. You can call `process_csv` from your Python pipeline scripts directly, or invoke it on the command line.

## Contributions

Contributions are welcome! Use Github's Issues and Pull Requests features to report bugs, plan features, or submit changes.

We have an open [Google Group mailing list zenysis-harmony@googlegroups.com](https://groups.google.com/forum/#!forum/zenysis-harmony), which you can join or email with questions and other discussion. For general open source matters at Zenysis you may contact open-source@zenysis.com.
