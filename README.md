# Brazil Training Repo

Contains:

- DataDigestApp
- Pipeline Code

## Getting the pipeline code running

1. Download python. Any version 3.6 - 3.10 should be fine.
2. Download pypy for our pipeline. Run `brew install pypy3`. The pypy installation and virtual environment setup is optional.
3. Update PYTHONPATH. In your bash profile (or z profile, etc.), set the PYTHONPATH environment variable to include the path to this codebase. Run `echo 'export PYTHONPATH="${PYTHONPATH}:<path to repo>"' >> ~/.bash_profile`. Note that anytime you update your bash profile, you either have to restart your terminal or run source ~/.bash_profile.
4. Creating a regular Virtual Environment. Change into the source directory. Run the following snippet:

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

5. Now we'll create the pypy Python virtual environment (which is a faster runtime that's used for our pipeline). Run the following:

```
pypy3 -m venv venv_pypy3
source venv_pypy3/bin/activate
pip install --upgrade pip setuptools
pip install -r requirements.txt
pip install -r requirements-pipeline.txt
deactivate
```

6. To enter the normal virtual environment, run `source venv/bin/activate`. To enter the pypy one, run `source venv_pypy3/bin/activate`.

## Running pipeline code

- All pipeline code can be run in the normal virtual environment. Pipeline steps that have `SetupEnvForPyPy` can also be run in the pypy virtual environment for improved performance. Not all steps can run be run with pypy due to incompatabilities with pandas.

- Use the syntax `./pipeline/brazil_covid/<step>/zeus_<step> run pipeline/brazil_covid/<step>/run/00_sim/<step>` for running pipeline steps. For example, `./pipeline/brazil_covid/generate/zeus_generate run pipeline/brazil_covid/generate/run/00_sim/00_fetch` or `./pipeline/brazil_covid/process/zeus_process run pipeline/brazil_covid/process/run/00_sim/05_convert`. Multiple steps can be run using a `*` wildcard.

- The steps interfacing with s3 (`/generate/.../10_sync.require_dir_success` or `/process/.../00_fetch*`) will not work because minio (our object storage) is not set up. Instead, all files that are necessary are in `pipeline/out/brazil_covid/process/feed/sim/20220217`. Edit the date folder name to match the current date (ie. 20220218) and the later process steps will be able to run.


# 2/24 Setting up harmony2.0
Add engineers to the repo as collab

Git clone repo: [git@github.com](mailto:git@github.com):Zenysis/harmony2.git



`export PYTHONPATH=/path/to/folder`

make venv

```
python3 -m venv venv
source venv/bin/activate
```

install python requirements

```
pip install --upgrade pip setuptools
pip install -r requirements.txt
pip install -r requirements-web.txt
pip install -r requirements-dev.txt
pip install -r requirements-pipeline.txt
```

install node requirements

`yarn install`

```
brew unlink freetds
brew install freetds@0.91
brew link --force freetds@0.91
```

**Merge br-covid changes in with harmony**

# Postgres

install postgres

`brew install postgresql`

start it

`./scripts/db/postgres/dev/start_postgres.sh`

enter psql cli

`psql postgres`

#create db

`create database “br_covid-local”;`

seed the postgres tables

`./scripts/db/postgres/dev/init_db.py br_covid`

## Database tables:: model scripts &amp; definitions

models/alchemy

**Query**

populate indicator tables

`./scripts/db/postgres/dev/init_db.py br_covid --populate_indicators`



- scripts/data_catalog/populate_query_models_from_config



**User**

`./scripts/create_user –help`

Arguments:

-f first name

-l last name

-u email

-p password

-d postgresql://postgres:@localhost/{db_name}



# Docker

Install docker desktop [mac](https://desktop.docker.com/mac/main/amd64/Docker.dmg?utm_source=docker&utm_medium=webreferral&utm_campaign=dd-smartbutton&utm_location=header)  [main page](https://www.docker.com/products/docker-desktop)

Start docker desktop

## Graphql + hasura

GraphQL is a query language for APIs and a runtime for fulfilling those queries with your existing data.

Hasura is a GraphQL Engine / [GraphQL](https://hasura.io/graphql/) server that gives you instant, realtime GraphQL APIs over Postgres, with [webhook triggers](https://github.com/hasura/graphql-engine/blob/master/event-triggers.md) on database events, and [remote schemas](https://github.com/hasura/graphql-engine/blob/master/remote-schemas.md) for business logic.

graphql/schema.graphql

Start hasura

`./scripts/db/hasura/dev/start_hasura.sh br-local`

Hasura engine console

[http://localhost:8088/console](http://localhost:8088/console)

![](https://static.slab.com/prod/uploads/rzv7xv5j/posts/images/3e40AEPw5b4MEbNxKDciPFwG.png)



**adding lots of indicators**

- Adding indicator groups en mass
    - Ui in the front end
    - Create a spreadsheet

| id | name | short_name | description | calculation | calculation_property | calculation_sub_property | category_id | pipeline_datasource_id |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| string | string | string | string | SUM | NULL |  | Id from mapping | Datasource id from mapping |



**SERVER**

specify druid host

in global_config.py

`DEFAULT_DRUID_HOST = 'http://br-demo-druid.corp.clambda.com'`

**run server**

`ZEN_ENV=br-local FLASK_ENV=development python ./web/runserver.py`


