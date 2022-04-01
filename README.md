# LOCAL ENVIRONMENT

## Setting up a local development environment

​
In order to run a local web server or run data pipeline steps on the command line, you'll need to set up a local development environment. This process is different from setting up production servers — we suggest containerizing all of those with Docker. Things like the web server will also need to run on productionized tools like gunicorn.
​
Operating systems supported by this documentation:
​

- Linux (Ubuntu)
- macOS
  ​

### System requirements

​

1. Download python. Any version 3.8 - 3.10 should be fine.
2. macOS: install [homebrew](https://brew.sh/).
3. Install docker.
   ​

   - macOS: Install [docker desktop](https://desktop.docker.com/mac/main/amd64/Docker.dmg?utm_source=docker&utm_medium=webreferral&utm_campaign=dd-smartbutton&utm_location=header) and start it by opening the app.

   - Ubuntu:

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

   ​

4. Ubuntu: update packages.
   ​
   ```
   sudo apt-get update # updates available package version list
   sudo apt-get upgrade # update packages
   sudo apt-get autoremove # remove old packages
   sudo do-release-upgrade # update os version
   ```
   ​
5. For mac, change the freetds version so python wheels will build correctly

```
brew unlink freetds
brew install freetds@0.91
brew link --force freetds@0.91
```

### Source code

​
Clone [Harmony](https://github.com/Zenysis/Harmony) repo: `git clone https://github.com/Zenysis/Harmony`. Alternatively, you may want to fork the repo and clone the fork — that way you can use version control for your customization.
​
For the brazil specific code, clone this repo and copy in the config/ and pipeline/ directories.

### Python dependencies

​

1. Update PYTHONPATH. In your bash profile (or z profile, etc.), set the PYTHONPATH environment variable to include the path to this codebase. Run `echo 'export PYTHONPATH="${PYTHONPATH}:<path to repo>"' >> ~/.bash_profile`. Note that anytime you update your bash profile, you either have to restart your terminal or run `source ~/.bash_profile`.
2. Creating a python3 virtual environment. Change into the source directory (ie ~/Harmony). Run the following snippet:
   ​
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
   If you see wheel-related errors here (on Ubuntu), run `pip install wheel` before iterating over the requirements files.
   ​
3. (Optional) Now we'll create the pypy Python virtual environment (which is a faster runtime that's used for our pipeline). Run the following:
   ​

   - macOS: `brew install pypy3`

   - Ubuntu: `sudo apt-get install pypy3 pypy3-dev`

   ```
   pypy3 -m venv venv_pypy3
   source venv_pypy3/bin/activate
   pip install --upgrade pip setuptools
   pip install -r requirements.txt
   pip install -r requirements-pipeline.txt
   deactivate
   ```

   ​
   To enter the normal virtual environment, run `source venv/bin/activate`. To enter the pypy one, run `source venv_pypy3/bin/activate`. The vast majority of development work will be done in the normal virtual environment.
   ​

### Javascript dependencies

​
We use [yarn](https://yarnpkg.com/) as a node.js package manager.
​

1. Install yarn.
   ​
   - macOS: `brew install yarn`
   - Ubuntu:
     ​
   ```
   curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
   echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
   sudo apt update && sudo apt install yarn
   ```
   ​
2. Install node.
   ​
   - macOS: `brew install node`
   - Ubuntu: `sudo apt install nodejs`
     ​
3. `yarn install` will install everything in `package.json`.
   ​

### Postgres

​

1. Install postgres.
   ​

   - macOS: `brew install postgresql`
   - Ubuntu: `sudo apt install postgresql postgresql-contrib`

2. Start postgres server:

   - macOS: `./scripts/db/postgres/dev/start_postgres.sh`
   - Ubuntu: `sudo systemctl start postgresql.service`

   For Ubuntu, the postgres permissions (the hba_file file) will also need to be updated:
   Run `sudo -u postgres psql -c "SHOW hba_file;"` to get the file location. Then, add the following lines to that file:

   ```
   host all all 127.0.0.1/32 trust
   host all all  ::1/0 trust
   ```

   Then restart the postgres cluster for the changes to take effect. Run `pg_lsclusters` to get the version and name of the cluster. Then run `sudo systemctl restart postgresql@<version>-<name>`
   ​

3. Enter psql client to check server success: `psql postgres`. If that does not work, try `sudo -u postgres psql postgres`.
   ​
4. Create a local postgres database: `create db "<project name>-local";` and seed its tables: `./scripts/db/postgres/dev/init_db.py <project name>` #TODO I think this requires the project name to be added somewherere to config

5. Populate the Data Catalog tables

`./scripts/db/postgres/dev/init_db.py <project name> --populate_indicators`

6. Create a user for your local web app.
   ​

```
$ ./scripts/create_user --help
​
Arguments:
​
-f first name
​
-l last name
​
-u email
​
-p password
​
-d postgresql://postgres:@localhost/{db_name}
```

### Start hasura

`./scripts/db/hasura/dev/start_hasura.sh <project name>-local`
​

### Druid and config setup

​
Specify druid host in global_config.py: `DEFAULT_DRUID_HOST = 'http://br-demo-druid.corp.clambda.com'`
​

### Run webpack

`webpack-dashboard -- webpack-dev-server --config web/webpack.config.js --mode 'development'`

### Run web server!

​
`ZEN_ENV=<project name>-local FLASK_ENV=development python ./web/runserver.py`

## Getting the pipeline code only running

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

## Other

### Graphql + hasura

GraphQL is a query language for APIs and a runtime for fulfilling those queries with your existing data.

Hasura is a GraphQL Engine / [GraphQL](https://hasura.io/graphql/) server that gives you instant, realtime GraphQL APIs over Postgres, with [webhook triggers](https://github.com/hasura/graphql-engine/blob/master/event-triggers.md) on database events, and [remote schemas](https://github.com/hasura/graphql-engine/blob/master/remote-schemas.md) for business logic.

Hasura engine console

[http://localhost:8088/console](http://localhost:8088/console)

![](https://static.slab.com/prod/uploads/rzv7xv5j/posts/images/3e40AEPw5b4MEbNxKDciPFwG.png)

### Adding lots of indicators

- Adding indicator groups en mass
  - Ui in the front end
  - Create a spreadsheet

| id     | name   | short_name | description | calculation | calculation_property | calculation_sub_property | category_id     | pipeline_datasource_id     |
| ------ | ------ | ---------- | ----------- | ----------- | -------------------- | ------------------------ | --------------- | -------------------------- |
| string | string | string     | string      | SUM         | NULL                 |                          | Id from mapping | Datasource id from mapping |
