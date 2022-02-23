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
