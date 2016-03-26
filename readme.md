# Access log aggregator script
This script is run by cron in daily, weekly or monthly mode (using `-m` flag).  
It starts a RedShift cluster, copies into it daily, weekly or monthly data from
the load balancer (the logs are stored on S3), runs aggregation queries specified
under `data_query_kinds` in the config file and saves the results into corresponding
tables in the analytics mysql server.

## Server requirements
- Install pip - `wget https://bootstrap.pypa.io/get-pip.py; sudo python get-pip.py; rm get-pip.py`
- install python and postgres libriaries - `sudo apt-get install -y libpq-dev python-dev`
- install s3cmd and virtualenv - `sudo -H pip install s3cmd virtualenv`
- Set up boto (aws) amd s3cmd credentials

## Installation and configuration
- `cd great_log_cruncher`
- `virtualenv venv`
- `source venv/bin/activate`
- `pip install -r requirements.txt`
- Create the `config.yml` file. Check `example_config.yml` for reference
- Make sure the server running the script could connect to redshift clusters and to the target db

## Running
- Daily - `(source venv/bin/activate; crunch_logs.py [-m daily])`
- Weekly - `(source venv/bin/activate; crunch_logs.py -m weekly)`
- Monthly - `(source venv/bin/activate; crunch_logs.py -m monthly)`
- To show which queries are going to be executed without starting a cluster, use the dry-run mode (-d flag).  
For example: `crunch_logs.py -m monthly -d`

## Adding a new query:
These are the steps needed to add a new query:
- Create 3 tables in the target db: `newquery_daily`, `newquery_weekly` and `newquery_monthly`
- Create 3 insert queries referencing these tables: `newquery_daily_insert`, `newquery_weekly_insert` and `newquery_monthly_insert`
- Create the new query and call it `newquery_select`
- Add the new query name (without _select) to config.yml under `data_query_kinds`
