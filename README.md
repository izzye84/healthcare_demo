# Analytics Warehouse

### Resources:

- [What exactly is DBT?](https://blog.getdbt.com/what--exactly--is-dbt-/)
- [Introduction to DBT](https://docs.getdbt.com/docs/introduction/)
- This structure was based on how dbt structures their projects: [dbt project structure guide](https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355)
- Best practices: [dbt suggested best practices](https://docs.getdbt.com/docs/guides/best-practices/)
- Suggested coding conventions: [dbt coding conventions](https://github.com/fishtown-analytics/corp/blob/master/dbt_coding_conventions.md)
- [DBT Viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)
- [DBT Commands](https://docs.getdbt.com/reference/dbt-commands/)

### Local Setup
- Create an isolated python environment (e.g. [pyenv](https://some_companyhealth.atlassian.net/wiki/spaces/some_companyHEAL/pages/68649119/Python))

- Install dependencies (MacOS):
```
pip install dbt
```
Check your installation with `dbt --version`.

- Connect to our warehouse (currently Redshift):

1. Create a profiles.yml file and save it to  `~/.dbt/profiles.yml` (create this directory if it does not already exist)
For more information on configuring the profiles.yml file, please see:
https://docs.getdbt.com/docs/profile

2. Copy, paste, and save the following into your profiles.yml file (remember to replace username with your Redshift username):

```
some_company_health:
  outputs:
    dev:
      type: redshift
      threads: 8
      method: iam
      cluster_id: some_cluster_id
      host: some_host_url
      port: 5439
      user: <username>
      iam_profile: prod
      database: some_company-dev
      schema: public
    prod:
      type: redshift
      threads: 4
      host: some_other_host_url
      port: 5439
      user: data_load_service
      password:
      dbname: some_company-prod
      schema: stage
  target: dev

  ```

  3. Connect to some_company's VPN—access to the VPN can be requested at `IT@some_companyhealth.com`.

  4. Connect to AWS with your SSO.

  5. run `dbt debug` from the CLI to test the Redshift connection.

  6. run `dbt deps` to install packages (e.g., `dbt_utils`) found in the packages.yml needed to run the project and generate dbt docs.

  7. run `dbt seed` to load csv files located in the data-paths directory of your dbt project into your data warehouse.

  ### Common commands to Run the project:

  - `dbt debug` tests database connection
  - `dbt run` will run the project and materialize the models
  - Model selection syntax. Specifying models can save you a lot of time by only running/testing the models that you think are relevant:
    - `dbt run --models modelname` - will only run modelname
    - `dbt run --models +modelname` - will run modelname and all parents
    - `dbt run --models modelname+` - will run modelname and all children
    - `dbt run --models +modelname+` - will run modelname, and all parents and children
    - `dbt run --models @modelname` - will run modelname, all parents, all children, AND all parents of all children
    - `dbt run --exclude modelname` - will run all models except modelname
    - Note that all of these work with folder selection syntax too:
      - `dbt run --models folder` - will run all models in a folder
      - `dbt run --models folder.subfolder` - will run all models in the subfolder
      - `dbt run --models +folder.subfolder` - will run all models in the subfolder and all parents
  - `dbt test` will run the tests for the project
    - TIP: `dbt test` takes the same `--models` and `--exclude` syntax referenced for `dbt run`
  - `dbt docs generate` will generate the documentation for the project
  - `dbt docs serve` will serve the documentation in the web browser at `http://localhost:8080`

### Copying files and adding partitions to Redshift

1. Log into some_company's VPN
2. Connect to AWS with your SSO
3. `python utils/s3_file_copying/copy_new_files.py`
4. Confirm that the correct files were copied into the Analytics Warehouse S3 bucket
5. `python utils/s3_file_copying/add_partitions.py`

Logs are written to the `.file_copying_logs` directory.

#### Adding a new Partner to the file copying script

1. Add a `<partner>_ingested_files` like:
```
conviva_ingested_files = {
    re.search(r's3://some_company-analytics-warehouse-pro/(.+)', f[0]).group(1) for fs in ingested_files.get('raw_conviva', []) for f in fs
}
```
2. Log the ingested files like: (NOTE: This logging should be generalized)
```
log_str = 'Conviva files that have already been ingested:\n'
for f in conviva_ingested_files:
    log_str += f'{f}\n'
logging.debug(log_str)
```
3. Update `mapping`, using `humana` as an example
4. Add the Partner to the *List and Filter Source Files* section, using Humana as an example
