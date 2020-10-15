# Analytics Warehouse

### Resources:

- [What exactly is DBT?](https://blog.getdbt.com/what--exactly--is-dbt-/)
- [Introduction to DBT](https://docs.getdbt.com/docs/introduction/)
- This structure was based on how dbt structures their projects: [dbt project structure guide](https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355)
- Best practices: [dbt suggested best practices](https://docs.getdbt.com/docs/guides/best-practices/)
- Suggested coding conventions: [dbt coding conventions](https://github.com/fishtown-analytics/corp/blob/master/dbt_coding_conventions.md)
- [DBT Viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)
- [DBT Commands](https://docs.getdbt.com/reference/dbt-commands/)

### Sample profiles.yml file to get the project to work locally:

Local profiles.yml file should be stored at ~/.dbt/profiles.yml
For more information on configuring the profiles.yml file, please see:
https://docs.getdbt.com/docs/profile

```
strive_health:
  outputs:
    dev:
      type: redshift
      threads: 4
      host: strive-health-redshift-cluster-1.ckveagnyu9df.us-east-1.redshift.amazonaws.com
      port: 5439
      user: <username>
      pass: <password>
      dbname: strive-dev
      schema: stage
    prod:
      type: redshift
      threads: 4
      host: strive-health-redshift-cluster-1.ckveagnyu9df.us-east-1.redshift.amazonaws.com
      port: 5439
      user: <username>
      pass: <password>
      dbname: strive-prod
      schema: stage
  target: dev

  ```

  ### Commands to Run the project:

  - `dbt debug` tests database connection
  - `dbt run` will run the project and materialize the models
  - `dbt test` will run the tests for the project
  - `dbt deps` installs packages (e.g., `dbt_utils`) found in the packages.yml needed to run the project and generate dbt docs
  - `dbt docs generate` will generate the documentation for the project
  - `dbt docs serve` will serve the documentation in the web browser at `http://localhost:8080`
