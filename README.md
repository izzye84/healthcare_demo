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
      type: postgres
      threads: 4
      host: 127.0.0.1
      port: 5432
      user: <username>
      pass: <pass>
      dbname: <db name>
      schema: dev
  target: dev
  ```
  
  This assumes you have a local instance of postgres running 
  with a database that contains a schema called `dev`.

  ### Commands to Run the project:
  
  - `dbt run` will run the project and materialize the models
  - `dbt test` will run the tests for the project
  - `dbt docs generate` will generate the documentation for the project
  - `dbt docs serve` will serve the documentation in the web browser at `http://localhost:8080`
  