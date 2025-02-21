- analytics engineer = mixture data analyst, data engineer
- introduces software engineering practices to the efforts of data analysts and data scientists
- dbt = data modelling tool
- data storing tools: snowflake, bigquery, redshift
- fct tables = facts about business process etc. "verbs" 
- dim tables = business entity, context "nouns" (customer)
- architecture of dimensional modeling
  - stage area: raw data (not exposed to everyone)
  - processing area: from raw data to data models; focus in efficiency, standards
  - presentation are: final presentation of the data to business stakeholder
- dbt = transformation workflow that allows anyone that knows SQL to deploy analytics code following software engineering best practices like moduarity, portability, CI/CD and documentation.

how does dbt work?
- each model is a .sql file
- select statement, no DDL or DML (what is this?)
- a file that dbt will compile and run in our DWH

(add schema of how dbt works)

