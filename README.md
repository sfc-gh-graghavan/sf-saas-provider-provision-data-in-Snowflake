# Repository contains code samples to help SaaS Providers build private listings and provision data to their customers

Copyright &copy; 2023 Snowflake Inc. All rights reserved.

---
>This code is not part of the Snowflake Service and is governed by the terms in LICENSE.txt, unless expressly agreed to in writing.  Your use of this code is at your own risk, and Snowflake has no obligation to support your use of this code.
---


## WHAT IS IN THE REPOSITORY?
--------------------------

There are two folders - setup & stored-procedures

### setup

Contains setup.sql that will create a database (SAAS), schema (SAAS), REQUESTS and REQUESTS_FULFILLED tables (in the SAAS schema of the SAAS database).  Please change the database & schema name as needed.


### Stored Procedures

1. _sproc_for_data_partition.sql_ - Snowflake stored procedure written in Javascript to partition customer data by an identifier.  It creates a database and a schema to store the partitioned data by customer.  The output data product is/are dynamic table(s). Additionally the table REQUESTS_FULFILLED will contain all the audit for data provisioned.

This procedure reads a REQUESTS_STREAM table - which is basically enabling streams on REQUESTS table, in the calling DB & Schema to determine any new data requests submitted by the SaaS Provider's customers.

2. _sproc_for_creating_listings.sql_ - Snowflake stored procedure written in Javascript to create private listings with dynamic tables created by the previous stored procedures.

This procedure needs two parameters - "customer_name" and "account_name".  The Snowflake Account Identifier is in the form [ORGNAME].[ACCOUNTNAME].  The "customer_name" used in this procedure is the [ACCOUNTNAME] component of the Snowflake Account Identifier.  The "account_name" is the Snowflake Account Identifier - which is [ORGNAME].[ACCOUNTNAME].  These can be changed as needed based on your data profile.

_sproc_for_data_partition.sql_ calls _sproc_for_creating_listings.sql_.

USAGE
---
1. Ensure that both REQUESTS and REQUESTS_FULFILLED tables are created in the calling database.  The database used by these code samples is SAAS and the schema is also SAAS.  This should be modified to suit your naming conventions and best practices.
2. Ensure that change_tracking is set to true and create streams (REQUESTS_STREAM) on the REQUESTS table.
3. Create the stored procedures in the calling database/schema (SAAS/SAAS is used in these sample scripts) by running both the _sproc_for_data_partition.sql_ and _sproc_for_creating_listings.sql_.
4. Create a task to monitor the REQUESTS_STREAM and execute the _partition_customer_data()_ stored procedure (which is built by running the _sproc_for_data_partition.sql_).  This procedure will partition the customer's data into Dynamic Tables and store them in the database created for the customer.  This procedure will also call _provision_data()_ (which is built by running the _sproc_for_creating_listings.sql_) to create the private listings and target the appropriate customer's Snowflake account.

MISCELLANEOUS
---
Currently Dynamic Tables are not supported for SubDB/Object-level replication.  So the strategy of storing customer-specific Dynamic Tables (and nothing else) in their own database not only makes the isolation cleaner but also enables cost efficiencies by only replicating those Dynamic Table(s) that are stored in each of the databases.

