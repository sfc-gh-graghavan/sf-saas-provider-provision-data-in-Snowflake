/*************************************************************************************************************
Script:             sproc_for_creating_listings.sql
Create Date:        2023-10-03
Author:             Gopal Raghavan
Description:        Stored Procedure to create private listings programmatically and target them to consumer
                    accounts
Audience:           Providers
Usage:              1.  The database where this procedure will be installed is named SAAS.  Please change as
                        needed.  The related schema (also named SAAS) can also be changed as needed.
                    2.  The "customer_name" argument refers to the ACCOUNTNAME portion of the consumer's 
                        Snowflake Account Identifier. The Account Identifier is in the format - 
                        [ORGNAME].[ACCOUNTNAME].  Please change as needed.
                    3.  The "account_name" argument refers to the consumer's complete Snowflake Account 
                        Identifier.  This is required to add to the Target Account List for sharing via
                        private listing.
                    4.  The consumer's data is partitioned and stored in their own databases on the provider
                        side.  These DBs are named after the "customer_name" parameter.  They can be changed 
                        as needed.
                    5.  Each of these databases have a schema named in the format "customer_name"_DATAPRODUCTS.
                        They can be changed as needed.
                    6.  The procedure name "provision_data" can also be changed as needed.    

Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-03          G. Raghavan                         Initial Creation
*************************************************************************************************************/

use database SAAS;
use schema SAAS;

create or replace procedure provision_data("customer_name" varchar, "account_name" varchar)
  returns string not null
  language javascript
  execute as caller
  as     
  $$

    //this function contains the manifest needed to build the listing(s)
    //values can be passed dynamically to populate the listing metadata
    //in this specific example, the fields "title" and "account_name"
    //are dynamically substituted during workflow execution
    function manifestBuilder(title, account_name) {
        return `
            \$\$
            title: ${title}
            description: \"Specific to Providers only\"
            subtitle: \"SaaS Provider Playbook\"
            terms_of_service:
                type: \"OFFLINE\"
            business_needs:
                - type: \"CUSTOM\"
                  name: \"SaaS Provider\"
                  description: \"Data Product provisioned by SaaS Provider for their customers\"
            resources:
                documentation: https://other-docs.snowflake.com/en/collaboration/consumer-becoming
            targets:
                accounts: [${account_name}]
            auto_fulfillment:
                refresh_schedule: \"120 MINUTE\"
                refresh_type: \"FULL_DATABASE\"
            \$\$
            DISTRIBUTION=EXTERNAL`;
        
    }

    //function to prepare error messages when encountered
    function return_error(err){
        //grab all the error information
        var result =  "Failed: Code: " + err.code + "  State: " + err.state;
        const msg = err.message.replace(/\'/g, "");
        result += "  Message: " +msg;
        result += " Stack Trace: " + err.stackTraceTxt;
        return result;
    }

    //function to set context back to the calling db or the db where the 
    //procedure is installed
    function reset_db_context(){
        try{
            var reset_db = 'USE DATABASE SAAS';
            var reset_db_cmd = snowflake.createStatement({sqlText: reset_db});
            var reset_db_exec = reset_db_cmd.execute();
            var reset_schema = 'USE SCHEMA SAAS';
            var reset_schema_cmd = snowflake.createStatement({sqlText: reset_schema});
            var reset_schema_exec = reset_schema_cmd.execute();
        }
        catch(err){
            var reset_db_error = 'Cannot set context back to SAAS Database ';
            reset_db_error += return_error(err);
            return reset_db_error;
        }
    }

    //call manifestBuilder to add consumer accounts
    var title = '\"DATA SHARED BY SaaS PROVIDER FOR CUSTOMER '+customer_name+'\"';
    var share_name = customer_name+'_share';
    var listing_name = customer_name+'_listing';
    const config = manifestBuilder(title,account_name);
    
    //create share first
    share_sql_stmt = 'CREATE OR REPLACE SHARE '+share_name;
    try {
            var share_sql_cmd = snowflake.createStatement({sqlText: share_sql_stmt});
            var exec_share_sql_cmd = share_sql_cmd.execute();
            //return "Share created";
        }
    catch(err)
        {
            var share_err = 'Share was not created due to the error: ';
            share_err += return_error(err);
            return share_result;
        }
    //granting usage on the customer db to the related share object
    share_db_stmt = 'GRANT USAGE ON DATABASE ';
    share_db_stmt += customer_name+' TO SHARE '+share_name;
    try {
            var share_db_cmd = snowflake.createStatement({sqlText: share_db_stmt});
            var exec_share_db_cmd = share_db_cmd.execute();
            //return "granted usage on database";
        }
    catch(err)
        {
            //grab all the error information
            var db_error =  'Grant access to db failed due to the error: ';
            db_error += return_error(err);
            return db_error;
        }
    
    //granting usage on the schema containing dynamic tables to the share object
    share_schema_stmt = 'GRANT USAGE ON SCHEMA ';
    share_schema_stmt += customer_name+'.'+customer_name+'_DATAPRODUCTS TO SHARE '+share_name;
    try {
            var share_schema_cmd = snowflake.createStatement({sqlText: share_schema_stmt});
            var exec_share_schema_cmd = share_schema_cmd.execute();
            //return "granted usage on schema";
        }
    catch(err)
        {
            //grab all the error information
            var schema_err =  'Grant usage access to schema failed due to the error: ';
            schema_err += return_error(err);
            return schema_err;
        }
    
    //grant select on all dynamic tables in the partitioned database to share
    //switch database to the <customer_name> db
    switch_db_stmt = 'USE DATABASE '+customer_name;
    try {
        var switch_db_cmd = snowflake.createStatement({sqlText: switch_db_stmt});
        var switch_db_exec = switch_db_cmd.execute();
    }
    catch(err){
        var switch_db_err = 'unable to switch to database ';
        switch_db_err += return_error(err);
        return switch_db_err;
    }
    //switch schema
    switch_schema_stmt = 'USE SCHEMA '+customer_name+'_DATAPRODUCTS';
    try {
        var switch_schema_cmd = snowflake.createStatement({sqlText: switch_schema_stmt});
        var switch_schema_exec = switch_schema_cmd.execute();
    }
    catch(err){
        var switch_schema_err = 'unable to switch to schema ';
        switch_schema_err += return_error(err);
        return switch_schema_err;
    }
    //query for dynamic tables and grant select on them to share
    //PLEASE NOTE THAT THE VALUE OF "database_name" in the RESULT_SCAN
    //IS ALWAYS CAPITALIZED.  MAKE SURE TO CAPITALIZE AS NEEDED IF THE
    //<CUSTOMER_NAME> IS PASSED IN LOWERCASE.  This code has a toUpperCase
    //function to capitalize the parameter
    var show_dt_stmt = 'SHOW DYNAMIC TABLES';
    try {
        var show_dt_cmd = snowflake.createStatement({sqlText: show_dt_stmt});
        var show_dt_exec = show_dt_cmd.execute();
        //pick up the dynamic tables from RESULT_SCAN
        var pickup_dt_stmt = 'SELECT \"name\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ';
        pickup_dt_stmt += 'WHERE \"database_name\" = ';
        pickup_dt_stmt += '\''+customer_name.toUpperCase()+'\'';
        try {
            var pickup_dt_cmd = snowflake.createStatement({sqlText: pickup_dt_stmt});
            var pickup_dt_exec = pickup_dt_cmd.execute();
            while (pickup_dt_exec.next()) {
                var grant_stmt = 'GRANT SELECT ON DYNAMIC TABLE '+pickup_dt_exec.getColumnValue(1);
                grant_stmt += ' TO SHARE '+share_name;
                try {
                    var grant_stmt_cmd = snowflake.createStatement({sqlText: grant_stmt});
                    var grant_stmt_exec = grant_stmt_cmd.execute();
                }
                catch(err){
                    var grant_err = 'unable to grant select on dynamic table to share '+share_name;
                    grant_err += 'due to error: ';
                    grant_err += return_error(err);
                    return grant_err;
                }
            }
        }
        catch(err){
            var vw_stmt_err = 'unable to pick up dynamic tables because of error: ';
            vw_stmt_err  += return_error(err);
            return vw_stmt_err;
        }
    //set context back to the calling database
    reset_db_context();
    }
    catch(err){
        var show_vw_err = 'unable to list dynamic tables ';
        show_vw_err += return_error(err);
        return show_vw_err;
    }
  
    //finally create the listing
    var sql_stmt = 'CREATE LISTING '+listing_name+' FOR SHARE '+share_name+' AS '+config;
    //console.log(sql_stmt);
    try{
        var sql_cmd = snowflake.createStatement({sqlText: sql_stmt});
        var exec_sql_cmd = sql_cmd.execute();
        return listing_name+' has been created';
    }
    catch(err){
        //grab all the error information
        var listing_err =  listing_name+ ' was not created due to the error: ';
        listing_err += return_error(err);
        return  listing_err;
    }
$$
;
