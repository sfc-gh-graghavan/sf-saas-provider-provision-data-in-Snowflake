/*************************************************************************************************************
Script:             sproc_for_data_partitioning.sql
Create Date:        2023-10-03
Author:             Gopal Raghavan
Description:        Stored Procedure to partition customer data into their own db and schema,create dynamic
                    tables and prepare them for sharing with consumer accounts via private listings
Audience:           Providers
Prerequisities:     1.  A REQUESTS table with change_tracking set to true.  This table contains all the data
                        requests submitted by a SaaS Provider's customers.
                    2.  Streams on the REQUESTS table - REQUESTS_STREAM.  This contains any new requests that 
                        are sent by a SaaS Provider's customers.
                    3.  REQUESTS_FULFILLED table which contains all the audit information on data shared with
                        a SaaS Provider's customers via Private Listings.
Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-03          G. Raghavan                         Initial Creation
*************************************************************************************************************/

use database saas;
use schema saas;

create or replace procedure partition_customer_data()
    returns string not null
    language javascript
    execute as caller
    as
    $$
        //function to create customer db and schema for isolation
        //isolation of customer's data in their own db and schema 
        //is a cleaner way of executing the workflow
        function create_db_schema(cust_name){
            try{
                //create the database if not exists - this will store all the partitioned customer data
                var sql_create_db = 'CREATE DATABASE IF NOT EXISTS '+cust_name;
                var create_db_cmd = snowflake.createStatement({sqlText: sql_create_db});
                var create_db_exec = create_db_cmd.execute();
                var sql_use_db = 'USE DATABASE '+cust_name;
                var use_db_cmd = snowflake.createStatement({sqlText: sql_use_db});
                var use_db_exec = use_db_cmd.execute();
                var sql_create_schema = 'CREATE SCHEMA IF NOT EXISTS '+cust_name+'_DATAPRODUCTS';
                var create_schema_cmd = snowflake.createStatement({sqlText: sql_create_schema});
                var create_schema_exec = create_schema_cmd.execute();
                var sql_use_schema = 'USE SCHEMA '+cust_name+'_DATAPRODUCTS';
                var schema_cmd = snowflake.createStatement({sqlText: sql_use_schema});
                var schema_exec = schema_cmd.execute();
            }
            catch(err){
                    var create_db_error = 'Cannot create Database & Schema for customer '+cust_name+' ';
                    create_db_error += return_error(err);
                    return create_db_error;
                }
        }

        //function to create dynamic tables 
        function create_dynamic_tables(cust_name){
            var sql_tbls_requested = 'SELECT table_requested FROM SAAS.SAAS.REQUESTS_STREAM WHERE customer_name = '+'\'';
            sql_tbls_requested += cust_name+'\''+' AND METADATA$ACTION = \'INSERT\'';
            sql_tbls_requested += ' AND METADATA$ACTION = \'INSERT\'';
            var tbl_req_cmd = snowflake.createStatement({sqlText: sql_tbls_requested});
            var tbl_req_exec = tbl_req_cmd.execute();
            while (tbl_req_exec.next()){
                try{
                    var sql_prep_tbl = 'CREATE OR REPLACE DYNAMIC TABLE '+tbl_req_exec.getColumnValue(1)+'_DT ';
                    sql_prep_tbl += 'TARGET_LAG = \'60 minutes\' WAREHOUSE = OSR ';
                    sql_prep_tbl += 'AS SELECT * FROM SAAS.SAAS.'+tbl_req_exec.getColumnValue(1);
                    sql_prep_tbl += ' WHERE C_OWNER = '+'\''+cust_name+'\'';
                    var prep_tbl_cmd = snowflake.createStatement({sqlText: sql_prep_tbl});
                    var prep_tbl_exec = prep_tbl_cmd.execute();
                    //log_reqs_fulfilled(cust_name, tbl_req_exec.getColumnValue(1));
                }
                catch(err){
                    var create_dt_error = 'Cannot create Dynamic Table ';
                    create_dt_error += return_error(err);
                    return create_dt_error;
                }
            }
        }

        //function to call stored procedure orchestrate_data to create private listing
        //all the messages are logged in the LISTING_AUDIT table
        function create_pvt_listing(cust_name, acct_name){
            var create_listings_stmt = snowflake.createStatement({
                                sqlText: 'CALL saas.saas.provision_data(:1, :2)',
                                binds: [cust_name, acct_name]
            });
            var create_listing_exec = create_listings_stmt.execute();
            create_listing_exec.next();
        }

        //function to set context back to the calling DB
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

        //populate REQUESTS_FULFILLED table with all the fulfilled requests
        //function log_reqs_fulfilled(cust_name, tbl_name){
        function log_reqs_fulfilled(cust_name){
            try{
                var off_stmt = 'INSERT INTO SAAS.SAAS.REQUESTS_FULFILLED ';
                off_stmt += 'SELECT CUSTOMER_NAME, CUSTOMER_ID, SNOWFLAKE_ACCOUNT, ';
                off_stmt += 'SNOWFLAKE_REGION, TABLE_REQUESTED, FILTERS, REQ_DT FROM SAAS.SAAS.REQUESTS WHERE ';
                //off_stmt += 'CUSTOMER_NAME = '+'\''+cust_name+'\''+' AND TABLE_REQUESTED = '+'\''+tbl_name+'\'';
                off_stmt += 'CUSTOMER_NAME = '+'\''+cust_name+'\'';
                var off_stmt_cmd = snowflake.createStatement({sqlText: off_stmt});
                var off_stmt_exec = off_stmt_cmd.execute();
            }
            catch(err){
                    var offset_error = 'Fulfilled Requests cannot be logged ';
                    offset_error += return_error(err);
                    return offset_error;
            }
        }
        
        //flush streams so we can capture all new requests that come in
        function reset_streams(){
            try{
                var streams_stmt = 'CREATE OR REPLACE TEMPORARY TABLE SAAS.SAAS.RESET_STREAMS AS ';
                streams_stmt += 'SELECT * FROM SAAS.SAAS.REQUESTS_STREAM ';
                streams_stmt += 'WHERE 1=0';
                var streams_cmd = snowflake.createStatement({sqlText: streams_stmt});
                var streams_exec = streams_cmd.execute();
            }
            catch(err){
                    var flush_error = 'Cannot flush the streams table ';
                    flush_error += return_error(err);
                    return flush_error;
            }
        }

        //generic error function
        function return_error(err){
            //grab all the error information
            var result =  "Failed: Code: " + err.code + "  State: " + err.state;
            //remove the single quote from err.message object to prevent SQL errors
            //when inserting into AUDIT table
            const msg = err.message.replace(/\'/g, "");
            result += "  Message: " +msg;
            result += " Stack Trace: " + err.stackTraceTxt;
            return result;
        }

        //get customer name from the Requests Stream
        var sql_db_stmt = 'SELECT DISTINCT CUSTOMER_NAME,SNOWFLAKE_ACCOUNT FROM SAAS.SAAS.REQUESTS_STREAM';
        var db_cmd = snowflake.createStatement({sqlText: sql_db_stmt});
        var db_exec = db_cmd.execute();
        while (db_exec.next()){
            try{
                //create the database if not exists - this will store all the partitioned customer data
                create_db_schema(db_exec.getColumnValue(1));
                //create DTs and corresponding secure views in the appropriate schema
                create_dynamic_tables(db_exec.getColumnValue(1));
                //log the fulfilled requests
                log_reqs_fulfilled(db_exec.getColumnValue(1));
                //create listings by calling stored procedure orchestrate_data
                create_pvt_listing(db_exec.getColumnValue(1), db_exec.getColumnValue(2));
                //set the context back to the main provider db and schema
                reset_db_context();
            }
            catch(err){
                var catch_error = return_error(err);
                return catch_error;
            }
        }
        //Clear out Streams after data requests have been fulfilled
        reset_streams();
        return 'Customer data requests have been fulfilled';
    $$
    ;
