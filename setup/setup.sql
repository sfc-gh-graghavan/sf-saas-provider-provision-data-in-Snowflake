//Here we create a base database and schema to install stored procedures
//and create the REQUESTS and REQUESTS_FULFILLED tables.

//change it to whatever database and schema you want
create database saas;
use database saas;
create schema saas;
use schema saas;

//Table to hold all the SaaS Provider's customers data requests
create or replace table REQUESTS (
    customer_name varchar(50),
    customer_id varchar(50),
    snowflake_account varchar(50),
    snowflake_region varchar(50),
    table_requested varchar(50),
    filters varchar (1000),
    req_dt date
)COMMENT='HOLDS ALL THE DATA PRODUCT REQUESTS FROM CUSTOMERS';

//setting change_tracking to true and creating streams on REQUESTS
alter table requests set change_tracking = true;
create stream if not exists requests_stream on table requests;

//creating a REQUESTS_FULFILLED table to hold all the audit information
//on which customer's data requests were fulfilled via private listings
CREATE OR REPLACE TABLE REQUESTS_FULFILLED(
    customer_name varchar(50),
    customer_id varchar(50),
    snowflake_account varchar(50),
    snowflake_region varchar(50),
    table_requested varchar(50),
    filters varchar (1000),
    req_dt date
)COMMENT='HOLDS ALL THE DATA PRODUCT REQUESTS FULFILLED';
