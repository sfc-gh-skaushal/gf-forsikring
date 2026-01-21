use role data_engineer;
use database insuranceco;
use schema curated;
use warehouse insuranceco_etl_wh;

select * from dim_policies;

use role governance_admin;
use database insuranceco;
use schema curated;
use warehouse insuranceco_admin_wh;

select * from dim_policies;

describe table dim_policies;