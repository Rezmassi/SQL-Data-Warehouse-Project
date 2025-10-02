/*
=============================================================
Create Database and Schemas
=============================================================
*/


create schema bronze;
create schema silver;
create schema gold;



/*
=============================================================
Load Layers
=============================================================
*/

call bronze.load_bronze;
call silver.load_silver;
call gold.load_gold;