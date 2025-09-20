-- Data Cleaning and EDA

select *
from superstore.superstore
;

create table superstore.superstore_staging
like superstore.superstore
;

select *
from superstore.superstore_staging
;

insert superstore.superstore_staging
select *
from superstore.superstore
;

select *
from superstore.superstore_staging
;

SELECT `Order ID`,`Product ID`, COUNT(*) AS count
FROM superstore.superstore_staging
GROUP BY  `Order ID`,`Product ID`
HAVING COUNT(*) > 1
;

SELECT *
FROM (
    SELECT s.*,
           COUNT(*) OVER (PARTITION BY  `Order ID`, `Product ID`) AS dup_count
    FROM superstore.superstore_staging s
) t
WHERE dup_count > 1;

SELECT `Order Date`,`Order ID`,`Product ID`, COUNT(*) AS count
FROM superstore.superstore_staging
GROUP BY  `Order Date`,`Order ID`,`Product ID`
HAVING COUNT(*) > 1
;

-- After checking the rows, found out that the order might be split up in two seperate shipments as the order date was same
-- and the proportion of sales: profit ratio for different quantity is same
-- There were no duplicates rows.


SELECT
    SUM(CASE WHEN `Row ID` IS NULL THEN 1 ELSE 0 END) AS Row_ID_nulls,
    SUM(CASE WHEN `Order ID` IS NULL THEN 1 ELSE 0 END) AS Order_ID_nulls,
    SUM(CASE WHEN `Order Date` IS NULL THEN 1 ELSE 0 END) AS Order_Date_nulls,
    SUM(CASE WHEN `Ship Date` IS NULL THEN 1 ELSE 0 END) AS Ship_Date_nulls,
    SUM(CASE WHEN `Ship Mode` IS NULL THEN 1 ELSE 0 END) AS Ship_Mode_nulls,
    SUM(CASE WHEN `Customer ID` IS NULL THEN 1 ELSE 0 END) AS Customer_ID_nulls,
    SUM(CASE WHEN `Customer Name` IS NULL THEN 1 ELSE 0 END) AS Customer_Name_nulls,
    SUM(CASE WHEN `Segment` IS NULL THEN 1 ELSE 0 END) AS Segment_nulls,
    SUM(CASE WHEN `Country` IS NULL THEN 1 ELSE 0 END) AS Country_nulls,
    SUM(CASE WHEN `City` IS NULL THEN 1 ELSE 0 END) AS City_nulls,
    SUM(CASE WHEN `State` IS NULL THEN 1 ELSE 0 END) AS State_nulls,
    SUM(CASE WHEN `Postal Code` IS NULL THEN 1 ELSE 0 END) AS Postal_Code_nulls,
    SUM(CASE WHEN `Region` IS NULL THEN 1 ELSE 0 END) AS Region_nulls,
    SUM(CASE WHEN `Product ID` IS NULL THEN 1 ELSE 0 END) AS Product_ID_nulls,
    SUM(CASE WHEN `Category` IS NULL THEN 1 ELSE 0 END) AS Category_nulls,
    SUM(CASE WHEN `Sub-Category` IS NULL THEN 1 ELSE 0 END) AS Sub_Category_nulls,
    SUM(CASE WHEN `Product Name` IS NULL THEN 1 ELSE 0 END) AS Product_Name_nulls,
    SUM(CASE WHEN `Sales` IS NULL THEN 1 ELSE 0 END) AS Sales_nulls,
    SUM(CASE WHEN `Quantity` IS NULL THEN 1 ELSE 0 END) AS Quantity_nulls,
    SUM(CASE WHEN `Discount` IS NULL THEN 1 ELSE 0 END) AS Discount_nulls,
    SUM(CASE WHEN `Profit` IS NULL THEN 1 ELSE 0 END) AS Profit_nulls
FROM superstore.superstore_staging;

SELECT
    SUM(CASE WHEN `Order ID` = '' THEN 1 ELSE 0 END) AS Order_ID_blanks,
    SUM(CASE WHEN `Order Date` = '' THEN 1 ELSE 0 END) AS Order_Date_blanks,
    SUM(CASE WHEN `Ship Date` = '' THEN 1 ELSE 0 END) AS Ship_Date_blanks,
    SUM(CASE WHEN `Ship Mode` = '' THEN 1 ELSE 0 END) AS Ship_Mode_blanks,
    SUM(CASE WHEN `Customer ID` = '' THEN 1 ELSE 0 END) AS Customer_ID_blanks,
    SUM(CASE WHEN `Customer Name` = '' THEN 1 ELSE 0 END) AS Customer_Name_blanks,
    SUM(CASE WHEN `Segment` = '' THEN 1 ELSE 0 END) AS Segment_blanks,
    SUM(CASE WHEN `Country` = '' THEN 1 ELSE 0 END) AS Country_blanks,
    SUM(CASE WHEN `City` = '' THEN 1 ELSE 0 END) AS City_blanks,
    SUM(CASE WHEN `State` = '' THEN 1 ELSE 0 END) AS State_blanks,
    SUM(CASE WHEN `Region` = '' THEN 1 ELSE 0 END) AS Region_blanks,
    SUM(CASE WHEN `Product ID` = '' THEN 1 ELSE 0 END) AS Product_ID_blanks,
    SUM(CASE WHEN `Category` = '' THEN 1 ELSE 0 END) AS Category_blanks,
    SUM(CASE WHEN `Sub-Category` = '' THEN 1 ELSE 0 END) AS Sub_Category_blanks,
    SUM(CASE WHEN `Product Name` = '' THEN 1 ELSE 0 END) AS Product_Name_blanks
FROM superstore.superstore_staging;

-- There are no rows with null or blank value
-- Checking each row discretely for any non-standardized data
-- and performing EDA

select distinct(`Row ID`), count(*) as count
from superstore.superstore_staging
group by `Row ID`
order by count desc
;

select distinct(`Order ID`), count(*) as count
from superstore.superstore_staging
group by `Order ID`
order by count desc
;

select `Order Date`,
str_to_date(`Order Date`, '%m/%d/%Y')
from superstore.superstore_staging
;

update superstore.superstore_staging
set `Order Date` = str_to_date(`Order Date`, '%m/%d/%Y')
;

alter table superstore.superstore_staging
modify column `Order Date` date
;

update superstore.superstore_staging
set `Ship Date` = str_to_date(`Ship Date`, '%m/%d/%Y')
;

alter table superstore.superstore_staging
modify column `Ship Date` date
;

-- Changing data type of Order Date and Ship Date from text to date

select distinct(`Ship Mode`), count(*) as count
from superstore.superstore_staging
group by `Ship Mode`
order by count desc
;

SELECT *
FROM superstore.superstore_staging
WHERE `Customer ID` NOT REGEXP '^[A-Z]{2}-[0-9]{5}$';


select distinct(`Segment`), count(*) as count
from superstore.superstore_staging
group by `Segment`
order by count desc
;

select distinct(country), count(*) as count
from superstore.superstore_staging
group by country
;

select distinct(city), count(*) as count
from superstore.superstore_staging
group by city 
order by count desc
;

select distinct(state), count(*) as count
from superstore.superstore_staging
group by state 
order by count desc
;

SELECT `Postal Code`,
       MIN(city) AS city,
       MIN(state) AS state,
       COUNT(*) AS count
FROM superstore.superstore_staging
GROUP BY `Postal Code`
ORDER BY count DESC;

select distinct(Category), count(*) as count
from superstore.superstore_staging
group by Category
order by count desc
;

select 
    `Sub-category`,
    min(category) as category,
    sum(sales) as total_sales,
    sum(profit) as total_profit,
    count(*) as count,
    sum(profit)/sum(sales) as profit_ratio
from superstore.superstore_staging
group by `sub-category`
order by profit_ratio desc
;

select *
from superstore.superstore_staging
;

