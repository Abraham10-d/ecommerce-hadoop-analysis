-- 4. (Create the table) 
-- (Be sure to replace your_databasename with your actual database name) 
DROP TABLE IF EXISTS October2019 ; 
CREATE EXTERNAL TABLE IF NOT EXISTS October2019( 
event_time TIMESTAMP, 
event_type STRING, 
product_id BIGINT, 
category_id BIGINT, 
category_code STRING, 
brand STRING, 
price FLOAT, 
user_id BIGINT, 
user_session STRING 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION 
'/user/your_databasename/EcommerceAnalysis/Oct20
 19' 
TBLPROPERTIES ('skip.header.line.count'='1');

-- 5. (Overview of table) 
SELECT * FROM October2019 LIMIT 10;

-- 6. (Sales Insight Analysis) 
SELECT  
DATE(event_time) AS event_date,  
COUNT(product_id) AS products_sold,  
ROUND(SUM(price), 2) AS total_revenue,  
COUNT(DISTINCT user_id) AS customer_count 
FROM October2019 
WHERE event_type = 'purchase' 
GROUP BY DATE(event_time) 
ORDER BY total_revenue DESC;

-- 7. (Sales Insight Analysis) 
SELECT  
category_code, 
ROUND(SUM(price), 2) AS total_revenue 
FROM October2019 
WHERE event_type = 'purchase' 
GROUP BY category_code 
ORDER BY total_revenue DESC 
LIMIT 25;

--  8. (Sales Insight Analysis) 
SELECT  
brand, 
ROUND(SUM(price), 2) AS total_revenue 
FROM October2019 
WHERE event_type = 'purchase' 
GROUP BY brand 
ORDER BY total_revenue DESC 
LIMIT 25;

--  9. (Customer Behavior Analysis) 
SELECT  
user_id, 
ROUND(SUM(price), 2) AS total_revenue, 
COUNT(DISTINCT user_session) AS 
total_sessions,  
COUNT(product_id) AS products_purchased, 
COUNT(DISTINCT category_code) AS 
unique_categories, 
COUNT(DISTINCT brand) AS unique_brands 
FROM October2019 
WHERE event_type = 'purchase' 
GROUP BY user_id 
ORDER BY total_revenue DESC 
LIMIT 25;

--  10.  (Tempo-Spatial Analysis) 
SELECT  
DATE(event_time) AS event_date,  
COUNT(DISTINCT user_id) AS unique_users,  
ROUND(SUM(price), 2) AS total_revenue,  
COUNT(product_id) AS total_products_sold,  
ROUND(AVG(price), 2) AS 
avg_revenue_per_product 
FROM October2019 
WHERE event_type = 'purchase' 
GROUP BY DATE(event_time) 
ORDER BY event_date ASC, total_revenue DESC;
