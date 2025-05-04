#  0. (Connect to Hadoop cluster) 
ssh ****@144.24.46.199
password: *******

#  1. (download the 2019-Oct.csv dataset)
wget -O 2019-Oct.csv "https://drive.usercontent.google.com/download?id=17Lk5PSub28UVBl-USPlDFh-Z3mcEZDH7&export=download&authuser=0&confirm=t&uuid=095f33cd-2ce9-4681-856d-f7d49e3b9681&at=APcmpoyh4cdQLNWEOcowd77ZW6tU%3A1745047922226"

#  2. (Upload 2019-Oct.csv to a directory in HDFS for analysis)
hdfs dfs -mkdir EcommerceAnalysis
hdfs dfs -mkdir EcommerceAnalysis/Oct2019
hdfs dfs -ls
hdfs dfs -put 2019-Oct.csv EcommerceAnalysis/Oct2019

#  3. Connect to beeline

#  4. (Create the table)
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
STORED AS TEXTFILE LOCATION '/user/your_databasename/EcommerceAnalysis/Oct2019'
TBLPROPERTIES ('skip.header.line.count'='1');


#  5. (Overview of table)
SELECT * FROM October2019 LIMIT 10;


#  6. (Sales Insight Analysis)
SELECT 
    DATE(event_time) AS event_date, 
    COUNT(product_id) AS products_sold, 
    ROUND(SUM(price), 2) AS total_revenue, 
    COUNT(DISTINCT user_id) AS customer_count
FROM October2019
WHERE event_type = 'purchase'
GROUP BY DATE(event_time)
ORDER BY total_revenue DESC;

#  7. (Sales Insight Analysis)
SELECT 
    category_code,
    ROUND(SUM(price), 2) AS total_revenue
FROM October2019
WHERE event_type = 'purchase'
GROUP BY category_code
ORDER BY total_revenue DESC
LIMIT 25;

#  8. (Sales Insight Analysis)
SELECT 
    brand,
    ROUND(SUM(price), 2) AS total_revenue
FROM October2019
WHERE event_type = 'purchase'
GROUP BY brand
ORDER BY total_revenue DESC
LIMIT 25;

#  9. (Customer Behavior Analysis)
SELECT 
    user_id,
    ROUND(SUM(price), 2) AS total_revenue,
    COUNT(DISTINCT user_session) AS total_sessions, 
    COUNT(product_id) AS products_purchased,
    COUNT(DISTINCT category_code) AS unique_categories,
    COUNT(DISTINCT brand) AS unique_brands
FROM October2019
WHERE event_type = 'purchase'
GROUP BY user_id
ORDER BY total_revenue DESC
LIMIT 25;

# 10.  (Tempo-Spatial Analysis)
SELECT 
    DATE(event_time) AS event_date, 
    COUNT(DISTINCT user_id) AS unique_users, 
    ROUND(SUM(price), 2) AS total_revenue, 
    COUNT(product_id) AS total_products_sold, 
    ROUND(AVG(price), 2) AS avg_revenue_per_product
FROM October2019
WHERE event_type = 'purchase'
GROUP BY DATE(event_time)
ORDER BY event_date ASC, total_revenue DESC;

# 11. (Open another terminal with git bash, and download the data into pc)
ssh *****@144.24.46.199
password: *******

hdfs dfs -get EcommerceAnalysis/Oct2019/2019-Oct.csv
ls -al

# 12. (Open another terminal with git bash in order to read/import the output file using your pc)
scp a*****@144.24.46.199:/home/your_database/2019-Oct.csv 2019-Oct.csv
