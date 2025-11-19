-- Создаем таблицы --

-- Таблица customer --

CREATE TABLE IF NOT EXISTS customer (

    customer_id integer PRIMARY KEY,
    first_name text,
    last_name text,
    gender text,
    DOB date,
    job_title text,
    job_industry_category text,
    wealth_segment text,
    deceased_indicator text,
    owns_car text,
    address text,
    postcode varchar,
    state text,
    country text,
    property_valuation integer
   
);

-- Таблица product --

CREATE TABLE IF NOT EXISTS product (

   product_id integer,
   brand text,
   product_line text,
   product_class text,
   product_size text,
   list_price numeric(10,2),
   standard_cost numeric(10,2)
  
);

-- Очищаем таблицу product от дубликатов --

CREATE TABLE product_cor AS
 SELECT *
 FROM (
  SELECT *
   ,ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY list_price DESC) AS rn
  FROM product)
 WHERE rn = 1;

-- Устанавливаем первичный ключ в таблице product_cor --

ALTER TABLE product_cor 
ADD PRIMARY KEY (product_id);

-- Таблица orders --

CREATE TABLE IF NOT EXISTS orders (

   order_id integer PRIMARY KEY,
   customer_id integer,
   order_date date,
   online_order boolean,
   order_status text

);

-- Таблица order_items --

CREATE TABLE IF NOT EXISTS order_items (

   order_item_id integer PRIMARY KEY,
   order_id integer,
   quantity integer,
   item_list_price_at_sale numeric(10,2),
   item_standard_cost_at_sale numeric(10,2)

);

-- Добавляем внешние ключи --

-- Связь orders -> customer

DELETE FROM orders 
WHERE customer_id NOT IN (SELECT customer_id FROM customer);

ALTER TABLE orders 
ADD CONSTRAINT fk_orders_customer 
FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

-- Связь order_items -> orders
DELETE FROM order_items 
WHERE order_id NOT IN (SELECT order_id FROM orders);

ALTER TABLE order_items 
ADD CONSTRAINT fk_order_items_orders 
FOREIGN KEY (order_id) REFERENCES orders(order_id);

-- Связь order_items -> product_cor

DELETE FROM order_items 
WHERE product_id NOT IN (SELECT product_id FROM product_cor);

ALTER TABLE order_items 
ADD CONSTRAINT fk_order_items_product 
FOREIGN KEY (product_id) REFERENCES product_cor(product_id);


------------------------------------------------------------------------
-- Запрос 1 --
-- Вывести все уникальные бренды, у которых есть хотя бы один продукт -- 
-- со стандартной стоимостью выше 1500 долларов, --
-- и суммарными продажами не менее 1000 единиц. --

SELECT
    p.brand
FROM order_items oi
JOIN product_cor p ON oi.product_id = p.product_id
WHERE p.standard_cost > 1500
GROUP BY p.brand
HAVING SUM(oi.quantity) >= 1000;

------------------------------------------------------------------------
-- Запрос 2 --
-- Для каждого дня в диапазоне с 2017-04-01 по 2017-04-09 включительно --
-- вывести количество подтвержденных онлайн-заказов и количество -- 
-- уникальных клиентов, совершивших эти заказы. --

SELECT 
    order_date,
    COUNT (order_id) AS order_count,
    COUNT (DISTINCT customer_id) AS customer_count
FROM orders
WHERE order_date BETWEEN '2017-04-01' AND '2017-04-09'
    AND online_order = True
    AND order_status = 'Approved'
GROUP BY order_date
ORDER BY order_date

------------------------------------------------------------------------
-- Запрос 3 --
-- Вывести профессии клиентов: --
-- из сферы IT, чья профессия начинается с Senior; --
-- из сферы Financial Services, чья профессия начинается с Lead. --
-- Для обеих групп учитывать только клиентов старше 35 лет. --
-- Объединить выборки с помощью UNION ALL. --

WITH max_date AS (
    SELECT MAX(order_date) AS max_date -- Возраст считаем от максимального года, представленного в данных --
    FROM orders
),
senior_it AS (
    SELECT DISTINCT c.job_title
    FROM customer c
    CROSS JOIN max_date
    WHERE c.job_industry_category = 'IT'
        AND c.job_title LIKE 'Senior%'
        AND EXTRACT(YEAR FROM AGE(max_date.max_date, c.DOB)) > 35
),
fin_services AS (
    SELECT DISTINCT c.job_title
    FROM customer c
    CROSS JOIN max_date
    WHERE c.job_industry_category = 'Financial Services'
        AND c.job_title LIKE 'Lead%'
        AND EXTRACT(YEAR FROM AGE(max_date.max_date, c.DOB)) > 35
)
SELECT * FROM senior_it
UNION ALL
SELECT * FROM fin_services;

------------------------------------------------------------------------
-- Запрос 4 --
-- Вывести бренды, которые были куплены клиентами из сферы Financial Services,--
-- но не были куплены клиентами из сферы IT. --

-- Оставляем данные по брендам, купленным клиентами из сфер IT и Financial Services --

WITH it_brands AS (
    SELECT
        DISTINCT brand
    FROM order_items AS oi
    JOIN product_cor AS p
    ON oi.product_id = p.product_id
    JOIN orders AS o
    ON oi.order_id = o.order_id 
    JOIN customer AS c
    ON o.customer_id = c.customer_id
    WHERE brand IS NOT NULL
        AND c.job_industry_category = 'IT'
        AND o.order_status = 'Approved'
),
financial_brands AS (
    SELECT
        DISTINCT brand
    FROM order_items AS oi
    JOIN product_cor AS p
    ON oi.product_id = p.product_id
    JOIN orders AS o
    ON oi.order_id = o.order_id 
    JOIN customer AS c
    ON o.customer_id = c.customer_id
    WHERE brand IS NOT NULL
        AND c.job_industry_category = 'Financial Services'
        AND o.order_status = 'Approved'
)
SELECT *
FROM financial_brands
WHERE brand NOT IN (
    SELECT brand
    FROM it_brands
);
------------------------------------------------------------------------
-- Запрос 5 --
-- Вывести 10 клиентов (ID, имя, фамилия), которые совершили наибольшее --
-- количество онлайн-заказов (в штуках) брендов Giant Bicycles, Norco Bicycles, --
-- Trek Bicycles, при условии, что они активны и имеют оценку имущества (property_valuation) --
-- выше среднего среди клиентов из того же штата. --

-- Запрос 5 --
WITH state_avg_valuation AS (
    SELECT 
        state,
        AVG(property_valuation) AS avg_pv_by_state
    FROM customer 
    WHERE deceased_indicator = 'N'
    GROUP BY state
),
customer_orders AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.state,
        c.property_valuation,
        o.order_id
    FROM customer AS c
    JOIN orders AS o
    ON c.customer_id = o.customer_id
    JOIN order_items AS oi 
    ON o.order_id = oi.order_id 
    JOIN product_cor AS p 
    ON oi.product_id = p.product_id
    WHERE p.brand IN ('Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles')
        AND c.deceased_indicator = 'N'
        AND o.online_order = TRUE
),
filtered_customers AS (
    SELECT 
        co.customer_id,
        co.first_name,
        co.last_name,
        co.order_id
    FROM customer_orders AS co
    JOIN state_avg_valuation AS sav ON co.state = sav.state
    WHERE co.property_valuation > sav.avg_pv_by_state
)
SELECT 
    customer_id,
    first_name,
    last_name
FROM (
    SELECT 
        customer_id,
        first_name,
        last_name,
        COUNT(DISTINCT order_id) AS order_count
    FROM filtered_customers
    GROUP BY customer_id, first_name, last_name
)
ORDER BY order_count DESC
LIMIT 10;

------------------------------------------------------------------------
-- Запрос 6 --
-- Вывести всех клиентов (ID, имя, фамилия), у которых нет подтвержденных --
-- онлайн-заказов за последний год, но при этом они владеют автомобилем и --
-- их сегмент благосостояния не Mass Customer. --

WITH year_data AS (
    SELECT 
        max(EXTRACT(YEAR FROM order_date)) AS last_year
    FROM orders
    ),
last_year_orders AS (
    SELECT 
        DISTINCT customer_id
    FROM orders
    WHERE EXTRACT(YEAR FROM order_date) = (SELECT last_year FROM year_data)
        AND online_order = TRUE
        AND order_status = 'Approved'
        )
 SELECT 
     customer_id,
     first_name,
     last_name
 FROM customer
 WHERE customer_id NOT IN (
     SELECT 
         customer_id
     FROM last_year_orders
     )
     AND owns_car = 'Yes'
     AND wealth_segment != 'Mass Customer';
     
------------------------------------------------------------------------
-- Запрос 7 --
-- Вывести всех клиентов из сферы IT (ID, имя, фамилия), которые купили 
-- 2 из 5 продуктов с самой высокой list_price в продуктовой линейке Road --
     
-- 1) Топ-5 продуктов по list_price --     
WITH top_5_products AS (
    SELECT DISTINCT
        p.product_id,
        p.list_price
    FROM product_cor AS p
    WHERE p.product_line = 'Road'
        AND p.list_price IS NOT NULL
    ORDER BY p.list_price DESC
    LIMIT 5
),
-- 2) Данные по покупкам продуктов из топ-5 линейки Road клиентами из сферы IT --
customer_purchases AS (
SELECT
    oi.product_id,
    oi.item_standard_cost_at_sale,
    c.customer_id,
    first_name,
    last_name
FROM order_items AS oi
JOIN orders AS o
ON oi.order_id = o.order_id
JOIN customer AS c
ON o.customer_id = c.customer_id
JOIN product_cor AS p
ON oi.product_id = p.product_id 
WHERE p.product_line = 'Road'
    AND c.job_industry_category = 'IT'
    AND p.product_id IN (SELECT product_id
                         FROM top_5_products)
    )
-- 3) Считаем количество купленных продуктов для каждого клиента --
SELECT
    customer_id,
    first_name,
    last_name
FROM customer_purchases
GROUP BY customer_id, first_name, last_name
HAVING COUNT (DISTINCT(product_id)) >= 2;

------------------------------------------------------------------------
-- Запрос 8 --
-- Вывести клиентов (ID, имя, фамилия, сфера деятельности) -- 
-- из сферы IT или Health, которые совершили не менее 3 подтвержденных --
-- заказов в период 2017-01-01 по 2017-03-01 и при этом их общий доход --
--  от этих заказов превышает 10000 долларов. Разделить вывод на две группы (IT и Health) с помощью UNION --
WITH approved_orders AS (
    SELECT
        order_id,
        customer_id
    FROM orders
    WHERE order_status = 'Approved'
        AND order_date BETWEEN '2017-01-01' AND '2017-03-01'
),
it_order_stats AS (
    SELECT 
        ao.customer_id,
        first_name,
        last_name,
        quantity * item_list_price_at_sale AS revenue,
        ao.order_id,
        job_industry_category 
    FROM order_items AS oi
    JOIN approved_orders AS ao
    ON oi.order_id = ao.order_id
    JOIN customer AS c
    ON ao.customer_id = c.customer_id
    WHERE c.job_industry_category = 'IT'
),
it_relevant_customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        job_industry_category
    FROM it_order_stats
    GROUP BY customer_id, first_name, last_name, job_industry_category 
    HAVING COUNT(DISTINCT order_id) >= 3
        AND sum(revenue) > 10000
),
health_order_stats AS (
    SELECT 
        ao.customer_id,
        first_name,
        last_name,
        quantity * item_list_price_at_sale AS revenue,
        ao.order_id,
        job_industry_category 
    FROM order_items AS oi
    JOIN approved_orders AS ao
    ON oi.order_id = ao.order_id
    JOIN customer AS c
    ON ao.customer_id = c.customer_id
    WHERE c.job_industry_category = 'Health'
),
health_relevant_customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        job_industry_category 
    FROM health_order_stats
    GROUP BY customer_id, first_name, last_name, job_industry_category 
    HAVING COUNT(DISTINCT order_id) >= 3
        AND sum(revenue) > 10000
)
SELECT *
FROM it_relevant_customers
UNION 
SELECT *
FROM health_relevant_customers
ORDER BY job_industry_category;
