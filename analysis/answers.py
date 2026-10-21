import duckdb


duck = duckdb.connect()
duck.execute("INSTALL postgres")
duck.execute("LOAD postgres")
duck.execute(
    "ATTACH 'host=localhost port=5437 dbname=dm_postgres user=dm_postgres password=dm_postgres' "
    "AS pg (TYPE postgres, READ_ONLY)"
)
duck.execute("USE pg")  


def run(title, query):
    """Print the title, then run the query and print the result table."""
    print(f"\n===== {title} =====")
    duck.sql(query).show(max_rows=100, max_width=250)


# ---------------------------------------------------------------
# Q1: Which manufacturers are making the highest payments to physicians?
# KPI: Total payment amount by manufacturer
# ---------------------------------------------------------------
run("Q1: Which manufacturers are making the highest payments to physicians?  |  KPI: Total payment amount by manufacturer", """
SELECT m.manufacturer_name, ROUND(SUM(f.total_amount_usd), 2) AS total_paid
FROM gold.fact_payment f
JOIN gold.dim_manufacturer m ON m.manufacturer_sk = f.manufacturer_sk
JOIN gold.dim_recipient r    ON r.recipient_sk    = f.recipient_sk
WHERE r.recipient_type = 'Covered Recipient Physician'
GROUP BY m.manufacturer_name
ORDER BY total_paid DESC
LIMIT 10;
""")

# ---------------------------------------------------------------
# Q2: Which physicians received the highest total payments?
# KPI: Total payment amount by physician
# ---------------------------------------------------------------
run("Q2: Which physicians received the highest total payments?  |  KPI: Total payment amount by physician", """
SELECT r.profile_id, r.first_name, r.last_name, ROUND(SUM(f.total_amount_usd), 2) AS total_received
FROM gold.fact_payment f
JOIN gold.dim_recipient r ON r.recipient_sk = f.recipient_sk
WHERE r.recipient_type = 'Covered Recipient Physician'
GROUP BY r.profile_id, r.first_name, r.last_name
ORDER BY total_received DESC
LIMIT 10;
""")

# ---------------------------------------------------------------
# Q3: What type of payment is most common?
# KPI: Payment count and amount by nature of payment
# ---------------------------------------------------------------
run("Q3: What type of payment is most common?  |  KPI: Payment count and amount by nature of payment", """
SELECT pt.nature_of_payment, COUNT(*) AS payment_count, ROUND(SUM(f.total_amount_usd), 2) AS total_amount
FROM gold.fact_payment f
JOIN gold.dim_payment_type pt ON pt.payment_type_sk = f.payment_type_sk
GROUP BY pt.nature_of_payment
ORDER BY payment_count DESC;
""")

# ---------------------------------------------------------------
# Q4: Which states or cities have the highest payment activity?
# KPI: Total payments by state/city
# ---------------------------------------------------------------

# Q4 (by state)
run("Q4: Which states or cities have the highest payment activity? (by state)  |  KPI: Total payments by state/city", """
SELECT r.state, COUNT(*) AS payment_count, ROUND(SUM(f.total_amount_usd), 2) AS total_amount
FROM gold.fact_payment f
JOIN gold.dim_recipient r ON r.recipient_sk = f.recipient_sk
WHERE r.state IS NOT NULL
GROUP BY r.state
ORDER BY total_amount DESC
LIMIT 10;
""")

# Q4 (by city)
run("Q4: Which states or cities have the highest payment activity? (by city)  |  KPI: Total payments by state/city", """
SELECT r.city, r.state, COUNT(*) AS payment_count, ROUND(SUM(f.total_amount_usd), 2) AS total_amount
FROM gold.fact_payment f
JOIN gold.dim_recipient r ON r.recipient_sk = f.recipient_sk
WHERE r.city IS NOT NULL
GROUP BY r.city, r.state
ORDER BY total_amount DESC
LIMIT 10;
""")

# ---------------------------------------------------------------
# Q5: What is the trend of payments over time?
# KPI: Monthly / quarterly / yearly payment trend
# ---------------------------------------------------------------

# Q5 (yearly)
run("Q5: What is the trend of payments over time? (yearly)  |  KPI: Monthly/quarterly/yearly payment trend", """
SELECT EXTRACT(year FROM f.date_of_payment) AS year, COUNT(*) AS payment_count, ROUND(SUM(f.total_amount_usd), 2) AS total_amount
FROM gold.fact_payment f
WHERE f.date_of_payment IS NOT NULL
GROUP BY year
ORDER BY year;
""")

# Q5 (quarterly)
run("Q5: What is the trend of payments over time? (quarterly)  |  KPI: Monthly/quarterly/yearly payment trend", """
SELECT EXTRACT(year FROM f.date_of_payment) AS year, EXTRACT(quarter FROM f.date_of_payment) AS quarter, COUNT(*) AS payment_count, ROUND(SUM(f.total_amount_usd), 2) AS total_amount
FROM gold.fact_payment f
WHERE f.date_of_payment IS NOT NULL
GROUP BY year, quarter
ORDER BY year, quarter;
""")

# Q5 (monthly)
run("Q5: What is the trend of payments over time? (monthly)  |  KPI: Monthly/quarterly/yearly payment trend", """
SELECT EXTRACT(year FROM f.date_of_payment)  AS year, EXTRACT(month FROM f.date_of_payment) AS month, COUNT(*) AS payment_count, ROUND(SUM(f.total_amount_usd), 2) AS total_amount
FROM gold.fact_payment f
WHERE f.date_of_payment IS NOT NULL
GROUP BY year, month
ORDER BY year, month;
""")

# ---------------------------------------------------------------
# Q6: Which medical specialties receive the highest payments?
# KPI: Total payment amount by physician specialty
# (specialty_1 is the physician's main specialty)
# ---------------------------------------------------------------
run("Q6: Which medical specialties receive the highest payments?  |  KPI: Total payment amount by physician specialty", """
SELECT r.specialty_1 AS specialty, ROUND(SUM(f.total_amount_usd), 2) AS total_amount
FROM gold.fact_payment f
JOIN gold.dim_recipient r ON r.recipient_sk = f.recipient_sk
WHERE r.specialty_1 IS NOT NULL
GROUP BY r.specialty_1
ORDER BY total_amount DESC
LIMIT 10;
""")

# ---------------------------------------------------------------
# Q7: Which drugs or medical products are linked with the highest payments?
# KPI: Total payment amount by drug/product
# A payment can have up to 5 products, so the bridge table splits the amount
# between them (allocation_weight). This way no amount is counted twice.
# ---------------------------------------------------------------
run("Q7: Which drugs or medical products are linked with the highest payments?  |  KPI: Total payment amount by drug/product", """
SELECT p.product_name, ROUND(SUM(f.total_amount_usd * b.allocation_weight), 2) AS total_amount
FROM gold.fact_payment f
JOIN gold.bridge_payment_product b ON b.payment_sk = f.payment_sk
JOIN gold.dim_product p            ON p.product_sk = b.product_sk
WHERE p.product_name IS NOT NULL
GROUP BY p.product_name
ORDER BY total_amount DESC
LIMIT 10;
""")

# ---------------------------------------------------------------
# Q8: Which payment categories are increasing or decreasing over time?
# KPI: Trend by payment category
# (payment category = nature of payment; read each category's amount from year to year)
# ---------------------------------------------------------------
run("Q8: Which payment categories are increasing or decreasing over time?  |  KPI: Trend by payment category", """
SELECT pt.nature_of_payment, EXTRACT(year FROM f.date_of_payment) AS year, ROUND(SUM(f.total_amount_usd), 2) AS total_amount
FROM gold.fact_payment f
JOIN gold.dim_payment_type pt ON pt.payment_type_sk = f.payment_type_sk
WHERE f.date_of_payment IS NOT NULL
GROUP BY pt.nature_of_payment, year
ORDER BY pt.nature_of_payment, year;
""")

duck.close()