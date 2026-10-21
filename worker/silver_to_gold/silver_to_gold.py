import os
import duckdb

# DuckDB does ALL the work (vectorized, multi-threaded) on local tables.
# Postgres is read once (silver) and written once (finished gold tables).
WORK_DB = "/tmp/gold_work.duckdb"
if os.path.exists(WORK_DB):
    os.remove(WORK_DB)

duck = duckdb.connect(WORK_DB)  # file-backed, spills to disk
duck.execute("SET memory_limit = '4GB'")  # raise as far as the machine allows
duck.execute("SET temp_directory = '/tmp/duckdb_tmp'")
duck.execute("SET preserve_insertion_order = false")

duck.execute("INSTALL postgres")
duck.execute("LOAD postgres")
duck.execute(
    "ATTACH 'host=postgres port=5432 dbname=dm_postgres user=dm_postgres password=dm_postgres' "
    "AS pg (TYPE postgres)"
)

SILVER = "stg_silver"  # local copy of pg.silver.silver_payments


# ---------------------------------------------------------------
# natural-key hash: ONE bigint per dimension instead of 3-31 COALESCE comparisons.
# '' and NULL are treated the same, so no duplicate dim rows / no fan-out in the fact.
# ---------------------------------------------------------------
def nk(cols):
    parts = ",".join(f"COALESCE(NULLIF({c}::text,''),'~')" for c in cols)
    return f"hash(concat_ws('|',{parts}))"


NK_COLS = {
    "recipient_nk": (
        [
            "covered_recipient_type", "covered_recipient_profile_id", "covered_recipient_npi",
            "covered_recipient_first_name", "covered_recipient_middle_name",
            "covered_recipient_last_name", "covered_recipient_name_suffix",
            "recipient_primary_business_street_address_line1",
            "recipient_primary_business_street_address_line2",
            "recipient_city", "recipient_state", "recipient_zip_code", "recipient_country",
            "recipient_province", "recipient_postal_code",
        ]
        + [f"covered_recipient_primary_type_{i}" for i in range(1, 7)]
        + [f"covered_recipient_specialty_{i}" for i in range(1, 7)]
        + [f"covered_recipient_license_state_code{i}" for i in range(1, 6)]
    ),
    "hospital_nk": ["teaching_hospital_id", "teaching_hospital_name", "teaching_hospital_ccn"],
    "manufacturer_nk": [
        "applicable_manufacturer_or_applicable_gpo_making_payment_id",
        "applicable_manufacturer_or_applicable_gpo_making_payment_name",
        "applicable_manufacturer_or_applicable_gpo_making_payment_state",
        "applicable_manufacturer_or_applicable_gpo_making_payment_countr",
        "submitting_applicable_manufacturer_or_applicable_gpo_name",
    ],
    "payment_type_nk": ["form_of_payment_or_transfer_of_value", "nature_of_payment_or_transfer_of_value"],
    "travel_nk": ["city_of_travel", "state_of_travel", "country_of_travel"],
    "flags_nk": [
        "change_type", "physician_ownership_indicator", "third_party_payment_recipient_indicator",
        "charity_indicator", "third_party_equals_covered_recipient_indicator",
        "delay_in_publication_indicator", "dispute_status_for_publication", "related_product_indicator",
    ],
}

# ---------------------------------------------------------------
# STAGE: read silver from Postgres ONCE, add a row id and every hash key once
# _rid becomes payment_sk, so the bridge needs no join back to the fact
# ---------------------------------------------------------------
nk_select = ",\n  ".join(f"{nk(cols)} AS {name}" for name, cols in NK_COLS.items())
STAGE_SILVER = f"""
CREATE TABLE stg_silver AS
SELECT ROW_NUMBER() OVER ()::BIGINT AS _rid, *,
  {nk_select}
FROM pg.silver.silver_payments;
"""

# ---------------------------------------------------------------
# DROP: start from nothing so the script can be run again (children first)
# ---------------------------------------------------------------
DROP_GOLD = [
    "DROP TABLE IF EXISTS pg.gold.bridge_payment_product",
    "DROP TABLE IF EXISTS pg.gold.fact_payment",
    "DROP TABLE IF EXISTS pg.gold.dim_recipient",
    "DROP TABLE IF EXISTS pg.gold.dim_teaching_hospital",
    "DROP TABLE IF EXISTS pg.gold.dim_manufacturer",
    "DROP TABLE IF EXISTS pg.gold.dim_payment_type",
    "DROP TABLE IF EXISTS pg.gold.dim_travel",
    "DROP TABLE IF EXISTS pg.gold.dim_payment_flags",
    "DROP TABLE IF EXISTS pg.gold.dim_product",
]

# ---------------------------------------------------------------
# DIMENSIONS (local): distinct values from silver, surrogate key = ROW_NUMBER() ordered by nk
# (NULLIF(...,'') makes '' and NULL the same value)
# ---------------------------------------------------------------
CREATE_DIM_RECIPIENT = f"""
CREATE TABLE dim_recipient AS
SELECT ROW_NUMBER() OVER (ORDER BY nk)::BIGINT AS recipient_sk, *
FROM (
  SELECT DISTINCT
    recipient_nk                                                  AS nk,
    NULLIF(covered_recipient_type::text,'')                          AS recipient_type,
    NULLIF(covered_recipient_profile_id::text,'')                    AS profile_id,
    NULLIF(covered_recipient_npi::text,'')                           AS npi,
    NULLIF(covered_recipient_first_name::text,'')                    AS first_name,
    NULLIF(covered_recipient_middle_name::text,'')                   AS middle_name,
    NULLIF(covered_recipient_last_name::text,'')                     AS last_name,
    NULLIF(covered_recipient_name_suffix::text,'')                   AS name_suffix,
    NULLIF(recipient_primary_business_street_address_line1::text,'') AS address_line1,
    NULLIF(recipient_primary_business_street_address_line2::text,'') AS address_line2,
    NULLIF(recipient_city::text,'')                                  AS city,
    NULLIF(recipient_state::text,'')                                 AS state,
    NULLIF(recipient_zip_code::text,'')                              AS zip_code,
    NULLIF(recipient_country::text,'')                               AS country,
    NULLIF(recipient_province::text,'')                              AS province,
    NULLIF(recipient_postal_code::text,'')                           AS postal_code,
    NULLIF(covered_recipient_primary_type_1::text,'')                AS primary_type_1,
    NULLIF(covered_recipient_primary_type_2::text,'')                AS primary_type_2,
    NULLIF(covered_recipient_primary_type_3::text,'')                AS primary_type_3,
    NULLIF(covered_recipient_primary_type_4::text,'')                AS primary_type_4,
    NULLIF(covered_recipient_primary_type_5::text,'')                AS primary_type_5,
    NULLIF(covered_recipient_primary_type_6::text,'')                AS primary_type_6,
    NULLIF(covered_recipient_specialty_1::text,'')                   AS specialty_1,
    NULLIF(covered_recipient_specialty_2::text,'')                   AS specialty_2,
    NULLIF(covered_recipient_specialty_3::text,'')                   AS specialty_3,
    NULLIF(covered_recipient_specialty_4::text,'')                   AS specialty_4,
    NULLIF(covered_recipient_specialty_5::text,'')                   AS specialty_5,
    NULLIF(covered_recipient_specialty_6::text,'')                   AS specialty_6,
    NULLIF(covered_recipient_license_state_code1::text,'')           AS license_state_1,
    NULLIF(covered_recipient_license_state_code2::text,'')           AS license_state_2,
    NULLIF(covered_recipient_license_state_code3::text,'')           AS license_state_3,
    NULLIF(covered_recipient_license_state_code4::text,'')           AS license_state_4,
    NULLIF(covered_recipient_license_state_code5::text,'')           AS license_state_5
  FROM {SILVER}
) t;
"""

CREATE_DIM_TEACHING_HOSPITAL = f"""
CREATE TABLE dim_teaching_hospital AS
SELECT ROW_NUMBER() OVER (ORDER BY nk)::BIGINT AS teaching_hospital_sk, *
FROM (
  SELECT DISTINCT
    hospital_nk                           AS nk,
    NULLIF(teaching_hospital_id::text,'')   AS hospital_id,
    NULLIF(teaching_hospital_name::text,'') AS hospital_name,
    NULLIF(teaching_hospital_ccn::text,'')  AS hospital_ccn
  FROM {SILVER}
) t;
"""

CREATE_DIM_MANUFACTURER = f"""
CREATE TABLE dim_manufacturer AS
SELECT ROW_NUMBER() OVER (ORDER BY nk)::BIGINT AS manufacturer_sk, *
FROM (
  SELECT DISTINCT
    manufacturer_nk AS nk,
    NULLIF(applicable_manufacturer_or_applicable_gpo_making_payment_id::text,'')     AS manufacturer_id,
    NULLIF(applicable_manufacturer_or_applicable_gpo_making_payment_name::text,'')   AS manufacturer_name,
    NULLIF(applicable_manufacturer_or_applicable_gpo_making_payment_state::text,'')  AS manufacturer_state,
    NULLIF(applicable_manufacturer_or_applicable_gpo_making_payment_countr::text,'') AS manufacturer_country,
    NULLIF(submitting_applicable_manufacturer_or_applicable_gpo_name::text,'')       AS submitting_manufacturer_name
  FROM {SILVER}
) t;
"""

CREATE_DIM_PAYMENT_TYPE = f"""
CREATE TABLE dim_payment_type AS
SELECT ROW_NUMBER() OVER (ORDER BY nk)::BIGINT AS payment_type_sk, *
FROM (
  SELECT DISTINCT
    payment_type_nk AS nk,
    NULLIF(form_of_payment_or_transfer_of_value::text,'')   AS form_of_payment,
    NULLIF(nature_of_payment_or_transfer_of_value::text,'') AS nature_of_payment
  FROM {SILVER}
) t;
"""

CREATE_DIM_TRAVEL = f"""
CREATE TABLE dim_travel AS
SELECT ROW_NUMBER() OVER (ORDER BY nk)::BIGINT AS travel_sk, *
FROM (
  SELECT DISTINCT
    travel_nk AS nk,
    NULLIF(city_of_travel::text,'')    AS travel_city,
    NULLIF(state_of_travel::text,'')   AS travel_state,
    NULLIF(country_of_travel::text,'') AS travel_country
  FROM {SILVER}
) t;
"""

CREATE_DIM_PAYMENT_FLAGS = f"""
CREATE TABLE dim_payment_flags AS
SELECT ROW_NUMBER() OVER (ORDER BY nk)::BIGINT AS payment_flags_sk, *
FROM (
  SELECT DISTINCT
    flags_nk AS nk,
    NULLIF(change_type::text,'')                                    AS change_type,
    NULLIF(physician_ownership_indicator::text,'')                  AS physician_ownership,
    NULLIF(third_party_payment_recipient_indicator::text,'')        AS third_party_payment_recipient,
    NULLIF(charity_indicator::text,'')                              AS charity,
    NULLIF(third_party_equals_covered_recipient_indicator::text,'') AS third_party_is_recipient,
    NULLIF(delay_in_publication_indicator::text,'')                 AS delay_in_publication,
    NULLIF(dispute_status_for_publication::text,'')                 AS dispute_status,
    NULLIF(related_product_indicator::text,'')                      AS related_product
  FROM {SILVER}
) t;
"""

PRODUCT_COLS = ["covered_indicator", "product_type", "product_category", "product_name", "ndc", "pdi"]


# the product is stored in 5 slots in silver, so we stack the 5 slots with UNION ALL (done ONCE)
def product_slots(table):
    return " UNION ALL ".join(
        f"""
  SELECT _rid,
         NULLIF(covered_or_noncovered_indicator_{n}::text,'') AS covered_indicator,
         NULLIF(indicate_drug_or_biological_or_device_or_medical_supply_{n}::text,'') AS product_type,
         NULLIF(product_category_or_therapeutic_area_{n}::text,'') AS product_category,
         NULLIF(name_of_drug_or_biological_or_device_or_medical_supply_{n}::text,'') AS product_name,
         NULLIF(associated_drug_or_biological_ndc_{n}::text,'') AS ndc,
         NULLIF(associated_device_or_medical_supply_pdi_{n}::text,'') AS pdi
  FROM {table}
  WHERE name_of_drug_or_biological_or_device_or_medical_supply_{n} IS NOT NULL
     OR associated_drug_or_biological_ndc_{n} IS NOT NULL
     OR associated_device_or_medical_supply_pdi_{n} IS NOT NULL"""
        for n in range(1, 6)
    )


STAGE_PRODUCT = f"""
CREATE TABLE stg_product AS
SELECT *, {nk(PRODUCT_COLS)} AS nk,
       COUNT(*) OVER (PARTITION BY _rid) AS total_products
FROM ({product_slots(SILVER)}) x;
"""

CREATE_DIM_PRODUCT = f"""
CREATE TABLE dim_product AS
SELECT ROW_NUMBER() OVER (ORDER BY nk)::BIGINT AS product_sk, *
FROM (
  SELECT DISTINCT nk, {", ".join(PRODUCT_COLS)}
  FROM stg_product
) t;
"""

# ---------------------------------------------------------------
# FACT (local): one BIGINT hash join per dimension
# ---------------------------------------------------------------
CREATE_FACT_PAYMENT = f"""
CREATE TABLE fact_payment AS
SELECT
  s._rid AS payment_sk,
  r.recipient_sk, h.teaching_hospital_sk, m.manufacturer_sk, pt.payment_type_sk, t.travel_sk, pf.payment_flags_sk,
  s.record_id::text AS record_id,
  NULLIF(s.date_of_payment::text, '')::date AS date_of_payment,
  NULLIF(s.total_amount_of_payment_usdollars::text, '')::numeric(14,2) AS total_amount_usd,
  NULLIF(s.number_of_payments_included_in_total_amount::text, '')::integer AS payment_count,
  s.name_of_third_party_entity_receiving_payment_or_transfer_of_val AS third_party_entity_name,
  s.contextual_information AS contextual_information,
  NULLIF(s.program_year::text, '')::integer AS program_year,
  NULLIF(s.payment_publication_date::text, '')::date AS payment_publication_date
FROM {SILVER} s
LEFT JOIN dim_recipient         r  ON r.nk  = s.recipient_nk
LEFT JOIN dim_teaching_hospital h  ON h.nk  = s.hospital_nk
LEFT JOIN dim_manufacturer      m  ON m.nk  = s.manufacturer_nk
LEFT JOIN dim_payment_type      pt ON pt.nk = s.payment_type_nk
LEFT JOIN dim_travel            t  ON t.nk  = s.travel_nk
LEFT JOIN dim_payment_flags     pf ON pf.nk = s.flags_nk;
"""

# ---------------------------------------------------------------
# BRIDGE (local): one row per payment + product, weight = 1 / products on the payment
# ---------------------------------------------------------------
CREATE_BRIDGE = """
CREATE TABLE bridge_payment_product AS
SELECT p._rid AS payment_sk, d.product_sk,
       SUM(1.0 / p.total_products)::NUMERIC(10,6) AS allocation_weight
FROM stg_product p
JOIN dim_product d ON d.nk = p.nk
GROUP BY p._rid, d.product_sk;
"""

# ---------------------------------------------------------------
# KEYS: CREATE TABLE AS does not create PK / FK,
# so we send plain Postgres statements through DuckDB
# ---------------------------------------------------------------
ADD_KEYS = [
    "ALTER TABLE gold.dim_recipient ADD PRIMARY KEY (recipient_sk)",
    "ALTER TABLE gold.dim_teaching_hospital ADD PRIMARY KEY (teaching_hospital_sk)",
    "ALTER TABLE gold.dim_manufacturer ADD PRIMARY KEY (manufacturer_sk)",
    "ALTER TABLE gold.dim_payment_type ADD PRIMARY KEY (payment_type_sk)",
    "ALTER TABLE gold.dim_travel ADD PRIMARY KEY (travel_sk)",
    "ALTER TABLE gold.dim_payment_flags ADD PRIMARY KEY (payment_flags_sk)",
    "ALTER TABLE gold.dim_product ADD PRIMARY KEY (product_sk)",
    "ALTER TABLE gold.fact_payment ADD PRIMARY KEY (payment_sk)",
    "ALTER TABLE gold.fact_payment ADD FOREIGN KEY (recipient_sk) REFERENCES gold.dim_recipient (recipient_sk)",
    "ALTER TABLE gold.fact_payment ADD FOREIGN KEY (teaching_hospital_sk) REFERENCES gold.dim_teaching_hospital (teaching_hospital_sk)",
    "ALTER TABLE gold.fact_payment ADD FOREIGN KEY (manufacturer_sk) REFERENCES gold.dim_manufacturer (manufacturer_sk)",
    "ALTER TABLE gold.fact_payment ADD FOREIGN KEY (payment_type_sk) REFERENCES gold.dim_payment_type (payment_type_sk)",
    "ALTER TABLE gold.fact_payment ADD FOREIGN KEY (travel_sk) REFERENCES gold.dim_travel (travel_sk)",
    "ALTER TABLE gold.fact_payment ADD FOREIGN KEY (payment_flags_sk) REFERENCES gold.dim_payment_flags (payment_flags_sk)",
    "ALTER TABLE gold.bridge_payment_product ADD PRIMARY KEY (payment_sk, product_sk)",
    "ALTER TABLE gold.bridge_payment_product ADD FOREIGN KEY (payment_sk) REFERENCES gold.fact_payment (payment_sk)",
    "ALTER TABLE gold.bridge_payment_product ADD FOREIGN KEY (product_sk) REFERENCES gold.dim_product (product_sk)",
]


def check_unique_nk(table):
    n, d = duck.execute(f"SELECT COUNT(*), COUNT(DISTINCT nk) FROM {table}").fetchone()
    assert n == d, f"{table}: hash collision ({n} rows, {d} keys)"


# ===============================================================
# STEP 1: stage silver (the only read from Postgres)
# ===============================================================
duck.execute(STAGE_SILVER)
n_silver = duck.execute(f"SELECT COUNT(*) FROM {SILVER}").fetchone()[0]
print(f"silver staged: {n_silver:,} rows")

# ===============================================================
# STEP 2: dimensions (local, in DuckDB)
# ===============================================================
for name, sql in [
    ("dim_recipient", CREATE_DIM_RECIPIENT),
    ("dim_teaching_hospital", CREATE_DIM_TEACHING_HOSPITAL),
    ("dim_manufacturer", CREATE_DIM_MANUFACTURER),
    ("dim_payment_type", CREATE_DIM_PAYMENT_TYPE),
    ("dim_travel", CREATE_DIM_TRAVEL),
    ("dim_payment_flags", CREATE_DIM_PAYMENT_FLAGS),
]:
    duck.execute(sql)
    check_unique_nk(name)
    print(f"{name} built")

duck.execute(STAGE_PRODUCT)
duck.execute(CREATE_DIM_PRODUCT)
check_unique_nk("dim_product")
print("dim_product built")

# ===============================================================
# STEP 3: fact and bridge (local, in DuckDB)
# ===============================================================
duck.execute(CREATE_FACT_PAYMENT)
n_fact = duck.execute("SELECT COUNT(*) FROM fact_payment").fetchone()[0]
assert n_fact == n_silver, f"fact has {n_fact} rows but silver has {n_silver}: a join duplicated rows"
print("fact_payment built")
duck.execute(CREATE_BRIDGE)
print("bridge_payment_product built")

# ===============================================================
# STEP 4: write the finished tables to Postgres (one write per table)
# ===============================================================
duck.execute("CREATE SCHEMA IF NOT EXISTS pg.gold")
for stmt in DROP_GOLD:
    duck.execute(stmt)
print("gold schema is ready")

for t in [
    "dim_recipient", "dim_teaching_hospital", "dim_manufacturer",
    "dim_payment_type", "dim_travel", "dim_payment_flags", "dim_product",
]:
    duck.execute(f"CREATE TABLE pg.gold.{t} AS SELECT * EXCLUDE (nk) FROM {t}")
    print(f"pg.gold.{t} loaded")
for t in ["fact_payment", "bridge_payment_product"]:
    duck.execute(f"CREATE TABLE pg.gold.{t} AS SELECT * FROM {t}")
    print(f"pg.gold.{t} loaded")

# ===============================================================
# STEP 5: primary / foreign keys
# ===============================================================
for stmt in ADD_KEYS:
    duck.execute(f"CALL postgres_execute('pg', '{stmt}')")
print("keys added")

duck.close()
os.remove(WORK_DB)