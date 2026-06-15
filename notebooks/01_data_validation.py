# ============================================================
# DATA VALIDATION NOTEBOOK
# Purpose: Check raw data for quality issues
# ============================================================

from pyspark.sql.functions import col, isnan, isnull, when
from datetime import datetime

print("=" * 70)
print("DATA VALIDATION REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Read the raw data
df_customers = spark.read.table("my_project.bronze.customers")
df_orders = spark.read.table("my_project.bronze.orders")

# ============================================================
# CHECK 1: ROW COUNTS
# ============================================================
print("\n1️⃣  ROW COUNT CHECK")
print("-" * 70)
customer_count = df_customers.count()
order_count = df_orders.count()
print(f"Total raw customers: {customer_count}")
print(f"Total raw orders: {order_count}")

# ============================================================
# CHECK 2: FIND DUPLICATES
# ============================================================
print("\n2️⃣  DUPLICATE CHECK")
print("-" * 70)
unique_customer_ids = df_customers.select("customer_id").distinct().count()
duplicate_count = customer_count - unique_customer_ids
print(f"Unique customer IDs: {unique_customer_ids}")
print(f"Total customers: {customer_count}")
print(f"Duplicate records: {duplicate_count}")

# Show which customers are duplicated
if duplicate_count > 0:
    print("\nDuplicate customers found:")
    df_customers.groupBy("customer_id").count() \
        .filter(col("count") > 1) \
        .show()

# ============================================================
# CHECK 3: MISSING PHONE NUMBERS
# ============================================================
print("\n3️⃣  MISSING PHONE NUMBER CHECK")
print("-" * 70)
missing_phone = df_customers.filter(col("phone").isNull()).count()
print(f"Customers missing phone: {missing_phone}")

if missing_phone > 0:
    print("\nCustomers with missing phone:")
    df_customers.filter(col("phone").isNull()) \
        .select("customer_id", "customer_name", "phone") \
        .show()

# ============================================================
# CHECK 4: NULL CUSTOMER NAMES
# ============================================================
print("\n4️⃣  NULL CUSTOMER NAME CHECK")
print("-" * 70)
null_names = df_customers.filter(col("customer_name").isNull()).count()
print(f"Customers with NULL name: {null_names}")

if null_names > 0:
    print("\nCustomers with missing names:")
    df_customers.filter(col("customer_name").isNull()).show()

# ============================================================
# CHECK 5: EMAIL FORMAT
# ============================================================
print("\n5️⃣  EMAIL FORMAT CHECK")
print("-" * 70)
invalid_emails = df_customers.filter(
    (~col("email").like("%@%.%")) | (col("email").isNull())
).count()
print(f"Invalid email formats: {invalid_emails}")

# ============================================================
# CHECK 6: ORDERS WITH NULL IDS
# ============================================================
print("\n6️⃣  ORDER DATA QUALITY")
print("-" * 70)
null_order_ids = df_orders.filter(col("order_id").isNull()).count()
null_customer_ids = df_orders.filter(col("customer_id").isNull()).count()
print(f"Orders with NULL order_id: {null_order_ids}")
print(f"Orders with NULL customer_id: {null_customer_ids}")

if null_order_ids > 0 or null_customer_ids > 0:
    print("\nProblematic orders:")
    df_orders.filter(
        (col("order_id").isNull()) | (col("customer_id").isNull())
    ).show()

# ============================================================
# CHECK 7: SUMMARY
# ============================================================
print("\n7️⃣  SUMMARY OF ISSUES FOUND")
print("-" * 70)
total_issues = duplicate_count + missing_phone + null_names + invalid_emails + null_order_ids
print(f"Total data quality issues: {total_issues}")
print("\n✅ VALIDATION COMPLETE")
print("=" * 70)

======================================================================
DATA VALIDATION REPORT
Generated: 2026-06-15 01:31:19
======================================================================

1️⃣  ROW COUNT CHECK
----------------------------------------------------------------------
Total raw customers: 5
Total raw orders: 6

2️⃣  DUPLICATE CHECK
----------------------------------------------------------------------
Unique customer IDs: 5
Total customers: 5
Duplicate records: 0

3️⃣  MISSING PHONE NUMBER CHECK
----------------------------------------------------------------------
Customers missing phone: 1

Customers with missing phone:
+-----------+-------------+-----+
|customer_id|customer_name|phone|
+-----------+-------------+-----+
|          5|  David Brown| NULL|
+-----------+-------------+-----+


4️⃣  NULL CUSTOMER NAME CHECK
----------------------------------------------------------------------
Customers with NULL name: 0

5️⃣  EMAIL FORMAT CHECK
----------------------------------------------------------------------
Invalid email formats: 0

6️⃣  ORDER DATA QUALITY
----------------------------------------------------------------------
Orders with NULL order_id: 0
Orders with NULL customer_id: 0

7️⃣  SUMMARY OF ISSUES FOUND
----------------------------------------------------------------------
Total data quality issues: 1

✅ VALIDATION COMPLETE
======================================================================
