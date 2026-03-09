# UCP Enrichment Code — Full Transcription

Source: Lumina Jupyter notebook (PySpark)
Photos: PXL_20260308_050958872.jpg through PXL_20260308_051100868.jpg (8 photos)

---

## Photo 1: PXL_20260308_050958872.jpg — Imports, UDF, client_base_with_partition setup

```python
# UCP DATA

from pyspark.sql.functions import udf, col, lit, when, last_day, broadcast
from pyspark.sql.types import StringType
from datetime import datetime, timedelta

# Function to determine the correct month-end date for a given reference date
def get_month_end_date(reference_date):
    """
    Convert a reference date to the corresponding month-end date for UCP data partition

    Args:
        reference_date: Date to be converted (string or date format)

    Returns:
        String representing the month-end date in 'yyyy-MM-dd' format
    """
    # Convert string to date if needed
    if isinstance(reference_date, str):
        date_obj = datetime.strptime(reference_date, '%Y-%m-%d')
    else:
        date_obj = reference_date

    # Get last day of the reference date's month
    month_end = date_obj.replace(day=28) + timedelta(days=4)
    month_end = month_end - timedelta(days=month_end.day)

    return month_end.strftime('%Y-%m-%d')

# Register UDF for PySpark
get_month_end_udf = udf(get_month_end_date, StringType())

# Add UCP_MONTH_END_DATE column to client_base_final
client_base_with_partition = client_base_final.withColumn(
    "UCP_MONTH_END_DATE",
    get_month_end_udf(col("UCP_REFERENCE_DATE"))
)

# IMPORTANT MAINTENENANCE INCLUDE THE LAST MONTH_END_DT (PARTITION NOT A TABLE FIELD) OF THE UCP TABLE
# IMPORTANT MAINTENENANCE INCLUDE THE LAST MONTH_END_DT (PARTITION NOT A TABLE FIELD) OF THE UCP TABLE
# IMPORTANT MAINTENENANCE INCLUDE THE LAST MONTH_END_DT (PARTITION NOT A TABLE FIELD) OF THE UCP TABLE
```

---

## Photo 2: PXL_20260308_051010540.jpg — Cap to 2025-05-31, display, extract partitions, define extract function

```python
# Lock UCP_MONTH_END_DATE to 2025-05-31 if UCP_REFERENCE_DATE > 2025-04-30!!!!!!
client_base_with_partition = client_base_with_partition.withColumn(
    "UCP_MONTH_END_DATE",
    when(
        col("UCP_REFERENCE_DATE") > lit("2025-05-31"),
        lit("2025-05-31")
    ).otherwise(col("UCP_MONTH_END_DATE"))
)

# Display sample of client base with partition dates
display(client_base_with_partition.select(
    "CLNT_NO", "UCP_REFERENCE_DATE", "UCP_MONTH_END_DATE"
).limit(10))

# Extract unique month-end dates needed
month_end_dates = client_base_with_partition.select("UCP_MONTH_END_DATE").distinct().collect()
month_end_dates = [row.UCP_MONTH_END_DATE for row in month_end_dates]
print(f"Need to extract UCP data from {len(month_end_dates)} month-end partitions: {', '.join(month_end_dates)}")

# Function to extract UCP data for specified clients from a specific partition
def extract_ucp_data_for_partition(month_end_date, client_list, ucp_columns=None):
    """
    Extract UCP data for specified clients from a specific month-end partition

    Args:
        month_end_date: The month-end date partition to read
        client_list: List of client numbers to filter
        ucp_columns: List of UCP columns to extract (None = all columns)

    Returns:
        DataFrame with UCP data for the specified clients
    """
    # Read the specific UCP partition
    partition_path = f'/prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE={month_end_date}'

    try:
        print(f"Reading UCP partition: {partition_path}")
        ucp_partition = spark.read.parquet(partition_path)

        # Select specific columns if provided
        if ucp_columns is not None:
```

---

## Photo 3: PXL_20260308_051016138.jpg — Rest of extract function + column list start

```python
        # (continuation of try block)
        print(f"Reading UCP partition: {partition_path}")
        ucp_partition = spark.read.parquet(partition_path)

        # Select specific columns if provided
        if ucp_columns is not None:
            ucp_partition = ucp_partition.select(*ucp_columns)

        # Filter for the specific clients
        ucp_filtered = ucp_partition.filter(col("CLNT_NO").isin(client_list))

        client_count = ucp_filtered.count()
        print(f"Successfully extracted UCP data for {client_count} clients from {month_end_date} partition")

        if client_count < len(client_list):
            missing_count = len(client_list) - client_count
            print(f"Warning: {missing_count} clients not found in this partition")

        return ucp_filtered
    except Exception as e:
        print(f"Error reading UCP partition {month_end_date}: {str(e)}")
        return None

# Add comprehensive list of UCP columns to extract based on our discussion
ucp_columns_to_extract = [
    # Client Identifier
    "CLNT_NO",  # Client Number Id
    # Demographic Features
    "AGE",  # Client age
    "AGE_RNG",  # Age range bins
    "GENERATION",  # Generation (Boomer, Gen X, Millennial, etc.)
    "CLNT_SPCFC_CD_VAL",
    "NXT_GEN_STUD_SEG_CD",
    "NEW_IMGRNT_SEG_CD",
    "LOG_COMP_SEG_CD",
    "ACTV_PROD_CNT",
    "ACTV_PROD_SRVC_CNT",

    # Tenure and Relationship
    "TENURE_RBC_YEARS",  # Years with bank
    "TENURE_RBC_RNG",  # Tenure range buckets
```

---

## Photo 4: PXL_20260308_051022696.jpg — Column list continued (Credit, Income, Relationship, Products, Lending)

```python
    # Credit Profile
    "CREDIT_SCORE_RNG",  # Credit score range
    "DLQY_IND",  # Delinquency indicator

    # Income and Value
    "INCOME_AFTER_TAX_RNG",  # Income after tax range
    "PROF_TOT_ANNUAL",  # Annual profitability
    "PROF_TOT_MONTHLY",  # Monthly profitability
    "PROF_SEG_CD",  # Profitability segment

    # Relationship Management
    "ACCT_MGD_RELTN_TP_CD",  # Account managed relation type
    "REL_TP_SEG_CD",  # Relationship management type
    "RELN_MG_UNIT_NO",  # Relationship management unit code

    # Product Indicators - Credit Cards
    "CC_VISA_ALL_TOT_IND",  # Has any Visa credit card
    "CC_VISA_CLAS_II_STUD_TOT_IND",
    "CC_VISA_CLAS_STUD_TOT_IND",
    "CC_VISA_DR_IND",  # Has Visa Debit
    "CC_VISA_CLSIC_TOT_IND",  # Has Visa Classic
    "CC_VISA_CLSIC_RWD_TOT_IND",  # Has Visa RBC Rewards+
    "CC_VISA_GOLD_PRFR_TOT_IND",  # Has Visa Gold Preferred
    "CC_VISA_IAV_TOT_IND",  # Has Visa Infinite Avion
    "CC_MASTERCARD_ALL_TOT_IND",  # Has MasterCard

    # Product Indicators - Lending
    "LOAN_TOT_IND",  # Has Lending account
    "MORTGAGE_RESID_TOT_IND",  # Has residential mortgage
    "LOAN_RCL_UNSEC_TOT_IND",  # Has unsecured RCL
    "LOAN_RCL_STUDENT_TOT_IND",
    "LOAN_STUDENT_TOT_IND",
    "PDA_STUDENT_CURR_TOT_IND",
    "PDA_STUDENT_TOT_IND",

    # Multi-Product Relationships
    "MULTI_PROD_RBT_TOT_IND",  # Multi-product rebate indicator
    "SRVC_CNT",  # Service count

    # Online/Digital Banking
    "MOBILE_AUTH_MOB_CNT",  # Mobile Authentication Count
    "OLB_ENROLLED_IND",
    "CPC_OLB_ELIGIBLE",
    "MOBILE_AUTH_CNT",
```

---

## Photo 5: PXL_20260308_051036269.jpg — Column list continued (Digital, Channel, Txn, OFI) + processing loop start

```python
    # Multi-Product Relationships
    "MULTI_PROD_RBT_TOT_IND",  # Multi-product rebate indicator
    "SRVC_CNT",  # Service count

    # Online/Digital Banking
    "MOBILE_AUTH_MOB_CNT",  # Mobile Authentication Count
    "OLB_ENROLLED_IND",
    "CPC_OLB_ELIGIBLE",
    "MOBILE_AUTH_CNT",

    # Channel Preferences
    "D2D_BILL_PYMT_CHNL_PREF_SEG_CD",
    "D2D_DEP_CHNL_PREF_SEG_CD",
    "D2D_INFO_SEEK_CHNL_PREF_SEG_CD",
    "D2D_THF_CHNL_PREF_SEG_CD",
    "D2D_MO_CHNL_PREF_SEG_CD",

    # Transaction Count Variables
    "T_TOT_CNT",  # Total transaction count
    "I_TOT_CNT",  # Information transaction count
    "B_TOT_CNT",  # Balance inquiry count
    "C_TOT_CNT",  # Cash transaction count

    # OFI
    "OFI_M_PROD_CNT",
    "OFI_L_PROD_CNT",
    "OFI_C_PROD_CNT",
    "OFI_T_PROD_CNT"
]

# Create empty DataFrame to store all UCP data
ucp_all = None

# Process each partition
for month_end_date in month_end_dates:
    # Get clients for this partition
    partition_clients = client_base_with_partition.filter(
        col("UCP_MONTH_END_DATE") == month_end_date
    ).select("CLNT_NO").distinct()

    client_count = partition_clients.count()

    if client_count == 0:
        print(f"No clients found for partition {month_end_date}, skipping...")
        continue
```

---

## Photo 6: PXL_20260308_051045977.jpg — Processing loop: small vs large client count branching

```python
# Process each partition
for month_end_date in month_end_dates:
    # Get clients for this partition
    partition_clients = client_base_with_partition.filter(
        col("UCP_MONTH_END_DATE") == month_end_date
    ).select("CLNT_NO").distinct()

    client_count = partition_clients.count()

    if client_count == 0:
        print(f"No clients found for partition {month_end_date}, skipping...")
        continue

    print(f"Extracting UCP data for {client_count} clients from {month_end_date} partition")

    # For small client counts, collect client list and use isin() filter
    # For large client counts, use join approach
    if client_count < 10000:  # Threshold can be adjusted based on your cluster capacity
        client_list = [row.CLNT_NO for row in partition_clients.collect()]

        # Extract UCP data for these clients
        ucp_partition_data = extract_ucp_data_for_partition(
            month_end_date,
            client_list,
            ucp_columns_to_extract
        )
    else:
        # For large client counts, use join instead of filter with isin
        try:
            partition_path = f'/prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE={month_end_date}'
            ucp_partition = spark.read.parquet(partition_path)

            # Select only needed columns
            if ucp_columns_to_extract is not None:
                ucp_partition = ucp_partition.select(*ucp_columns_to_extract)

            # Join with client list using broadcast join
            ucp_partition_data = ucp_partition.join(
                broadcast(partition_clients),
                on="CLNT_NO",
                how="inner"
            )

            print(f"Successfully extracted UCP data for {ucp_partition_data.count()} clients from {month_end_date} partition using join")
        except Exception as e:
            print(f"Error reading UCP partition {month_end_date}: {str(e)}")
```

---

## Photo 7: PXL_20260308_051053746.jpg — Union partitions, count, verify, join with client base

```python
            # Join with client list using broadcast join
            ucp_partition_data = ucp_partition.join(
                broadcast(partition_clients),
                on="CLNT_NO",
                how="inner"
            )

            print(f"Successfully extracted UCP data for {ucp_partition_data.count()} clients from {month_end_date} partition using join")
        except Exception as e:
            print(f"Error reading UCP partition {month_end_date}: {str(e)}")
            ucp_partition_data = None

    if ucp_partition_data is not None:
        # Union with existing data
        if ucp_all is None:
            ucp_all = ucp_partition_data
        else:
            ucp_all = ucp_all.union(ucp_partition_data)

# Count total records
if ucp_all is not None:
    total_ucp_records = ucp_all.count()
    print(f"Total UCP records extracted: {total_ucp_records}")

    # Verify that we have one record per client
    unique_clients = ucp_all.select("CLNT_NO").distinct().count()
    print(f"Unique clients in UCP data: {unique_clients}")

    if total_ucp_records > unique_clients:
        print(f"Warning: Found {total_ucp_records - unique_clients} duplicate client records")

    # Show sample of UCP data
    display(ucp_all.limit(10))
else:
    print("No UCP data was extracted")

# Join UCP data with client base
client_with_ucp = client_base_with_partition.join(
    ucp_all,
    on="CLNT_NO",
    how="left"
)

# Check for clients with missing UCP data
missing_ucp = client_with_ucp.filter(col("AGE").isNull()).count()
```

---

## Photo 8: PXL_20260308_051100868.jpg — Missing UCP check, display, cache, save + output log

```python
# Join UCP data with client base
client_with_ucp = client_base_with_partition.join(
    ucp_all,
    on="CLNT_NO",
    how="left"
)

# Check for clients with missing UCP data
missing_ucp = client_with_ucp.filter(col("AGE").isNull()).count()
total_clients = client_with_ucp.count()

if missing_ucp > 0:
    print(f"Clients missing UCP data: {missing_ucp} out of {total_clients} ({missing_ucp/total_clients*100:.2f}%)")

    # Show sample of clients with missing UCP data
    print("Sample of clients with missing UCP data:")
    display(client_with_ucp.filter(col("AGE").isNull()).select(
        "CLNT_NO", "CLIENT_SEGMENT", "UCP_REFERENCE_DATE", "UCP_MONTH_END_DATE"
    ).limit(10))

# Display final dataset
print("Sample of final client base with UCP data:")
display(client_with_ucp.limit(10))

# Cache the final dataset if needed for further analysis
client_with_ucp.cache()
client_with_ucp.write.mode("overwrite").parquet("/user/427966379/client_with_ucp.parquet")

# Save the dataset if needed
# client_with_ucp.write.parquet('/path/to/save/client_with_ucp', mode='overwrite')
```

### Output log visible at bottom of photo 8:
```
DataFrame[CLNT_NO: string, UCP_REFERENCE_DATE: date, UCP_MONTH_END_DATE: string]
Need to extract UCP data from 30 month-end partitions: 2024-11-30, 2023-12-31, 2023-05-31, 2023-08-31, 2023-04-30, 2023-03-31, 2023-10-31,
-28, 2024-05-31, 2023-09-30, 2025-01-31, 2025-02-28, 2024-12-31, 2025-04-30, 2024-04-30, 2024-07-31, 2025-03-31, 2023-10-31,
-28, 2024-05-31, 2023-09-30, 2025-01-31, 2025-02-28, 2024-12-31, 2025-04-30, 2024-04-30, 2024-07-31, 2025-03-31, 2023-10-31, (? partial)
...2022-12-31, 2024-09-30, 2023-06-30, 2023-01-31, 2023-07-31, 2025-05-31, 2024-02-29, 2024-01-31, 2024-10-31,
1
Extracting UCP data for 100454 clients from 2024-11-30 partition
Successfully extracted UCP data for 99268 clients from 2024-11-30 partition using join
Extracting UCP data for 13917 clients from 2023-12-31 partition
Successfully extracted UCP data for 13892 clients from 2023-12-31 partition using join
```

---

## Key Observations

1. **30 UCP partitions** needed, spanning 2022-12 through 2025-05.
2. **UCP capped at 2025-05-31** — any UCP_REFERENCE_DATE after 2025-04-30 gets mapped to the 2025-05-31 partition (stale data for recent months).
3. **Dual strategy**: isin() filter for <10K clients, broadcast join for >=10K clients per partition.
4. **53 UCP columns** extracted across 11 categories: Demographics, Tenure, Credit, Income, Relationship, Credit Cards, Lending, Multi-Product, Digital Banking, Channel Preferences, Transaction Counts, OFI.
5. **Output**: `/user/427966379/client_with_ucp.parquet`
6. **Missing clients expected**: Not all clients found in every UCP partition (99,268 of 100,454 in the 2024-11-30 partition example).
