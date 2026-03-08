# UCP Enrichment Logic — Transcribed from Jupyter Notebook Screenshots

Source: Phone screenshots in `c:/Users/andre/Proton Drive/Andre.avs/My files/pics/`
Images: `PXL_20260308_050958872.jpg` through `PXL_20260308_051100868.jpg` (8 images)
Date captured: 2026-03-08

---

## Section 1: Raw Code Transcription

### Cell: UCP DATA — Imports and Month-End Date UDF

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

### Cell: Lock UCP_MONTH_END_DATE Cap + Extract Partition Dates

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
```

### Cell: UCP Partition Extraction Function

```python
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
```

### Cell: UCP Columns to Extract (Comprehensive List)

```python
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
    "CC_VISA_CAS_STUD_TOT_IND",
    "CC_VISA_DR_IND",  # Has Visa Debit
    "CC_VISA_CLSIC_TOT_IND",  # Has Visa Classic
    "CC_VISA_CLSIC_RWD_TOT_IND",  # Has Visa RBC Rewards+
    "CC_VISA_GOLD_PRFR_TOT_IND",  # Has Visa Gold Preferred
    "CC_VISA_INF_TOT_IND",  # Has Visa Infinite
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

    # Channel Preferences
    "D2D_BILL_PYMT_CHNL_PREF_SEG_CD",
    "D2D_DEP_CHNL_PREF_SEG_CD",
    "D2D_INFO_SEEK_CHNL_PREF_SEG_CD",
    "D2D_TRF_CHNL_PREF_SEG_CD",
    "D2D_MQ_CHNL_PREF_SEG_CD",

    # Transaction Count Variables
    "T_TOT_CNT",  # Total transaction count
    "I_TOT_CNT",  # Information transaction count
    "B_TOT_CNT",  # Balance inquiry count
    "C_TOT_CNT",  # Cash transaction count

    # OFI
    "OFI_M_PROD_CNT",
    "OFI_L_PROD_CNT",
    "OFI_C_PROD_CNT",
    "OFI_I_PROD_CNT",
    "OFI_T_PROD_CNT",
]
```

### Cell: Process Each Partition — Main Loop

```python
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
            ucp_partition_data = None

    if ucp_partition_data is not None:
        # Union with existing data
        if ucp_all is None:
            ucp_all = ucp_partition_data
        else:
            ucp_all = ucp_all.union(ucp_partition_data)
```

### Cell: Validation + Join with Client Base

```python
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

### Cell Output (visible at bottom of last screenshot)

```
DataFrame[CLNT_NO: string, UCP_REFERENCE_DATE: date, UCP_MONTH_END_DATE: string]
Need to extract UCP data from 30 month-end partitions: 2024-11-30, 2023-12-31, 2023-05-31, 2023-08-31, 2023-04-30, 2023-03-31, 2024-04-30, 2024-07-31, 2025-03-31, 2023-10-31,
-28, 2024-05-31, 2023-09-30, 2025-01-31, 2025-02-28, 2024-12-31, 2025-04-30, 2024-07-31, 2025-03-31, 2023-10-31,
2022-12-31, 2024-09-30, 2023-06-30, 2023-01-31, 2023-07-31, 2024-02-29, 2024-01-31, 2024-10-31,
...

Extracting UCP data for 100454 clients from 2024-11-30 partition
Successfully extracted UCP data for 99268 clients from 2024-11-30 partition using join
Extracting UCP data for 13917 clients from 2023-12-31 partition
Successfully extracted UCP data for 13892 clients from 2023-12-31 partition using join
```

---

## Section 2: Table/Column Inventory

### UCP Source Table
- **Path**: `/prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE={yyyy-MM-dd}`
- **Format**: Parquet, partitioned by `MONTH_END_DATE`
- **Partition key**: `MONTH_END_DATE` (string, format `yyyy-MM-dd`, last day of month)
- **Note**: `MONTH_END_DATE` is a PARTITION field, NOT a table column

### UCP Columns Extracted (by Category)

| Category | Column | Description |
|----------|--------|-------------|
| **Identifier** | `CLNT_NO` | Client number (join key) |
| **Demographics** | `AGE` | Client age |
| | `AGE_RNG` | Age range bins |
| | `GENERATION` | Generation segment (Boomer, Gen X, Millennial, etc.) |
| | `CLNT_SPCFC_CD_VAL` | Client specific code value |
| | `NXT_GEN_STUD_SEG_CD` | Next gen student segment code |
| | `NEW_IMGRNT_SEG_CD` | New immigrant segment code |
| | `LOG_COMP_SEG_CD` | Loyalty/competitive segment code |
| | `ACTV_PROD_CNT` | Active product count |
| | `ACTV_PROD_SRVC_CNT` | Active product/service count |
| **Tenure** | `TENURE_RBC_YEARS` | Years with bank |
| | `TENURE_RBC_RNG` | Tenure range buckets |
| **Credit** | `CREDIT_SCORE_RNG` | Credit score range |
| | `DLQY_IND` | Delinquency indicator |
| **Income/Value** | `INCOME_AFTER_TAX_RNG` | Income after tax range |
| | `PROF_TOT_ANNUAL` | Annual profitability |
| | `PROF_TOT_MONTHLY` | Monthly profitability |
| | `PROF_SEG_CD` | Profitability segment |
| **Relationship** | `ACCT_MGD_RELTN_TP_CD` | Account managed relation type |
| | `REL_TP_SEG_CD` | Relationship management type |
| | `RELN_MG_UNIT_NO` | Relationship management unit code |
| **Visa Cards** | `CC_VISA_ALL_TOT_IND` | Has any Visa credit card |
| | `CC_VISA_CLAS_II_STUD_TOT_IND` | Visa Classic II Student indicator |
| | `CC_VISA_CAS_STUD_TOT_IND` | Visa Cash Student indicator |
| | `CC_VISA_DR_IND` | Has Visa Debit |
| | `CC_VISA_CLSIC_TOT_IND` | Has Visa Classic |
| | `CC_VISA_CLSIC_RWD_TOT_IND` | Has Visa RBC Rewards+ |
| | `CC_VISA_GOLD_PRFR_TOT_IND` | Has Visa Gold Preferred |
| | `CC_VISA_INF_TOT_IND` | Has Visa Infinite |
| | `CC_VISA_IAV_TOT_IND` | Has Visa Infinite Avion |
| | `CC_MASTERCARD_ALL_TOT_IND` | Has MasterCard |
| **Lending** | `LOAN_TOT_IND` | Has Lending account |
| | `MORTGAGE_RESID_TOT_IND` | Has residential mortgage |
| | `LOAN_RCL_UNSEC_TOT_IND` | Has unsecured RCL |
| | `LOAN_RCL_STUDENT_TOT_IND` | Student RCL indicator |
| | `LOAN_STUDENT_TOT_IND` | Student loan indicator |
| | `PDA_STUDENT_CURR_TOT_IND` | PDA student current indicator |
| | `PDA_STUDENT_TOT_IND` | PDA student total indicator |
| **Multi-Product** | `MULTI_PROD_RBT_TOT_IND` | Multi-product rebate indicator |
| | `SRVC_CNT` | Service count |
| **Digital** | `MOBILE_AUTH_MOB_CNT` | Mobile authentication count |
| | `OLB_ENROLLED_IND` | Online banking enrolled indicator |
| | `CPC_OLB_ELIGIBLE` | CPC online banking eligible |
| | `MOBILE_AUTH_CNT` | Mobile authentication count |
| **Channel Pref** | `D2D_BILL_PYMT_CHNL_PREF_SEG_CD` | Bill payment channel preference |
| | `D2D_DEP_CHNL_PREF_SEG_CD` | Deposit channel preference |
| | `D2D_INFO_SEEK_CHNL_PREF_SEG_CD` | Info seeking channel preference |
| | `D2D_TRF_CHNL_PREF_SEG_CD` | Transfer channel preference |
| | `D2D_MQ_CHNL_PREF_SEG_CD` | MQ channel preference |
| **Transactions** | `T_TOT_CNT` | Total transaction count |
| | `I_TOT_CNT` | Information transaction count |
| | `B_TOT_CNT` | Balance inquiry count |
| | `C_TOT_CNT` | Cash transaction count |
| **OFI** | `OFI_M_PROD_CNT` | OFI mutual fund product count |
| | `OFI_L_PROD_CNT` | OFI lending product count |
| | `OFI_C_PROD_CNT` | OFI credit product count |
| | `OFI_I_PROD_CNT` | OFI investment product count |
| | `OFI_T_PROD_CNT` | OFI total product count |

**Total columns extracted: 53** (including CLNT_NO)

### Upstream DataFrame: `client_base_final`
Contains at minimum:
- `CLNT_NO` (string)
- `UCP_REFERENCE_DATE` (date) -- the reference date used to determine which UCP partition to read
- `CLIENT_SEGMENT` -- visible in diagnostics

---

## Section 3: Join Logic Summary

### Step 1: Derive UCP Partition Date
Each client in `client_base_final` has a `UCP_REFERENCE_DATE`. A UDF converts this to the last day of that month (`UCP_MONTH_END_DATE`) to match UCP partition keys.

**Key override**: Any `UCP_REFERENCE_DATE > 2025-05-31` is capped to `2025-05-31`. This means the most recent available UCP partition is May 2025. Data after that date reuses the May 2025 snapshot.

### Step 2: Identify Required Partitions
Distinct `UCP_MONTH_END_DATE` values are collected. The output shows **30 distinct month-end partitions** spanning from approximately 2022-12-31 to 2025-05-31.

### Step 3: Per-Partition Extraction (Two Strategies)
For each partition:
1. **Small partitions (< 10,000 clients)**: Collect client list to driver, use `isin()` filter on UCP parquet
2. **Large partitions (>= 10,000 clients)**: Read full UCP partition, select needed columns, then **broadcast join** with partition_clients on `CLNT_NO` using `inner` join

### Step 4: Union All Partitions
All per-partition results are unioned into `ucp_all` DataFrame.

### Step 5: Final Join to Client Base
```
client_with_ucp = client_base_with_partition.join(
    ucp_all,
    on="CLNT_NO",
    how="left"
)
```
- **Left join** preserves all experiment clients even if no UCP data found
- Missing UCP data is detected by checking `AGE IS NULL`

### Step 6: Persist
```
client_with_ucp.write.mode("overwrite").parquet("/user/427966379/client_with_ucp.parquet")
```

### Join Key
- `CLNT_NO` (string) in both UCP and experiment data
- No type conversion needed -- both are already strings at this point

---

## Section 4: Key Observations and Gotchas

### Critical Issues

1. **UCP_MONTH_END_DATE cap at 2025-05-31**: Any experiment data after May 2025 uses stale UCP profile data. Since the experiment runs through 2026-03-06, roughly 9+ months of data will use the May 2025 UCP snapshot. This is a significant staleness issue -- client demographics, product holdings, and profitability may have changed.

2. **MONTH_END_DATE is a PARTITION field, not a column**: The code comments emphasize this three times with exclamation marks. You cannot query it as a regular column -- you must use the partition path.

3. **30 partitions required**: The output shows 30 distinct month-end partitions are needed. This means 30 separate parquet reads per run.

4. **Missing clients in partitions**: The output shows 100,454 clients needed from 2024-11-30 but only 99,268 found (1,186 missing = 1.2%). Some clients exist in the experiment but have no UCP record in the corresponding month.

5. **Duplicate risk**: The code explicitly checks for `total_ucp_records > unique_clients` and warns about duplicates. If UCP has multiple rows per client in a partition, this will inflate the join.

6. **10,000 client threshold**: The switch between `isin()` and broadcast join is at 10,000 clients. This is a tunable parameter -- the comment says "Threshold can be adjusted based on your cluster capacity."

### Upstream Dependency: `client_base_final`

The code starts with `client_base_final` which must already contain `CLNT_NO` and `UCP_REFERENCE_DATE`. This DataFrame is NOT defined in the visible code -- it comes from an earlier cell. It likely derives from the VVD experiment pipeline's `result_df`.

### `UCP_REFERENCE_DATE` -- Source Unknown

The column `UCP_REFERENCE_DATE` on `client_base_final` is the anchor for which UCP snapshot to use per client. Its derivation is NOT visible in these screenshots. Possible sources:
- `TREATMT_STRT_DT` (treatment start date from experiment)
- `FIRST_ACQUISITION_SUCCESS_DT` or similar success date
- A fixed reference date per cohort

This is a **critical unknown** -- the choice of reference date determines which UCP snapshot each client gets matched to.

### HDFS Output Path
- Final enriched dataset: `/user/427966379/client_with_ucp.parquet`
- Written with `mode("overwrite")` -- destructive if run multiple times

### Performance Considerations
- Uses `broadcast()` for the client list join -- assumes partition_clients fits in driver memory (should be fine for <500K clients per partition)
- No explicit caching of intermediate UCP partitions
- `client_with_ucp.cache()` is called on the final result before writing

### What's NOT in These Screenshots
- How `client_base_final` is constructed (the upstream pipeline cell)
- What `UCP_REFERENCE_DATE` is derived from
- Any analysis cells that USE the enriched data
- Whether there's deduplication logic for clients appearing in multiple cohorts
- The `CLIENT_SEGMENT` column definition
