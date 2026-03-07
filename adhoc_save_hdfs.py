# Save result_df to HDFS as parquet (run after Cell 4 or Cell 4b)
# Re-run anytime to update the persisted copy

HDFS_PATH = "/user/427966379/vvd_v2_result"

result_df.write.mode("overwrite").parquet(HDFS_PATH)
print(f"Saved result_df to {HDFS_PATH}")

# Verify
check = spark.read.parquet(HDFS_PATH)
print(f"Verification: {check.count():,} rows, {len(check.columns)} columns")
