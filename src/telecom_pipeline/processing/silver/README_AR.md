# Telecom Spark — Incremental (5 datasets)

## أين أضع الملفات؟
انسخ فولدر `incremental/` بالكامل داخل:
`src/telecom_pipeline/processing/silver/incremental/`

احتفظ بملفات `clean_*.py` و`pipeline_paths.py` الأصلية كما هي. السكريبتات الجديدة تستدعي قواعد الـCleaning والـValidation الحالية.

## المتطلبات
1. **Full baseline كامل متحقق منه**: شغل الخمسة Full Refresh cleaners على نفس `TELECOM_RUN_ID` (مثلاً `baseline_001`) بترتيب reference → network reference → customers → incident impact → customer feedback. شغّل الخمسة verifiers أيضًا. لا تستخدم `test_run_001` إلا لو فيه **كل الـ11 datasets**، مش customers فقط.
2. ضع دفعات تغييرات CSV منفصلة (بنفس ترتيب الأعمدة الأصلي) تحت `data/raw/incremental/`: `customers.csv`, `complaints.csv`, `customer_experience.csv`, `network_incidents.csv`, `bridge_incident_customer.csv`.
3. كل Batch عبارة عن **صفوف جديدة/متغيرة فقط**. لا تعدّل ملفات raw/batch الأصلية. لا تحذف سجلات لمجرد غيابها عن Delta.
4. يحتاج جدول `customers` قيم `last_updated_at` متزايدة عند التعديل. الجداول الأخرى لا يوجد فيها حقل تحديث موثوق في الملفات المرفوعة؛ `compare` سيقبل أي اختلاف صادر من Batch أحدث حتى لو كان التغيير قديمًا. يلزم ضمان ترتيب الـBatches من المصدر أو إضافة CDC version/sequence قبل Production.

## PowerShell داخل جذر الـrepo

```powershell
$run = 'inc_001'
$prev = 'baseline_001'
$dir = '/opt/telecom/src/telecom_pipeline/processing/silver/incremental'

# Copy 6 full-refresh reference tables into this run (do not rerun on same run)
docker compose --profile jobs run --rm -e "TELECOM_RUN_ID=$run" -e "TELECOM_PREVIOUS_RUN_ID=$prev" spark-client python3 "$dir/prepare_run.py"

$jobs = @('incremental_customers.py', 'incremental_network_incidents.py', 'incremental_complaints.py', 'incremental_customer_experience.py', 'incremental_incident_customer.py')
foreach ($job in $jobs) {
  docker compose --profile jobs run --rm -e "TELECOM_RUN_ID=$run" -e "TELECOM_PREVIOUS_RUN_ID=$prev" spark-client /opt/spark/bin/spark-submit --master spark://spark-master:7077 "$dir/$job"
  if ($LASTEXITCODE -ne 0) { throw "FAILED: $job" }
}

$tables = @('customers','network_incidents_reference','complaints','customer_experience','bridge_incident_customer')
foreach ($table in $tables) {
  docker compose --profile jobs run --rm -e "TELECOM_RUN_ID=$run" spark-client /opt/spark/bin/spark-submit --master spark://spark-master:7077 "$dir/verify_incremental.py" --table $table
  if ($LASTEXITCODE -ne 0) { throw "FAILED: $table" }
}
```

**IMPORTANT:** `prepare_run.py` copies the six reference tables only. It refuses to overwrite an existing run. Run each incremental table job once per fresh run. Running a job twice on the same Run ID errors, intentionally. For a replay use a different Run ID with the prior successful run as `TELECOM_PREVIOUS_RUN_ID`.

## Behavior
- `customers`: insert or accept strictly newer `last_updated_at` update; reject equal timestamp but changed content and ignore stale updates.
- `complaints`, `network_incidents_reference`, `bridge_incident_customer`: compare all cleaned fields; new keys inserted, differing rows updated. Requires source batches applied in order.
- `customer_experience`: append new `cx_id` only (no historical updates).
- Each job outputs full current Parquet snapshot in its RUN_ID, plus quarantine and JSON delta report.
- Reject duplicate keys inside incoming delta and schema mismatches.
- No deletes/CDC tombstones, no atomic multi-dataset commit, no Azure upload or watermark persistence yet. `RUN_ID` is passed explicitly; only mark the *whole run* committed after five jobs + verification + Azure upload succeed.
- For production use immutable delta files per batch and provide transactional publication/latest pointer, orchestration locks, late-update policy, and retention lifecycle.

## Known limitations
- `verify_customers.py` and some existing full-refresh verifiers have hard-coded expected counts (100000, etc.). For incremental jobs use **verify_incremental.py**, then evolve the existing domain-specific quality checks to accept varying input counts. This generic verification doesn't replace all their business rules.
- This implementation is a **starter/initial integration**, syntax-checked but not executed against your Docker/Spark environment here. Verify against test deltas before integrating with Azure.
- Spark writes a full Parquet snapshot each run, even when only some rows changed.
