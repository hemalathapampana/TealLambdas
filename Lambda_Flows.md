## Lambda Flows

### AltaworxTealAWSGetDevices.Function

- High-level flow (sequential function calls)
  1. FunctionHandler
  2. TryProcessDeviceList
  3. ProcessDeviceList
  4. CompleteProcessGetDevice or SendMessageToQueue
  5. (Within CompleteProcessGetDevice) UpdateTealDevicesWithPolicy -> UpdateTealDevicesFromStaging -> SendMessageToGetDeviceUsageQueue

- Low-level flow (what each function does)
  - FunctionHandler(SQSEvent, ILambdaContext):
    - Initialize `KeySysLambdaContext` via `BaseFunctionHandler` and create `TealRepository`.
    - Resolve env vars: `TEAL_DEVICES_GET_URL`, `TEAL_DESTINATION_QUEUE_GET_DEVICES_URL`, `TEAL_DEVICE_USAGE_QUEUE_URL` (fallback to `context.ClientContext.Environment` if missing, and log warnings).
    - If `sqsEvent` contains records: iterate records -> construct `SqsValues` per record -> call `TryProcessDeviceList` for each. Else: create default `SqsValues` and call `TryProcessDeviceList`.
    - Finally, call `CleanUp`.
  - TryProcessDeviceList(context, sqsValues):
    - If `CurrentServiceProviderId` is 0: get next SP id via `ServiceProviderCommon.GetNextServiceProviderId`.
      - If found: `TruncateTealDeviceAndUsageStagingWithPolicy` then set `CurrentServiceProviderId`.
      - If not: log error and return.
    - Call `ProcessDeviceList`.
  - TruncateTealDeviceAndUsageStagingWithPolicy(context):
    - Build SQL transient retry policy via `RetryPolicyHelper.GetSqlTransientPolicy` and execute `TruncateTealDeviceAndUsageStaging`.
  - TruncateTealDeviceAndUsageStaging(context):
    - Execute stored procedure `usp_Teal_Truncate_Device_And_Usage_Staging` on `CentralDbConnectionString`.
  - ProcessDeviceList(context, sqsValues):
    - Build `TealAPIService` using auth from `_tealRepository.GetTealAuthenticationInformation`.
    - Loop pages while not last page and within time cut-off; keep a `failCount` with threshold 5.
    - For each page: call `TealAPIService.RequestTealListAsync` with page params under retry policy; if success, follow operation result link via `GetTealOperationResultAsync`.
    - Deserialize `TealDeviceResponseRootObject`; map entries into a `DataTable` via `AddToDataRow`; increment `PageNumber`.
    - When no entries and first page -> log “no devices” and return; when no entries later -> mark `isLastPage=true`.
    - If table has rows: bulk copy to DB via `SqlBulkCopy(..., TealDeviceStaging)`.
    - If failed pages exceed limit -> stop syncing, treat as last page.
    - If `isLastPage` -> `CompleteProcessGetDevice`; else -> `SendMessageToQueue` for next page.
  - CompleteProcessGetDevice(context, sqsValues, tealAuth):
    - `UpdateTealDevicesWithPolicy` (which wraps `UpdateTealDevicesFromStaging` under SQL retry) to merge staging into final tables.
    - `SendMessageToGetDeviceUsageQueue` to kick off `AltaworxTealAWSGetDeviceUsage` for the same Service Provider.
    - Check for next service provider via `ServiceProviderCommon.GetNextServiceProviderId`; if present, reset `PageNumber=0` and `SendMessageToQueue` to continue devices sync for next SP.
  - UpdateTealDevicesWithPolicy(context, spId, conn, tealAuth, logger):
    - Execute `UpdateTealDevicesFromStaging` under retry policy.
  - UpdateTealDevicesFromStaging(context, spId, conn, tealAuth, logger):
    - Compute `BillingPeriod` via `GetBillingPeriod` then execute stored procedure `usp_Teal_Update_Device_From_Staging` with billing params.
  - SendMessageToQueue(context, sqsValues):
    - Send SQS message to `_tealDestinationQueueGetDevicesURL` with attributes `PageNumber` and `CurrentServiceProviderId` and a delay of 30 seconds.
  - SendMessageToGetDeviceUsageQueue(context, url, spId):
    - Send SQS message to `url` with attribute `ServiceProviderId`.
  - GetBillingPeriod(...):
    - Compute billing window via `BillingPeriodHelper.GetBillingPeriodForServiceProvider` and adjust to next month if needed.

### AltaworxTealAWSGetDeviceUsage.Function

- High-level flow (sequential function calls)
  1. FunctionHandler
  2. ProcessGetDeviceUsage
  3. ProcessGetDeviceDataUsage (called twice per loop for period and daily windows)
  4. CopyDataToTable
  5. ProcessFinalProcessGetDeviceUsage or SendMessageToGetDeviceUsageQueue
  6. (Within ProcessFinalProcessGetDeviceUsage) UpdateTealDeviceUsageWithPolicy -> UpdateTealDeviceUsage -> SendMessageToJasperDeviceCleanUpQueue

- Low-level flow (what each function does)
  - FunctionHandler(SQSEvent, ILambdaContext):
    - Initialize `KeySysLambdaContext` via `BaseFunctionHandler` and create `TealRepository`.
    - Resolve env vars: `TEAL_DEVICES_DATA_USAGE_URL`, `TEAL_DEVICE_USAGE_QUEUE_URL`, `CLEAN_UP_QUEUE_URL` (fallback to `context.ClientContext.Environment` with warnings).
    - Validate `sqsEvent` has records; otherwise log and return (not intended for scheduled runs).
    - For each SQS record: log, create `SqsValues`, and call `ProcessGetDeviceUsage`.
    - Finally, call `CleanUp`.
  - ProcessGetDeviceUsage(context, sqsValues):
    - Get `TealAuthentication` and build `TealAPIService`; compute `BillingPeriod`.
    - While time remains and not last page:
      - Compute `billingPeriodStart` (midnight). Call `ProcessGetDeviceDataUsage` for billing-period range.
      - If results exist: `CopyDataToTable` into `TealDeviceUsageStaging` and increment `PageNumber`; else: if first page, log “no device usage”, then mark last page and break.
      - Additionally compute yesterday/today midnight and call `ProcessGetDeviceDataUsage` for daily usage window; if results exist, `CopyDataToTable` into `TealDeviceUsageDailyStaging`.
    - If `isLastPage`: `ProcessFinalProcessGetDeviceUsage`; else: `SendMessageToGetDeviceUsageQueue` for next page.
  - CopyDataToTable(context, usages, sqsValues, tableName, usageDate):
    - Group usage by `EID` to compute total usage, build `DataTable` via `BuildTealDeviceDataUsageTable` + `AddToDataRow`, then `SqlBulkCopy` to target table.
  - ProcessGetDeviceDataUsage(context, sqsValues, tealService, url, start, end):
    - Request usage list via `RequestTealDeviceUsageAsync` with retry; get operation result via `GetTealOperationResultAsync` and deserialize `TealDeviceUsageResponseRootObject`. Return `Entries` as list.
  - ProcessFinalProcessGetDeviceUsage(context, sqsValues, tealAuth):
    - `UpdateTealDeviceUsageWithPolicy` (wraps `UpdateTealDeviceUsage`) then `SendMessageToJasperDeviceCleanUpQueue` to trigger cleanup Lambda.
  - UpdateTealDeviceUsageWithPolicy(context, spId, conn, tealAuth, logger):
    - Execute `UpdateTealDeviceUsage` under SQL retry policy.
  - UpdateTealDeviceUsage(context, spId, conn, tealAuth, logger):
    - Execute stored procedure `usp_Teal_Update_Device_Usage`.
  - SendMessageToGetDeviceUsageQueue(context, url, sqsValues):
    - Send SQS message to continue pagination with attributes `ServiceProviderId` and `PageNumber`.
  - SendMessageToJasperDeviceCleanUpQueue(context, url, spId):
    - Send SQS message with attributes `RetryCount`, `IntegrationType`, `ServiceProviderId` and `MessageBody` signaling end of devices process.
  - GetBillingPeriod(...):
    - Compute billing window via `BillingPeriodHelper.GetBillingPeriodForServiceProvider`, adjust month if needed, and log.