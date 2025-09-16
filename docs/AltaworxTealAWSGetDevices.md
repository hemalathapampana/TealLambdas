## AltaworxTealAWSGetDevices – Detailed Design, Flow, and Operations

This document describes, in depth, how the Lambda `AltaworxTealAWSGetDevices.Function` works, addressing the 12 topics and sub‑questions requested. All behaviors are based on the current implementation in:

- `AltaworxTealAWSGetDevices.cs`
- `AwsFunctionBase.cs`
- `TealHelper.cs`
- `RetryPolicyHelper.cs`

Where applicable, concrete constants, environment variables, functions, and stored procedures are cited exactly as implemented.

### 1) Trigger & Setup

- **What generates the event that triggers this Lambda? What makes the trigger happen?**
  - The Lambda supports two entry paths:
    - Direct invocation (e.g., scheduled by EventBridge): If there is no SQS event, `FunctionHandler` creates a default `SqsValues` and runs the flow. This is explicitly called out in the header comment of `AltaworxTealAWSGetDevices.cs` describing step 1 being triggered by EventBridge.
    - SQS message: When invoked by SQS (`SQSEvent` records present), each message is processed via `TryProcessDeviceList(...)`. The Lambda also re‑enqueues itself (via SQS) to paginate across all device pages and to move to the next service provider.

- **Reads sync state from the message (provider ID, page number, retry/initialization flags):**
  - Message attributes parsed by `SqsValues` include at least:
    - `CurrentServiceProviderId`
    - `PageNumber`
  - There are no explicit custom retry/initialization flags in the SQS message for this Lambda. Initialization is inferred when `CurrentServiceProviderId == 0`.

- **Initializes the Lambda context and prepares staging tables:**
  - Initialization is done via `BaseFunctionHandler(context)` (from `AwsFunctionBase`) which constructs a `KeySysLambdaContext` and loads configuration.
  - When initialization is needed (no `CurrentServiceProviderId` in the message), the Lambda:
    1) Determines the next service provider with `ServiceProviderCommon.GetNextServiceProviderId(...)`.
    2) Clears staging via `TruncateTealDeviceAndUsageStagingWithPolicy(...)` (SQL retry‑wrapped call to a truncate stored procedure).

Environment variables used during setup:

- `TealHelper.CommonString.TEAL_DEVICES_GET_URL` → `TealDevicesGetURL`
- `TealHelper.CommonString.TEAL_DESTINATION_QUEUE_GET_DEVICES_URL` → `TealDestinationQueueGetDevicesURL`
- `TealHelper.CommonString.TEAL_DEVICE_USAGE_QUEUE_URL` → `TealDeviceUsageQueueURL`

### 2) Processing Paths

#### 2.1) Retry Initialization (Why is retry the first step?)

- On first entry for a service provider (`CurrentServiceProviderId == 0`), the code resolves the provider and immediately clears staging with a SQL retry policy:
  - `TruncateTealDeviceAndUsageStagingWithPolicy(...)` → wraps `TruncateTealDeviceAndUsageStaging(...)` in `RetryPolicyHelper.GetSqlTransientPolicy(...)`.
  - Rationale: Protects truncation from transient SQL connectivity or resource errors before any data pull begins. Ensures a clean staging state is reliably established before processing pages.

#### 2.2) Clearing Staging Tables

- **Are device/usage staging tables cleared at the start of each run?**
  - Yes, at the moment the Lambda selects a new `ServiceProviderId` (initialization), it calls `usp_Teal_Truncate_Device_And_Usage_Staging` (via `TruncateTealDeviceAndUsageStaging`) to clear the relevant Teal staging tables.

- **Would staging tables not be cleared automatically once the previous day’s run completed?**
  - The code does not rely on end‑of‑run cleanup. It explicitly truncates at the start of a new run/instance for safety and determinism. This is resilient against prior failures or partial runs.

- **Stored procedure used:**
  - `Amop.Core.Constants.SQLConstant.StoredProcedureName.usp_Teal_Truncate_Device_And_Usage_Staging`

#### 2.3) BAN/FAN Status Storage (Telegence‑specific)

- Not applicable to Teal. The Teal device sync does not fetch or store BAN/FAN statuses. There are no `BillingAccountNumberStatusStaging` references in this Lambda.

#### 2.4) Normal Flow

- **Retrieves BAN list statuses and applies FAN filters—retrieves from where?**
  - Not applicable to Teal. The Teal device sync does not involve BAN/FAN filtering.

- **Fetches device list from Teal API in pages—API details:**
  - The Lambda uses `TealAPIService` to call Teal’s device list API in two steps:
    1) Submit a list request: `tealService.RequestTealListAsync(_tealGetDevicesPath, TealHelper.CommonString.TEAL_GET_DEVICE, pageNumber, PAGE_SIZE, logger)`.
       - `TealHelper.CommonString.TEAL_GET_DEVICE` is an operation tag.
       - The request returns an operation link (async job) in `Link.OperationResultLink.Href`.
    2) Poll the operation result: `tealService.GetTealOperationResultAsync(operationResultLink, logger)` to obtain the `entries` (device list page).
  - API path and parameters:
    - Base path is provided via env var `TealDevicesGetURL`.
    - Pagination is offset/limit style using:
      - `offset` ← `PageNumber`
      - `limit` ← `TealHelper.CommonConfig.PAGE_SIZE` (100)
    - Constants available in `TealHelper.CommonString`: `OFFSET`, `LIMIT`, `TEAl_DEVICE_API`.

- **Page size / page limit:**
  - Page size: `TealHelper.CommonConfig.PAGE_SIZE = 100`.
  - There is no predefined “page limit” count. Pagination proceeds until the API returns an empty `entries` page.

- **Bulk inserts devices into Device Staging:**
  - Devices returned in the page are accumulated into a `DataTable` constructed by `BuildTealDeviceDataTable()` and then bulk copied:
    - Destination table: `DatabaseTableNames.TealDeviceStaging` (via `AwsFunctionBase.SqlBulkCopy(...)`).
  - Staging columns populated include: `ICCID`, `IMSI`, `IMEI` (note: set to `device.IMSI` in code), `MSISDN`, `EID`, `DeviceStatus`, plan details (`PlanId`, `PlanUuid`, `PlanName`, `PlanVolumeUnit`, `PlanPrice`, `PlanChangeStatus`), client details (`ClientId`, `ClientName`, `ClientTitle`, `ClientUuid`), `Sku`, `BatchNumber`, `DeviceGroupName`, `ClientChangeTimestamp`, `ProfileChangeStatus`, `FlowType`, `ServiceProviderId`, `CreatedDate`.

- **If there are more pages → enqueues another fetch job. What is the page limit?**
  - After a successful page with entries, `PageNumber` is incremented. If not at the last page, the Lambda enqueues itself to continue:
    - `SendMessageToQueue(...)` sends a new SQS message to `TealDestinationQueueGetDevicesURL` with message attributes:
      - `PageNumber`
      - `CurrentServiceProviderId`
    - The message is sent with a delay: `DelaySeconds = 30` (`DELAY_SQS_MESSAGE_IN_SECONDS`).
  - There is no fixed “page limit”; processing continues until an empty `entries` is encountered.

- **If all pages are complete → triggers the Device Usage Lambda. How is completeness determined?**
  - Completion condition: The loop sets `isLastPage = true` when `entries` is null/empty for a non‑initial page. It then calls:
    - `CompleteProcessGetDevice(...)` which:
      1) Merges staging to main via `UpdateTealDevicesWithPolicy(...)` → `usp_Teal_Update_Device_From_Staging`.
      2) Enqueues the device usage workflow via `SendMessageToGetDeviceUsageQueue(...)` to `TealDeviceUsageQueueURL` with attribute `ServiceProviderId`.
      3) Attempts to move to the next service provider by getting `ServiceProviderCommon.GetNextServiceProviderId(...)` and, if found, re‑enqueues the device list Lambda for page 0 of the next provider.

#### 2.5) Missing Devices Handling (Telegence‑style)

- Not implemented in Teal. This Lambda does not detect cross‑system “missing devices,” does not perform subscriber‑level validation, and does not enqueue special handling for such devices. Those steps are specific to the Telegence integration and are not present here.

### 3) Error Handling & Retry

- **Exponential backoff—how?**
  - Two retry layers are used:
    - HTTP/API retry: `TealHelper.BuildRetryPolicy(...)` wraps API calls such as `RequestTealListAsync` and `GetTealOperationResultAsync` with Polly `WaitAndRetryAsync`.
      - Attempts: `TealHelper.CommonConfig.RETRY_NUMBER = 3`.
      - Backoff: `TimeSpan.FromSeconds(Math.Pow(retryNumber, retryAttempt))` → 3s, 9s, 27s.
      - Includes a fallback that marks the result as error (`HasErrors = true`).
    - SQL retry: `RetryPolicyHelper.GetSqlTransientPolicy(...)` with `SQL_TRANSIENT_RETRY_MAX_COUNT = 3` guards truncate and update stored procedure calls.
    - Additionally, the underlying HTTP service (`TealAPIService`) uses `RetryPolicyHelper.PollyRetryHttpRequestAsync(...)` to add resilience to the raw HTTP request layer.

- **Re‑enqueues messages if the device list is incomplete or timed out—detail this:**
  - Time‑remaining cutoff: If `context.Context.RemainingTime` is below `CommonConstants.REMAINING_TIME_CUT_OFF`, the loop breaks early.
  - After the loop:
    - If `isLastPage == false` and the function broke due to time or because there are more pages, it calls `SendMessageToQueue(...)` to continue on the next invocation with the current `PageNumber` and `CurrentServiceProviderId`.
    - If API calls failed too many times (`failCount >= TEAL_SYNC_FAIL_ACCEPTABLE_LIMIT = 5`), the Lambda stops the device sync by forcing `isLastPage = true`, updates devices from staging, and proceeds to device usage. This intentionally avoids infinite loops under persistent external failures.
  - SQS message details for continuation:
    - Queue URL: `TealDestinationQueueGetDevicesURL` (env var).
    - Attributes: `PageNumber`, `CurrentServiceProviderId`.
    - `DelaySeconds`: 30.

### 4) Reference of Core Components and Their Roles

- **Main entrypoints**
  - `FunctionHandler(SQSEvent, ILambdaContext)`: Top‑level handler. Initializes context, parses SQS messages or creates default `SqsValues`, and delegates to `TryProcessDeviceList`.
  - `TryProcessDeviceList(KeySysLambdaContext, SqsValues)`: Resolves `ServiceProviderId` if missing, truncates staging with SQL retry policy, and calls `ProcessDeviceList`.
  - `ProcessDeviceList(KeySysLambdaContext, SqsValues)`: Core pagination loop, API calls, DataTable creation, bulk copy into `TealDeviceStaging`, continuation vs. completion branching.

- **Initialization functions**
  - `BaseFunctionHandler(ILambdaContext)`: Creates `KeySysLambdaContext`, loads OU settings/configuration.
  - `ServiceProviderCommon.GetNextServiceProviderId(connection, IntegrationType.Teal, currentId)`: Determines initial or next service provider to process.

- **Device fetch**
  - `TealAPIService.RequestTealListAsync(baseUrl, opTag, offset, limit, logger)`: Kicks off async device list request; returns an operation result link.
  - `TealAPIService.GetTealOperationResultAsync(operationResultLink, logger)`: Polls final list result, returning entries for the page.
  - Retry wrappers: `TealHelper.BuildRetryPolicy(...)` and `RetryPolicyHelper.PollyRetryHttpRequestAsync(...)`.

- **Queues**
  - `SendMessageToQueue(...)`: Enqueues next page for this Lambda with `PageNumber` and `CurrentServiceProviderId` (30s delay).
  - `SendMessageToGetDeviceUsageQueue(...)`: Enqueues `AltaworxTealAWSGetDeviceUsage` with `ServiceProviderId` upon completion of all device pages.

- **Staging tables**
  - `DatabaseTableNames.TealDeviceStaging`: Bulk import destination for device pages prior to merge.

- **Stored procedures**
  - `usp_Teal_Truncate_Device_And_Usage_Staging`: Clears Teal device and usage staging tables at run start (per service provider initialization).
  - `usp_Teal_Update_Device_From_Staging`: Merges staging into main device tables, considering billing period metadata for Teal (parameters include `@ServiceProviderId`, `@BillingCycleEndDay`, `@BillingCycleEndHour`, `@BillMonth`, `@BillYear`, `@NextBillCycleDate`).

- **Retry logic**
  - HTTP/API: 3 attempts, exponential backoff 3s/9s/27s, plus fallback.
  - SQL: Transient retry up to 3 attempts (`RetryPolicyHelper.SQL_TRANSIENT_RETRY_MAX_COUNT`).

### 5) Business Rules and Guardrails

- `TEAL_SYNC_FAIL_ACCEPTABLE_LIMIT = 5`: If total API page failures reach 5 in one pass, the Lambda stops paging and treats it as last page, proceeding to merge and usage to avoid indefinite retries.
- Remaining time guard: If remaining time drops below `CommonConstants.REMAINING_TIME_CUT_OFF`, the Lambda breaks out and re‑enqueues itself to continue on a fresh invocation.
- Paging completion rule: “Empty page” determines completion; there is no dependence on API‑reported total counts.
- Staging first, then merge: All inserts go to `TealDeviceStaging` via bulk copy; only after paging completes does the Lambda call `usp_Teal_Update_Device_From_Staging` to merge to primary tables.

### 6) Detailed Logging Overview

The Lambda uses `AwsFunctionBase.LogInfo(...)` to emit structured logs. Representative log checkpoints include:

- Start/finish:
  - `BEGINNING_PROCESS {recordCount}` when handling SQS records.
  - `PROCESSED_DEVICE Teal` upon completion of the handler.

- Configuration and environment:
  - Logs warnings if env vars are missing and falls back to `context.ClientContext.Environment[...]` for: `TealDevicesGetURL`, `TealDestinationQueueGetDevicesURL`, `TealDeviceUsageQueueURL`.

- Pagination and API calls:
  - `Page offset: {PageNumber}, Page size: 100` at loop start.
  - Per page: `Page {PageNumber} has {Count} devices.`
  - Errors: `ERROR_REQUESTING_DEVICES Teal` on API failures.
  - Not enough time: `NOT_ENOUGH_TIME_TO_CONTINUE_PROCESS_WITH_REMAINING_SECONDS {remaining}`.

- Database operations:
  - `SQL_BULK_COPY_START` before bulk copy into `TealDeviceStaging`.
  - Exceptions during SQL are logged with formatted messages (`EXCEPTION_WHEN_EXECUTING_SQL_COMMAND`, `EXCEPTION_WHEN_CONNECTING_DATABASE`).

- Queueing:
  - `SENDING_MESSAGE_TO_URL {QueueUrl}` and `SEND_MESSAGE_REQUEST_READY` before sending SQS messages.
  - Logs message body, queue URL, and HTTP status/result.

### 7) API Inputs/Outputs and Parameters (Teal)

- Base URLs from environment:
  - `TealDevicesGetURL`: Base for device list requests.

- Request pattern for device listing:
  - Inputs: `offset = PageNumber`, `limit = 100`.
  - Flow: Submit async list request → receive `operationResultLink` → fetch operation result → read `entries`.
  - Completion: `entries == null || entries.Count == 0` (and not the first page) marks last page.

### 8) Data Model Inserted into Staging

Columns added into `TealDeviceStaging` from the API’s device entries (subset listed in code):

- Identification: `ICCID`, `IMSI`, `IMEI` (set from `IMSI` in code), `MSISDN`, `EID`
- Status and plan: `DeviceStatus`, `PlanId`, `PlanUuid`, `PlanName`, `PlanVolumeUnit`, `PlanPrice`, `PlanChangeStatus`
- Client info: `ClientId`, `ClientName`, `ClientTitle`, `ClientUuid`
- Misc: `Sku`, `BatchNumber`, `DeviceGroupName`, `ClientChangeTimestamp`, `ProfileChangeStatus`, `FlowType`
- Context: `ServiceProviderId`, `CreatedDate`

### 9) SQS Message Contracts

- Continuation messages to self (devices queue `TealDestinationQueueGetDevicesURL`):
  - Attributes: `PageNumber` (string), `CurrentServiceProviderId` (string)
  - Delay: `30` seconds
  - Body: Informational string including page and provider values

- Hand‑off to usage (`TealDeviceUsageQueueURL`):

  - Attributes: `ServiceProviderId` (string)
  - Body: Informational string indicating start of usage sync

### 10) Stored Procedures – Purpose and Flow

- `usp_Teal_Truncate_Device_And_Usage_Staging`:
  - Purpose: Clear Teal device and usage staging tables before a fresh run per service provider.
  - Called under SQL retry policy to withstand transient failures.

- `usp_Teal_Update_Device_From_Staging`:
  - Purpose: Merge new device data from staging into the main device tables and compute/set the correct billing period fields per service provider.
  - Parameters set in code: `@ServiceProviderId`, `@BillingCycleEndDay`, `@BillingCycleEndHour`, `@BillMonth`, `@BillYear`, `@NextBillCycleDate`.
  - Wrapped by SQL retry policy.

### 11) End‑to‑End Control Flow Summary

1) Entry (EventBridge or SQS)
2) If initializing provider: resolve next provider → truncate staging (SQL retry)
3) For page `N`:
   - Call Teal list (retry) → Get operation result (retry)
   - If entries present: accumulate rows → bulk copy to `TealDeviceStaging` → increment `PageNumber`
   - If time low: break
   - If API fails repeatedly: stop paging
4) If not last page: enqueue SQS to continue with updated `PageNumber`
5) If last page (or forced stop): merge via `usp_Teal_Update_Device_From_Staging` and enqueue device usage; attempt next service provider by enqueuing page 0 for it

### 12) Items Not Applicable to Teal (but present in Telegence flows)

- BAN/FAN status retrieval and storage (`BillingAccountNumberStatusStaging`), subscriber‑level validation, and missing devices backfill logic are not part of `AltaworxTealAWSGetDevices`. Those concepts belong to the Telegence integration.

---

## Quick Reference Index

- **Environment variables**
  - `TealDevicesGetURL`, `TealDestinationQueueGetDevicesURL`, `TealDeviceUsageQueueURL`

- **Key constants**
  - `TealHelper.CommonConfig.PAGE_SIZE = 100`
  - `TealHelper.CommonConfig.RETRY_NUMBER = 3`
  - `DELAY_SQS_MESSAGE_IN_SECONDS = 30`
  - `TEAL_SYNC_FAIL_ACCEPTABLE_LIMIT = 5`

- **Queues**
  - Self‑pagination queue: `TealDestinationQueueGetDevicesURL`
  - Usage hand‑off queue: `TealDeviceUsageQueueURL`

- **Tables and procedures**
  - Staging: `DatabaseTableNames.TealDeviceStaging`
  - Truncate SP: `usp_Teal_Truncate_Device_And_Usage_Staging`
  - Merge SP: `usp_Teal_Update_Device_From_Staging`

