### AltaworxTealAWSGetDeviceUsage Lambda Flow Documentation

Overview

The AltaworxTealAWSGetDeviceUsage Lambda function processes SQS messages to retrieve device usage data from the Teal API and store it in the database. The function operates in two modes: initialization and processing.

---

1. HIGH-LEVEL FLOW (Sequential Function Flow)

Main Entry Point

1. `FunctionHandler` (Entry point)

- Receives SQS event and Lambda context
- Initializes base function handler
- Iterates through SQS records

Initialization Flow (`InitializeProcessing = true`)

2. `StartDailyUsageProcessing`

- `CallDailyGetUsageSP`
- `GetGroupCount`
- `SendProcessMessagesToQueue`
- `SendNotificationMessageToQueue`

Processing Flow (`InitializeProcessing = false`)

3. `ProcessUsageList`

- `GetTealAuthenticationInformation` (from `TealCommon`)
- `GetAccessToken` (from `TealDeviceDetailService`)
- `GetSessionToken` (from `TealDeviceDetailService`)
- `GetTealUsageAsync` (from `TealDeviceDetailService`)
- `GetTealDailyUsageAsync` (from `TealDeviceDetailService`)
- `GetBillingPeriod` (from `TealCommon`)
- `SqlBulkCopy` (from `AwsFunctionBase`)
- `UpdateDeviceUsage`
- `RemoveProcessedICCID`
- `SendProcessMessageToQueue`

Utility Functions

4. `GetMessageQueueValues`

5. `InitDeviceUsageDataTable`

6. `AddToDataRow`

7. `GetHttpRetryPolicy`

---

2. LOW-LEVEL FLOW (Detailed Method Explanations)

`FunctionHandler` (Main Entry Point)

Input: `SQSEvent sqsEvent`, `ILambdaContext context`  
Purpose: Main Lambda entry point that processes SQS messages

What happens:

- Initializes `KeySysLambdaContext` by calling `BaseFunctionHandler()` from `AwsFunctionBase`
- Retrieves environment variables (`TealDeviceUsageGetURL`, `TealDeviceNotificationQueueURL`, etc.)
- Sets security protocols for HTTPS communication
- Validates that the function was triggered by SQS (not CloudWatch)
- Iterates through each SQS record and processes them
- For each record:
  - Logs message details (`MessageId`, `EventSource`, `Body`)
  - Calls `GetMessageQueueValues()` to parse SQS message attributes
  - Checks `InitializeProcessing` flag to determine processing mode
  - Either calls `StartDailyUsageProcessing()` or `ProcessUsageList()`
- Handles exceptions and calls `CleanUp()` at the end

---

`StartDailyUsageProcessing` (Initialization Mode)

Input: `KeySysLambdaContext context`, `int serviceProviderId`  
Purpose: Initializes the daily usage processing workflow

What happens:

- `CallDailyGetUsageSP()`: Executes stored procedure `usp_Teal_Devices_GetUsageFilter`
  - Populates `TealDeviceUsageICCIDsToProcess` table with devices that need usage data retrieval
  - Groups devices into batches for processing
- `GetGroupCount()`: Queries database to get maximum group number
  - Executes: `SELECT MAX(GroupNumber) FROM TealDeviceUsageICCIDsToProcess WHERE ServiceProviderId = @ServiceProviderId`
  - Returns the total number of groups created for processing
- `SendProcessMessagesToQueue()`: Creates SQS messages for each group
  - Loops through all groups (0 to `groupCount`)
  - Calls `SendProcessMessageToQueue()` for each group with 30-second delay
  - Each message contains `GroupNumber` and `ServiceProviderId` attributes
- `SendNotificationMessageToQueue()`: Sends notification message
  - Creates SQS message for device notification queue
  - Sets 5-minute delay for notification processing
  - Includes `RetryCount`, `IntegrationType`, and `ServiceProviderId` attributes

---

`ProcessUsageList` (Processing Mode)

Input: `KeySysLambdaContext context`, `GetDeviceUsageSqsValues sqsValues`  
Purpose: Processes batches of devices to retrieve usage data from Teal API

What happens:

1. Data Retrieval Setup

- Queries database to get batch of ICCIDs to process:

```
SELECT TOP {BatchSize}
  [ICCID], [ServiceProviderId], [BillingCycleEndDate]
FROM [TealDeviceUsageICCIDsToProcess]
WHERE ServiceProviderId = @ServiceProviderId
  AND GroupNumber = {GroupNumber}
ORDER BY [ICCID]
```

- Creates `List<TealICCIDToProcess>` with retrieved devices
- Initializes `DataTables` for usage data storage (`InitDeviceUsageDataTable()`)

2. Authentication Setup

- `GetTealAuthenticationInformation()`: Retrieves Teal credentials from database
  - Executes stored procedure `usp_Teal_Get_AuthenticationByProviderId`
  - Returns `TealAuthentication` object with `BaseUrl`, `ClientId`, `ClientSecret`, etc.
- Creates `TealDeviceDetailService` instance with authentication and retry policy
- `GetAccessToken()`: Obtains OAuth2 access token from Teal
  - Makes POST request to Teal token endpoint
  - Uses Basic authentication with Base64-encoded `ClientId:ClientSecret`
  - Returns `TealTokenResponse` with access token
- `GetSessionToken()`: Obtains session token for API calls
  - Makes POST request with username/password and access token
  - Returns `TealLoginResponse` with session token

3. Device Processing Loop

For each ICCID in the batch:

- Checks remaining Lambda execution time (must be > 60 seconds)
- `GetTealUsageAsync()`: Retrieves billing cycle usage data
  - Calculates date range from previous month to billing cycle end date
  - Makes POST request to Teal usage API with ICCID and date range
  - Returns `DeviceUsageResponseRootObject` with usage history
- `GetTealDailyUsageAsync()`: Retrieves previous day’s usage data
  - Uses yesterday’s date as both earliest and latest date
  - Makes similar POST request for daily usage
  - Returns daily usage data in same format
- `GetBillingPeriod()`: Determines billing period for the device
  - Gets service provider billing settings (end day/hour)
  - Calculates current billing period based on current date
  - Returns `BillingPeriod` object with billing cycle information

4. Data Processing and Storage

- Processes usage response data:
  - Extracts service plan, bytes used, SMS used, session count
  - Calculates cumulative usage from usage history
  - Handles cases where no usage data is available (sets to 0)
- `AddToDataRow()`: Creates `DataRow` with processed usage data
  - Maps device usage data to database columns
  - Includes billing period information (year/month)
  - Sets metadata (`CreatedBy`, `CreatedDate`, `Processed` flag)
- `RemoveProcessedICCID()`: Removes processed ICCID from queue table
  - Executes: `DELETE FROM TealDeviceUsageICCIDsToProcess WHERE ICCID = @iccid`

5. Batch Storage

- `SqlBulkCopy()`: Bulk inserts usage data to database
  - Inserts billing cycle usage to `TealDeviceUsageStaging` table
  - Inserts daily usage to `TealDeviceDailyUsage` table
  - Uses `SqlBulkCopy` for efficient batch insertion
- `SendProcessMessageToQueue()`: Sends message for next batch processing
  - Creates SQS message with same `GroupNumber` and `ServiceProviderId`
  - Sets 5-second delay for immediate processing of next batch

6. Finalization

If no more devices to process:

- `UpdateDeviceUsage()`: Moves data from staging to final tables
  - Executes stored procedure `usp_Teal_Update_DeviceUsage_FromStaging`
  - Updates main device usage tables with staged data
  - Applies business logic for usage calculations

---

Supporting Functions

- `GetMessageQueueValues`
  - Parses SQS message attributes (`InitializeProcessing`, `GroupNumber`, `ServiceProviderId`)
  - Returns `GetDeviceUsageSqsValues` object with parsed values
- `InitDeviceUsageDataTable`
  - Creates `DataTable` structure matching database schema
  - Includes columns for ICCID, usage data, billing period, and metadata
  - Optional `ServiceProviderId` column for daily usage table
- `AddToDataRow`
  - Maps `TealDeviceUsage` object to `DataTable` row
  - Handles null values and data type conversions
  - Sets default values for required fields
- `GetHttpRetryPolicy`
  - Creates retry policy for HTTP requests (e.g., Polly)
  - Configured for 3 retry attempts with exponential backoff
- `UpdateDeviceUsage`
  - Executes stored procedure to move data from staging to production tables
  - Applies business rules and calculations
  - Updates device usage records with latest data
- `RemoveProcessedICCID`
  - Removes individual ICCID from processing queue
  - Ensures processed devices aren’t reprocessed
- `SendProcessMessageToQueue` & `SendProcessMessagesToQueue`
  - Creates SQS messages for continued processing
  - Manages batch processing workflow
  - Handles delays and message attributes
- `SendNotificationMessageToQueue`
  - Sends completion notification to downstream systems
  - Triggers additional processing or notifications
  - Includes integration type and service provider information

---

Key Dependencies and Integrations

- `AwsFunctionBase` (Base Class)
  - Provides common Lambda functionality (logging, database connections, AWS credentials)
  - `BaseFunctionHandler()`: Initializes Lambda context
  - `SqlBulkCopy()`: Handles bulk database operations
  - `LogInfo()`: Centralized logging functionality
  - `CleanUp()`: Resource cleanup and disposal
- `TealCommon` (Static Helper Class)
  - `GetTealAuthenticationInformation()`: Database authentication retrieval
  - `GetBillingPeriod()`: Billing period calculations
  - Provides common Teal API functionality
- `TealDeviceDetailService` (API Service Class)
  - `GetAccessToken()`: OAuth2 token management
  - `GetSessionToken()`: Session authentication
  - `GetTealUsageAsync()`: Usage data retrieval
  - `GetTealDailyUsageAsync()`: Daily usage data retrieval
  - Handles HTTP communication with Teal API
- `BillingPeriodHelper` (Utility Class)
  - `GetBillingPeriodForServiceProvider()`: Database billing period lookup
  - `GetBillingPeriodForServiceProviderByCurrentDate()`: Current period calculation
  - Handles billing cycle logic and database queries
- `ServiceProviderCommon` (Static Helper Class)
  - `GetServiceProvider()`: Service provider configuration retrieval
  - Provides service provider-specific settings and configurations

---

Data Flow Summary

1. Initialization: Lambda receives SQS message to start processing
2. Setup: Stored procedure populates processing queue with device ICCIDs
3. Batching: Devices are grouped and queued for parallel processing
4. Authentication: OAuth2 tokens obtained from Teal API
5. Processing: Each device’s usage data retrieved via API calls
6. Storage: Usage data bulk-inserted into staging and daily tables
7. Finalization: Staging data processed into production tables
8. Cleanup: Processed devices removed from queue
9. Continuation: Next batch queued for processing
10. Notification: Completion notification sent to downstream systems

---


### AltaworxTealAWSGetDevices Lambda Flow Documentation

Overview

The AltaworxTealAWSGetDevices Lambda function synchronizes device inventory from the Teal API into the database. It can initialize a device sync session and then process device pages in batches using SQS messages.

---

1. HIGH-LEVEL FLOW (Sequential Function Flow)

Main Entry Point

1. `FunctionHandler` (Entry point)

- Receives SQS event and Lambda context
- Initializes base function handler
- Iterates through SQS records

Initialization Flow (`InitializeProcessing = true`)

2. `StartDeviceSyncInitialization`

- `CallDeviceSyncSeedSP`
- `GetGroupCount`
- `SendProcessMessagesToQueue`
- `SendNotificationMessageToQueue`

Processing Flow (`InitializeProcessing = false`)

3. `ProcessDeviceListPage`

- `GetTealAuthenticationInformation` (from `TealCommon`)
- `GetAccessToken` (from `TealDeviceService`)
- `GetSessionToken` (from `TealDeviceService`)
- `GetTealDevicesAsync` (paged) (from `TealDeviceService`)
- `SqlBulkCopy` (from `AwsFunctionBase`)
- `UpsertDevices`
- `RemoveProcessedPageMarker`
- `SendProcessMessageToQueue`

Utility Functions

4. `GetMessageQueueValues`

5. `InitDeviceDataTable`

6. `AddDeviceToDataRow`

7. `GetHttpRetryPolicy`

---

2. LOW-LEVEL FLOW (Detailed Method Explanations)

`FunctionHandler` (Main Entry Point)

Input: `SQSEvent sqsEvent`, `ILambdaContext context`  
Purpose: Processes SQS messages to orchestrate device synchronization

What happens:

- Initializes `KeySysLambdaContext` via `BaseFunctionHandler()`
- Reads environment variables (`TealDevicesGetURL`, `TealDeviceNotificationQueueURL`, etc.)
- Ensures SQS trigger validity and iterates each record
- For each record:
  - Logs diagnostics
  - Parses attributes with `GetMessageQueueValues()` including `InitializeProcessing`, `PageToken`, `GroupNumber`, `ServiceProviderId`, `RetryCount`
  - Routes to `StartDeviceSyncInitialization()` or `ProcessDeviceListPage()`
- Handles exceptions and calls `CleanUp()`

---

`StartDeviceSyncInitialization` (Initialization Mode)

Input: `KeySysLambdaContext context`, `int serviceProviderId`  
Purpose: Seeds the sync process and fans out processing across groups/pages

What happens:

- `CallDeviceSyncSeedSP()`: Executes `usp_Teal_Devices_Sync_Seed`
  - Seeds `TealDevicesToSync` with markers (e.g., page tokens or group ranges)
  - Optionally limits by `LastUpdatedSince` for delta sync
- `GetGroupCount()`: Determines number of groups to process
  - Executes: `SELECT MAX(GroupNumber) FROM TealDevicesToSync WHERE ServiceProviderId = @ServiceProviderId`
- `SendProcessMessagesToQueue()`: Enqueues one message per group
  - Attaches `GroupNumber`, `ServiceProviderId`, `RetryCount = 0`
  - Applies short delay to spread load
- `SendNotificationMessageToQueue()`: Emits a notification for downstream workflows
  - 5-minute delay, includes `IntegrationType = DeviceSync`

---

`ProcessDeviceListPage` (Processing Mode)

Input: `KeySysLambdaContext context`, `GetDevicesSqsValues sqsValues`  
Purpose: Pulls one page/batch of devices from Teal and upserts to DB

What happens:

1. Page Marker Retrieval

- Reads next page marker for this `GroupNumber`:

```
SELECT TOP 1 [PageToken], [GroupNumber], [ServiceProviderId]
FROM [TealDevicesToSync]
WHERE ServiceProviderId = @ServiceProviderId
  AND GroupNumber = {GroupNumber}
ORDER BY [CreatedDate]
```

- If none remains, finalize group and stop

2. Authentication

- `GetTealAuthenticationInformation()` from DB via `usp_Teal_Get_AuthenticationByProviderId`
- Instantiate `TealDeviceService` with retry policy
- `GetAccessToken()` then `GetSessionToken()`

3. API Fetch

- `GetTealDevicesAsync(pageToken, pageSize)`
  - Calls Teal devices endpoint with pagination
  - Returns device list and `nextPageToken`

4. Transform & Stage

- Build `DataTable` via `InitDeviceDataTable()`
- For each device, map fields using `AddDeviceToDataRow()`
- `SqlBulkCopy()` into `TealDevicesStaging`

5. Upsert & Advance

- `UpsertDevices()`: `EXEC usp_Teal_Update_Devices_FromStaging @ServiceProviderId`
- `RemoveProcessedPageMarker()`: deletes consumed marker from `TealDevicesToSync`
- If `nextPageToken` exists:
  - Insert new marker into `TealDevicesToSync` for same `GroupNumber`
  - `SendProcessMessageToQueue()` with short delay (e.g., 5 seconds)
- Else if group empty across all markers:
  - `SendNotificationMessageToQueue()` indicating group completion

---

Supporting Functions

- `GetMessageQueueValues`: Parses SQS attributes (`InitializeProcessing`, `GroupNumber`, `PageToken`, `ServiceProviderId`, `RetryCount`)
- `InitDeviceDataTable`: Shapes staging schema (identifiers, plan, status, last seen, metadata)
- `AddDeviceToDataRow`: Safe mapping, type conversions, defaults
- `GetHttpRetryPolicy`: Retry with exponential backoff and circuit-breaker for 5xx/429
- `UpsertDevices`: Moves data from staging to canonical tables with merge semantics
- `RemoveProcessedPageMarker`: Prevents reprocessing of already-read pages
- `SendProcessMessageToQueue` & `SendNotificationMessageToQueue`: Workflow fan-out and lifecycle messaging

---

Key Dependencies and Integrations

- `AwsFunctionBase`: logging, config, DB connections, bulk copy, cleanup
- `TealCommon`: auth retrieval, provider context, common helpers
- `TealDeviceService`: device list API, token/session management
- `ServiceProviderCommon`: provider configuration and limits

---

Data Flow Summary

1. Initialization: seed markers for device pages/groups
2. Authentication: obtain tokens for Teal API
3. Fetch: pull one page of devices
4. Stage: bulk insert to `TealDevicesStaging`
5. Upsert: merge into production device tables
6. Advance: enqueue next page or complete group
7. Notify: send completion messages if configured

---


### AltaworxTealAWSGetRatePlans Lambda Flow Documentation

Overview

The AltaworxTealAWSGetRatePlans Lambda function retrieves rate plan definitions from the Teal API, stages them, and upserts to production tables. It supports initialization and paginated processing via SQS.

---

1. HIGH-LEVEL FLOW (Sequential Function Flow)

Main Entry Point

1. `FunctionHandler` (Entry point)

- Receives SQS event and Lambda context
- Initializes base function handler
- Iterates through SQS records

Initialization Flow (`InitializeProcessing = true`)

2. `StartRatePlanInitialization`

- `CallRatePlanSyncSeedSP`
- `SendProcessMessagesToQueue`
- `SendNotificationMessageToQueue`

Processing Flow (`InitializeProcessing = false`)

3. `ProcessRatePlansPage`

- `GetTealAuthenticationInformation` (from `TealCommon`)
- `GetAccessToken` (from `TealRatePlanService`)
- `GetSessionToken` (from `TealRatePlanService`)
- `GetTealRatePlansAsync` (paged) (from `TealRatePlanService`)
- `SqlBulkCopy` (from `AwsFunctionBase`)
- `UpsertRatePlans`
- `AdvancePagination`
- `SendProcessMessageToQueue`

Utility Functions

4. `GetMessageQueueValues`

5. `InitRatePlanDataTable`

6. `AddRatePlanToDataRow`

7. `GetHttpRetryPolicy`

---

2. LOW-LEVEL FLOW (Detailed Method Explanations)

`FunctionHandler` (Main Entry Point)

Input: `SQSEvent sqsEvent`, `ILambdaContext context`  
Purpose: Orchestrates the rate plan sync workflow via SQS

What happens:

- Initializes `KeySysLambdaContext` via `BaseFunctionHandler()`
- Reads environment/config for rate plan endpoints and SQS queues
- Iterates SQS records, parses attributes with `GetMessageQueueValues()` (`InitializeProcessing`, `PageToken`, `ServiceProviderId`)
- Routes to `StartRatePlanInitialization()` or `ProcessRatePlansPage()`
- Ensures cleanup and error handling

---

`StartRatePlanInitialization` (Initialization Mode)

Input: `KeySysLambdaContext context`, `int serviceProviderId`  
Purpose: Seeds the page markers and begins processing

What happens:

- `CallRatePlanSyncSeedSP()`: `EXEC usp_Teal_RatePlans_Sync_Seed @ServiceProviderId`
  - Seeds `TealRatePlansToSync` (e.g., single initial page marker)
- `SendProcessMessagesToQueue()`: Sends one processing message per marker (often one)
  - Includes `ServiceProviderId` and `RetryCount = 0`
- `SendNotificationMessageToQueue()`: Optional kickoff/monitoring notification

---

`ProcessRatePlansPage` (Processing Mode)

Input: `KeySysLambdaContext context`, `GetRatePlansSqsValues sqsValues`  
Purpose: Fetches a page of rate plans and upserts them

What happens:

1. Marker Retrieval

- Fetch next marker for rate plans:

```
SELECT TOP 1 [PageToken]
FROM [TealRatePlansToSync]
WHERE ServiceProviderId = @ServiceProviderId
ORDER BY [CreatedDate]
```

2. Authentication & API

- `GetTealAuthenticationInformation()`
- `GetAccessToken()` then `GetSessionToken()` via `TealRatePlanService`
- `GetTealRatePlansAsync(pageToken, pageSize)` returns rate plans and `nextPageToken`

3. Transform & Stage

- Build `DataTable` via `InitRatePlanDataTable()`
- Map each plan with `AddRatePlanToDataRow()`
- `SqlBulkCopy()` into `TealRatePlansStaging`

4. Upsert & Advance

- `UpsertRatePlans()`: `EXEC usp_Teal_Update_RatePlans_FromStaging @ServiceProviderId`
- `AdvancePagination()`:
  - Delete consumed marker
  - If `nextPageToken` exists, insert new marker and `SendProcessMessageToQueue()`
  - Else send completion notification via `SendNotificationMessageToQueue()`

---

Supporting Functions

- `GetMessageQueueValues`: Parses `InitializeProcessing`, `PageToken`, `ServiceProviderId`, `RetryCount`
- `InitRatePlanDataTable`: Shapes staging schema (plan id, name, allowances, fees, metadata)
- `AddRatePlanToDataRow`: Mapping and normalization
- `GetHttpRetryPolicy`: Transient-fault handling for API calls
- `UpsertRatePlans`: Moves staged plans to production via stored procedure
- `AdvancePagination`: Manages page markers lifecycle

---

Key Dependencies and Integrations

- `AwsFunctionBase`: base Lambda utilities
- `TealCommon`: authentication and common helpers
- `TealRatePlanService`: rate plan API client and auth
- `ServiceProviderCommon`: provider config

---

Data Flow Summary

1. Initialization: seed first page marker for rate plans
2. Authentication: obtain tokens
3. Fetch: retrieve a page of rate plans
4. Stage: bulk-insert to `TealRatePlansStaging`
5. Upsert: merge into production tables
6. Advance: continue until no `nextPageToken`
7. Notify: signal completion to downstream systems

---

This documentation outlines high-level and low-level execution flows for the three Altaworx Teal Lambdas, referencing supporting classes where relevant without detailing separate class flows.