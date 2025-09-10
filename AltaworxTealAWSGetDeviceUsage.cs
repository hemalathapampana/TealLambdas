using System.Data;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Repositories.Teal;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS.Model;
using Amazon.SQS;
using Amop.Core.Constants;
using Amop.Core.Models.Teal;
using Amop.Core.Services.Base64Service;
using Amop.Core.Services.Http;
using Amop.Core.Services.Teal;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using Amop.Core.Models;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using AMOPSQLConstants = Amop.Core.Constants.SQLConstant;
using Altaworx.AWS.Core.Helpers;
using Amop.Core.Helpers.Teal;
using Altaworx.AWS.Core.Services.Http;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxTealAWSGetDeviceUsage
{
    public class Function : AwsFunctionBase
    {
        private string _tealDeviceDataUsageURL = Environment.GetEnvironmentVariable(TealHelper.CommonString.TEAL_DEVICES_DATA_USAGE_URL);
        private string _tealDeviceDataUsageQueueURL = Environment.GetEnvironmentVariable(TealHelper.CommonString.TEAL_DEVICE_USAGE_QUEUE_URL);
        private string _cleanUpQueueURL = Environment.GetEnvironmentVariable(TealHelper.CommonString.CLEAN_UP_QUEUE_URL);
        private TealRepository _tealRepository = null;

        /// <summary>
        /// In this lambda we will implement for two main steps:
        /// 1. The lambda will be trigged by SQS to call Teal API to get all devices usage. Then sync new data into the staging table
        /// 2. Retrigger this lambda to check if there still have more devices usage that need to be synced or not.
        /// 2.1 If Yes then back to step 1
        /// 2.2 If No then call the lambda JasperGetDeviceCleanup to sync data information and data usage for Teal Devices
        /// </summary>
        /// <param name="sqsEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext lambdaContext = null;
            try
            {
                lambdaContext = BaseFunctionHandler(context);
                _tealRepository = new TealRepository(lambdaContext);

                if (string.IsNullOrWhiteSpace(_tealDeviceDataUsageURL))
                {
                    _tealDeviceDataUsageURL = context.ClientContext.Environment[TealHelper.CommonString.TEAL_DEVICES_DATA_USAGE_URL];
                    LogInfo(lambdaContext, LogTypeConstant.Exception, string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_DEFINED, TealHelper.CommonString.TEAL_DEVICES_DATA_USAGE_URL));
                }

                if (string.IsNullOrWhiteSpace(_tealDeviceDataUsageQueueURL))
                {
                    _tealDeviceDataUsageQueueURL = context.ClientContext.Environment[TealHelper.CommonString.TEAL_DEVICE_USAGE_QUEUE_URL];
                    LogInfo(lambdaContext, LogTypeConstant.Exception, string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_DEFINED, TealHelper.CommonString.TEAL_DEVICE_USAGE_QUEUE_URL));
                }

                if (string.IsNullOrWhiteSpace(_cleanUpQueueURL))
                {
                    _cleanUpQueueURL = context.ClientContext.Environment[TealHelper.CommonString.CLEAN_UP_QUEUE_URL];
                    LogInfo(lambdaContext, LogTypeConstant.Exception, string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_DEFINED, TealHelper.CommonString.CLEAN_UP_QUEUE_URL));
                }

                if (sqsEvent == null || sqsEvent.Records == null || sqsEvent.Records.Count == 0)
                {
                    LogInfo(lambdaContext, LogTypeConstant.Exception, LogCommonStrings.LAMBDA_CANNOT_BE_RUN_FROM_SCHEDULED_EVENT);
                    return;
                }

                foreach (var record in sqsEvent.Records)
                {
                    LogInfo(lambdaContext, LogTypeConstant.Info, $"MessageId: {record.MessageId}");
                    var sqsValues = new SqsValues(lambdaContext, record);
                    await ProcessGetDeviceUsage(lambdaContext, sqsValues);
                }

                LogInfo(lambdaContext, LogTypeConstant.Info, string.Format(LogCommonStrings.PROCESSED_DEVICE, CommonConstants.TEAL_CARRIER_NAME));
            }
            catch (Exception ex)
            {
                LogInfo(lambdaContext, LogTypeConstant.Exception, ex.Message + " " + ex.StackTrace);
            }

            CleanUp(lambdaContext);
        }

        private async Task ProcessGetDeviceUsage(KeySysLambdaContext context, SqsValues sqsValues)
        {
            LogInfo(context, LogTypeConstant.Sub, "");

            var tealAuthentication = _tealRepository.GetTealAuthenticationInformation(sqsValues.ServiceProviderId);

            if (tealAuthentication == null)
            {
                throw new Exception(string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, CommonConstants.TEAL_CARRIER_NAME));
            }

            var base64Service = new Base64Service();
            var tealService = new TealAPIService(tealAuthentication, base64Service, new TealHttpSingleton(), new HttpRequestFactory());
            var billingPeriod = GetBillingPeriod(context, sqsValues.ServiceProviderId, DateTime.UtcNow, tealAuthentication.BillPeriodEndDay, tealAuthentication.BillPeriodEndHour);
            bool isLastPage = false;

            while (!isLastPage)
            {
                if (context.Context.RemainingTime < TimeSpan.FromSeconds(CommonConstants.REMAINING_TIME_CUT_OFF))
                {
                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.NOT_ENOUGH_TIME_TO_CONTINUE_PROCESS_WITH_REMAINING_SECONDS, context.Context.RemainingTime));
                    break;
                }

                var billingPeriodStart = new DateTime(billingPeriod.BillingPeriodStart.Year, billingPeriod.BillingPeriodStart.Month, billingPeriod.BillingPeriodStart.Day);
                var deviceDataUsage = await ProcessGetDeviceDataUsage(context, sqsValues, tealService, _tealDeviceDataUsageURL, billingPeriodStart, billingPeriod.BillingPeriodEnd);

                if (deviceDataUsage.Count > 0)
                {
                    CopyDataToTable(context, deviceDataUsage, sqsValues, DatabaseTableNames.TealDeviceUsageStaging, DateTime.Now);
                    sqsValues.PageNumber = sqsValues.PageNumber + 1;
                }
                else
                {
                    if (deviceDataUsage.Count <= 0 && sqsValues.PageNumber == 0)
                    {
                        LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.SERVICE_PROVIDER_WITHOUT_DEVICE_USAGE, sqsValues.ServiceProviderId));
                    }
                    isLastPage = true;
                    break;
                }

                // Get device daily usage
                DateTime now = DateTime.Now;
                DateTime todayMidNight = new DateTime(now.Year, now.Month, now.Day, 0, 0, 0);
                DateTime yesterdayMidNight = todayMidNight.AddDays(-1);
                var deviceDailyUsage = await ProcessGetDeviceDataUsage(context, sqsValues, tealService, _tealDeviceDataUsageURL, yesterdayMidNight, todayMidNight);

                if (deviceDailyUsage.Count > 0)
                {
                    CopyDataToTable(context, deviceDataUsage, sqsValues, DatabaseTableNames.TealDeviceUsageDailyStaging, yesterdayMidNight);
                }
            }

            if (isLastPage)
            {
                ProcessFinalProcessGetDeviceUsage(context, sqsValues, tealAuthentication);
            }
            else
            {
                SendMessageToGetDeviceUsageQueue(context, _tealDeviceDataUsageQueueURL, sqsValues);
            }
        }

        private void CopyDataToTable(KeySysLambdaContext context, List<TealDeviceUsage> tealDeviceUsages, SqsValues sqsValues, string tableName, DateTime usageDate)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({sqsValues.PageNumber}, {sqsValues.ServiceProviderId}, {tableName})");

            var tealDeviceUsageGroup = tealDeviceUsages
                .GroupBy(x => x.EID,
                (key, group) => new TealDeviceTotalUsage() { EID = key, Usage = group.Sum(x => x.Usage), DeviceUsage = group.FirstOrDefault() })
                .ToList();
            var tealDeviceDataUsageTable = BuildTealDeviceDataUsageTable();

            foreach (var tealDevice in tealDeviceUsageGroup)
            {
                var tealUsageDataRow = AddToDataRow(sqsValues, tealDeviceDataUsageTable, tealDevice, usageDate);
                tealDeviceDataUsageTable.Rows.Add(tealUsageDataRow);
            }

            LogInfo(context, LogTypeConstant.Info, $"Page {sqsValues.PageNumber} has {tealDeviceUsages.Count} devices.");
            LogInfo(context, LogTypeConstant.Status, LogCommonStrings.SQL_BULK_COPY_START);
            SqlBulkCopy(context, context.CentralDbConnectionString, tealDeviceDataUsageTable, tableName);
        }

        private async Task<List<TealDeviceUsage>> ProcessGetDeviceDataUsage(KeySysLambdaContext context, SqsValues sqsValues, TealAPIService tealAPIService, string deviceUsagePath, DateTime startDate, DateTime endDate)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({sqsValues.PageNumber}, {sqsValues.ServiceProviderId})");

            var tealDeviceUsageResult = await TealHelper.BuildRetryPolicy(string.Empty, TealHelper.CommonString.TEAL_GET_DEVICE_USAGE, context.logger).ExecuteAsync(async () => await tealAPIService.RequestTealDeviceUsageAsync(deviceUsagePath, sqsValues.PageNumber, TealHelper.CommonString.TEAL_DATA_TYPE_DAILY, startDate, endDate, TealHelper.CommonConfig.PAGE_SIZE, context.logger));
            var tealDeviceUsages = new List<TealDeviceUsage>();

            if (tealDeviceUsageResult.HasErrors)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.ERROR_REQUESTING_DEVICE_USAGE, CommonConstants.TEAL_CARRIER_NAME));
                return tealDeviceUsages;
            }

            var tealDeviceUsageApiResponse = JsonConvert.DeserializeObject<TealAPIResponse>(tealDeviceUsageResult.ResponseObject);
            if (tealDeviceUsageApiResponse.Success)
            {
                var operationResultLink = tealDeviceUsageApiResponse.Link.OperationResultLink.Href;
                operationResultLink = operationResultLink.Replace(TealHelper.CommonString.HTTP, TealHelper.CommonString.HTTPS);
                // Call the Teal Optimization Result API
                var tealOperationResult = await TealHelper.BuildRetryPolicy(operationResultLink, TealHelper.CommonString.OPERATION_RESULT, context.logger).ExecuteAsync(async () => await tealAPIService.GetTealOperationResultAsync(operationResultLink, context.logger));

                if (tealOperationResult.HasErrors)
                {
                    LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.ERROR_REQUESTING_DEVICE_USAGE, CommonConstants.TEAL_CARRIER_NAME));
                    return tealDeviceUsages;
                }
                var tealDeviceResponseRootObject = JsonConvert.DeserializeObject<TealDeviceUsageResponseRootObject>(tealOperationResult.ResponseObject);
                tealDeviceUsages = tealDeviceResponseRootObject.Entries;
            }
            else
            {
                LogInfo(context, LogTypeConstant.Exception, $"{tealDeviceUsageApiResponse.Message}");
            }

            return tealDeviceUsages;
        }

        private DataTable BuildTealDeviceDataUsageTable()
        {
            DataTable tealDeviceUsageTable = new DataTable();
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.Id);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.EID);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.OperationalImsi);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.Type);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.Period);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.Usage);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.MccMnc);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.Pmn);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.ServiceProviderId);
            tealDeviceUsageTable.Columns.Add(CommonColumnNames.CreatedDate);

            return tealDeviceUsageTable;
        }

        private DataRow AddToDataRow(SqsValues sqsValues, DataTable table, TealDeviceTotalUsage deviceUsageTotal, DateTime createdDate)
        {
            var tealDeviceUsageRow = table.NewRow();
            var device = deviceUsageTotal.DeviceUsage;

            tealDeviceUsageRow[CommonColumnNames.EID] = device.EID;
            tealDeviceUsageRow[CommonColumnNames.OperationalImsi] = device.OperationalImsi;
            tealDeviceUsageRow[CommonColumnNames.Type] = device.Type;
            tealDeviceUsageRow[CommonColumnNames.Period] = device.Period;
            tealDeviceUsageRow[CommonColumnNames.Usage] = deviceUsageTotal.Usage;
            tealDeviceUsageRow[CommonColumnNames.MccMnc] = device.MccMnc;
            tealDeviceUsageRow[CommonColumnNames.Pmn] = device.PMN;
            tealDeviceUsageRow[CommonColumnNames.ServiceProviderId] = sqsValues.ServiceProviderId;
            tealDeviceUsageRow[CommonColumnNames.CreatedDate] = createdDate;

            return tealDeviceUsageRow;
        }

        private void ProcessFinalProcessGetDeviceUsage(KeySysLambdaContext context, SqsValues sqsValues, TealAuthentication tealAuthentication)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({sqsValues.PageNumber}, {sqsValues.ServiceProviderId})");

            UpdateTealDeviceUsageWithPolicy(context, sqsValues.ServiceProviderId, context.CentralDbConnectionString, tealAuthentication, context.logger);
            SendMessageToJasperDeviceCleanUpQueue(context, _cleanUpQueueURL, sqsValues.ServiceProviderId);
        }

        private void UpdateTealDeviceUsageWithPolicy(KeySysLambdaContext context, int serviceProviderId, string connectionString, TealAuthentication tealAuthentication, IKeysysLogger logger)
        {
            logger.LogInfo(LogTypeConstant.Sub, $"{serviceProviderId}");
            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages);
            sqlTransientRetryPolicy.Execute(() => UpdateTealDeviceUsage(context, serviceProviderId, connectionString, tealAuthentication, logger));
        }

        private void UpdateTealDeviceUsage(KeySysLambdaContext context, int serviceProviderId, string connectionString, TealAuthentication tealAuthentication, IKeysysLogger logger)
        {
            logger.LogInfo(LogTypeConstant.Sub, $"({serviceProviderId}, {tealAuthentication.BillPeriodEndDay}, {tealAuthentication.BillPeriodEndHour})");

            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    using (var command = new SqlCommand(AMOPSQLConstants.StoredProcedureName.usp_Teal_Update_Device_Usage, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        connection.Open();
                        command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                        command.CommandTimeout = AMOPSQLConstants.TimeoutSeconds;

                        var affectedRows = command.ExecuteNonQuery();
                        if (affectedRows > 0)
                        {
                            LogInfo(context, LogTypeConstant.Info, LogCommonStrings.MERGED_DEVICE_USAGE_SUCCESSFULLY);
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, ex.Message);
            }
        }

        private void SendMessageToGetDeviceUsageQueue(KeySysLambdaContext context, string deviceUsageQueueURL, SqsValues sqsValues)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({deviceUsageQueueURL}, {sqsValues.ServiceProviderId}, {sqsValues.PageNumber})");

            if (string.IsNullOrWhiteSpace(deviceUsageQueueURL))
            {
                // to be able to skip enqueuing messages in a test
                return;
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = string.Format(LogCommonStrings.GETTING_TEAL_DEVICES_WITH_PAGE_NUMBER_AND_SERVICE_PROVIDER, sqsValues.PageNumber, sqsValues.ServiceProviderId);
                LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.SENDING_MESSAGE_TO_URL, deviceUsageQueueURL));

                var request = new SendMessageRequest
                {
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "ServiceProviderId", new MessageAttributeValue
                            {
                                DataType = "String", StringValue = sqsValues.ServiceProviderId.ToString()
                            }
                        },
                        {
                            "PageNumber", new MessageAttributeValue
                            {
                                DataType = "String", StringValue = sqsValues.PageNumber.ToString()
                            }
                        }
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = deviceUsageQueueURL
                };

                LogInfo(context, LogTypeConstant.Info, LogCommonStrings.SEND_MESSAGE_REQUEST_READY);
                LogInfo(context, LogTypeConstant.Info, $"MessageBody: {request.MessageBody}");
                LogInfo(context, LogTypeConstant.Info, $"QueueURL: {request.QueueUrl}");

                var response = client.SendMessageAsync(request);
                response.Wait();
                LogInfo(context, LogTypeConstant.Info, response.Status);
            }
        }

        private void SendMessageToJasperDeviceCleanUpQueue(KeySysLambdaContext context, string cleanUpQueueURL, int serviceProviderId)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({cleanUpQueueURL}, {serviceProviderId})");

            if (string.IsNullOrWhiteSpace(cleanUpQueueURL))
            {
                // to be able to skip enqueuing messages in a test
                return;
            }

            var retryCount = 0;
            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.SENDING_MESSAGE_TO_URL, cleanUpQueueURL));

                var request = new SendMessageRequest
                {
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { "RetryCount", new MessageAttributeValue { DataType = "String", StringValue = retryCount.ToString()}},
                        { "IntegrationType", new MessageAttributeValue { DataType = "String", StringValue = ((int)IntegrationType.Teal).ToString() }},
                        { "ServiceProviderId", new MessageAttributeValue { DataType = "String", StringValue = serviceProviderId.ToString() }}
                    },
                    MessageBody = LogCommonStrings.END_PROCESS_GET_DEVICES,
                    QueueUrl = cleanUpQueueURL
                };

                LogInfo(context, LogTypeConstant.Info, LogCommonStrings.SEND_MESSAGE_REQUEST_READY);
                LogInfo(context, LogTypeConstant.Info, $"MessageBody: {request.MessageBody}");
                LogInfo(context, LogTypeConstant.Info, $"QueueURL: {request.QueueUrl}");

                var response = client.SendMessageAsync(request);
                response.Wait();
                LogInfo(context, LogTypeConstant.Info, response.Status);
            }
        }

        public static BillingPeriod GetBillingPeriod(KeySysLambdaContext context, int serviceProviderId, DateTime currentDateTime, int billPeriodEndDay, int billPeriodEndHour)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({serviceProviderId})");

            var billingPeriodYear = currentDateTime.Year;
            var billingPeriodMonth = currentDateTime.Month;

            var billingPeriod = BillingPeriodHelper.GetBillingPeriodForServiceProvider(context.CentralDbConnectionString, serviceProviderId, billingPeriodYear, billingPeriodMonth, null, billPeriodEndDay, billPeriodEndHour);

            // if billing end day in service provider < current day, + 1 month
            if (billingPeriod.BillingPeriodEndDay < currentDateTime.Day)
            {
                var endDate = billingPeriod.BillingPeriodEnd.AddMonths(1);
                billingPeriodYear = endDate.Year;
                billingPeriodMonth = endDate.Month;
                billingPeriod = BillingPeriodHelper.GetBillingPeriodForServiceProvider(context.CentralDbConnectionString, serviceProviderId, billingPeriodYear, billingPeriodMonth, null, billPeriodEndDay, billPeriodEndHour);
            }

            LogInfo(context, LogTypeConstant.Info, $"{nameof(billingPeriod.BillingPeriodEnd)}={billingPeriod.BillingPeriodEnd}");
            return billingPeriod;
        }
    }
}
