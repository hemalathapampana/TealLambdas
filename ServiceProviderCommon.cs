using System;
using System.Collections.Generic;
using System.Data;
using Altaworx.AWS.Core.Helpers.Constants;
using Amop.Core.Models;
using Microsoft.Data.SqlClient;

namespace Altaworx.AWS.Core
{
    public static class ServiceProviderCommon
    {
        public static int GetNextServiceProviderId(string connectionString, IntegrationType integrationType, int currentServiceProviderId)
        {
            int nextProviderId = 0;

            try
            {
                using (var Conn = new SqlConnection(connectionString))
                {
                    using (var Cmd = new SqlCommand("usp_DeviceSync_Get_NextServiceProviderIdByIntegration", Conn))
                    {
                        Cmd.CommandType = CommandType.StoredProcedure;
                        Cmd.Parameters.AddWithValue("@providerId", currentServiceProviderId);
                        Cmd.Parameters.AddWithValue("@integrationId", (int)integrationType);
                        Conn.Open();

                        var result = Cmd.ExecuteScalar();
                        if (result != null && result != DBNull.Value)
                        {
                            nextProviderId = (Int32)result;
                        }

                        Conn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return nextProviderId;
        }

        public static ServiceProvider GetServiceProvider(string connectionString, int serviceProviderId)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand("SELECT Id, Name, DisplayName, IntegrationId, TenantId, [BillPeriodEndDay], [BillPeriodEndHour], [OptimizationStartHourLocalTime], [ContinuousLastDayOptimizationStartHourLocalTime], [WriteIsEnabled], [RegisterCarrierServiceCallBack] FROM ServiceProvider WHERE id = @serviceProviderId", connection))
                {
                    command.CommandType = CommandType.Text;
                    command.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                    connection.Open();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            return new ServiceProvider
                            {
                                Id = int.Parse(reader["id"].ToString()),
                                DisplayName = reader["DisplayName"].ToString(),
                                Name = reader["Name"].ToString(),
                                IntegrationId = int.Parse(reader["IntegrationId"].ToString()),
                                TenantId = !reader.IsDBNull(reader.GetOrdinal("TenantId")) ? new int?(int.Parse(reader["TenantId"].ToString())) : null,
                                BillPeriodEndDay = !reader.IsDBNull(reader.GetOrdinal("BillPeriodEndDay")) ? new int?(int.Parse(reader["BillPeriodEndDay"].ToString())) : null,
                                BillPeriodEndHour = !reader.IsDBNull(reader.GetOrdinal("BillPeriodEndHour")) ? new int?(int.Parse(reader["BillPeriodEndHour"].ToString())) : null,
                                OptimizationStartHour = !reader.IsDBNull(reader.GetOrdinal("OptimizationStartHourLocalTime")) ? new int?(int.Parse(reader["OptimizationStartHourLocalTime"].ToString())) : null,
                                ContinuousLastDayOptimizationStartHour = !reader.IsDBNull(reader.GetOrdinal("ContinuousLastDayOptimizationStartHourLocalTime")) ? new int?(int.Parse(reader["ContinuousLastDayOptimizationStartHourLocalTime"].ToString())) : null,
                                WriteIsEnabled = reader.GetBoolean(reader.GetOrdinal("WriteIsEnabled")),
                                RegisterCarrierServiceCallBack = reader.GetBoolean(reader.GetOrdinal("RegisterCarrierServiceCallBack"))
                            };
                        }
                    }

                    connection.Close();
                }
            }

            return null;
        }
        public static List<ServiceProvider> GetServiceProviders(string connectionString)
        {
            var ListServiceProvider = new List<ServiceProvider>();
            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand("SELECT Id, Name, DisplayName, IntegrationId, TenantId, [BillPeriodEndDay], [BillPeriodEndHour], [OptimizationStartHourLocalTime], [ContinuousLastDayOptimizationStartHourLocalTime], [WriteIsEnabled] FROM ServiceProvider", connection))
                {
                    command.CommandType = CommandType.Text;
                    connection.Open();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            ListServiceProvider.Add(new ServiceProvider
                            {
                                Id = int.Parse(reader["id"].ToString()),
                                DisplayName = reader["DisplayName"].ToString(),
                                Name = reader["Name"].ToString(),
                                IntegrationId = int.Parse(reader["IntegrationId"].ToString()),
                                TenantId = !reader.IsDBNull(reader.GetOrdinal("TenantId")) ? new int?(int.Parse(reader["TenantId"].ToString())) : null,
                                BillPeriodEndDay = !reader.IsDBNull(reader.GetOrdinal("BillPeriodEndDay")) ? new int?(int.Parse(reader["BillPeriodEndDay"].ToString())) : null,
                                BillPeriodEndHour = !reader.IsDBNull(reader.GetOrdinal("BillPeriodEndHour")) ? new int?(int.Parse(reader["BillPeriodEndHour"].ToString())) : null,
                                OptimizationStartHour = !reader.IsDBNull(reader.GetOrdinal("OptimizationStartHourLocalTime")) ? new int?(int.Parse(reader["OptimizationStartHourLocalTime"].ToString())) : null,
                                ContinuousLastDayOptimizationStartHour = !reader.IsDBNull(reader.GetOrdinal("ContinuousLastDayOptimizationStartHourLocalTime")) ? new int?(int.Parse(reader["ContinuousLastDayOptimizationStartHourLocalTime"].ToString())) : null,
                                WriteIsEnabled = reader.GetBoolean(reader.GetOrdinal("WriteIsEnabled"))
                            });
                        }
                    }

                    connection.Close();
                }
            }

            return ListServiceProvider;
        }

        public static ServiceProvider GetServiceProviderByName(string connectionString, string serviceProviderName)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand("SELECT [Id], [Name], [DisplayName], [IntegrationId], [TenantId], [BillPeriodEndDay], [BillPeriodEndHour], [OptimizationStartHourLocalTime], [ContinuousLastDayOptimizationStartHourLocalTime], [WriteIsEnabled], [RegisterCarrierServiceCallBack] FROM ServiceProvider WHERE [Name] = @serviceProviderName", connection))
                {
                    command.CommandType = CommandType.Text;
                    command.Parameters.AddWithValue("@serviceProviderName", serviceProviderName);
                    command.CommandTimeout = SQLConstant.TimeoutSeconds;
                    connection.Open();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            return ServiceProviderFromReader(reader);
                        }
                    }

                    connection.Close();
                }
            }

            return null;
        }

        private static ServiceProvider ServiceProviderFromReader(SqlDataReader reader)
        {
            return new ServiceProvider
            {
                Id = int.Parse(reader["id"].ToString()),
                DisplayName = reader["DisplayName"].ToString(),
                Name = reader["Name"].ToString(),
                IntegrationId = int.Parse(reader["IntegrationId"].ToString()),
                TenantId = !reader.IsDBNull(reader.GetOrdinal("TenantId")) ? new int?(int.Parse(reader["TenantId"].ToString())) : null,
                BillPeriodEndDay = !reader.IsDBNull(reader.GetOrdinal("BillPeriodEndDay")) ? new int?(int.Parse(reader["BillPeriodEndDay"].ToString())) : null,
                BillPeriodEndHour = !reader.IsDBNull(reader.GetOrdinal("BillPeriodEndHour")) ? new int?(int.Parse(reader["BillPeriodEndHour"].ToString())) : null,
                OptimizationStartHour = !reader.IsDBNull(reader.GetOrdinal("OptimizationStartHourLocalTime")) ? new int?(int.Parse(reader["OptimizationStartHourLocalTime"].ToString())) : null,
                ContinuousLastDayOptimizationStartHour = !reader.IsDBNull(reader.GetOrdinal("ContinuousLastDayOptimizationStartHourLocalTime")) ? new int?(int.Parse(reader["ContinuousLastDayOptimizationStartHourLocalTime"].ToString())) : null,
                WriteIsEnabled = reader.GetBoolean(reader.GetOrdinal("WriteIsEnabled")),
                RegisterCarrierServiceCallBack = reader.GetBoolean(reader.GetOrdinal("RegisterCarrierServiceCallBack"))
            };
        }
    }
}
