using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Messaging;
using Orleans.Redis.Clustering.Messaging;
using Orleans.Redis.Clustering.Options;
using StackExchange.Redis;
using TestExtensions;
using UnitTests.MembershipTests;
using Xunit;

namespace Orleans.Redis.FunctionalTests.Clustering
{
    /// <summary>
    /// Tests for operation of Orleans Membership Table using Redis - Requires access to external Redis cluster
    /// </summary>
    public class RedisMembershipTableTest : MembershipTableTestsBase
    {
        public RedisMembershipTableTest() : base(CreateFilters())
        {
        }

        private static LoggerFilterOptions CreateFilters()
        {
            var filters = new LoggerFilterOptions();
            filters.AddFilter("RedisMembershipTable", LogLevel.Trace);
            return filters;
        }

        protected override IMembershipTable CreateMembershipTable(ILogger logger)
        {
            var options = new RedisClusteringSiloOptions();
            return new RedisMembershipTable(clusterOptions, Options.Create(options), connectionMultiplexer,
                loggerFactory.CreateLogger<RedisMembershipTable>());
        }

        protected override IGatewayListProvider CreateGatewayListProvider(ILogger logger)
        {
            var options = new RedisClusteringClientOptions();
            return new RedisGatewayListProvider(clusterOptions, Options.Create(options), connectionMultiplexer,
                gatewayOptions, loggerFactory.CreateLogger<RedisGatewayListProvider>());
        }

        [Fact]
        public async Task MembershipTable_Redis_GetGateways()
        {
            await MembershipTable_GetGateways();
        }

        [Fact]
        public async Task MembershipTable_Redis_ReadAll_EmptyTable()
        {
            await MembershipTable_ReadAll_EmptyTable();
        }

        [Fact]
        public async Task MembershipTable_Redis_InsertRow()
        {
            await MembershipTable_InsertRow();
        }

        [Fact]
        public async Task MembershipTable_Redis_ReadRow_Insert_Read()
        {
            await MembershipTable_ReadRow_Insert_Read();
        }

        [Fact]
        public async Task MembershipTable_Redis_ReadAll_Insert_ReadAll()
        {
            await MembershipTable_ReadAll_Insert_ReadAll();
        }

        [Fact]
        public async Task MembershipTable_Redis_UpdateRow()
        {
            await MembershipTable_UpdateRow();
        }

        [Fact]
        public async Task MembershipTable_Redis_UpdateRowInParallel()
        {
            await MembershipTable_UpdateRowInParallel();
        }

        [Fact]
        public async Task MembershipTable_Redis_UpdateIAmAlive()
        {
            await MembershipTable_UpdateIAmAlive();
        }
    }
}