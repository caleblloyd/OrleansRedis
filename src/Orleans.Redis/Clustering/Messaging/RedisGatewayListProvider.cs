using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Redis.Clustering.Options;
using Orleans.Runtime;
using Orleans.Serialization;
using StackExchange.Redis;

namespace Orleans.Redis.Clustering.Messaging
{
    public class RedisGatewayListProvider : IGatewayListProvider
    {
        private readonly RedisKey _clusterKey;
        private readonly IDatabase _db;
        private readonly ILogger<RedisGatewayListProvider> _logger;

        public RedisGatewayListProvider(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<RedisClusteringClientOptions> clusteringOptions,
            IConnectionMultiplexer connectionMultiplexer,
            IOptions<GatewayOptions> gatewayOptions,
            ILogger<RedisGatewayListProvider> logger
        )
        {
            _clusterKey = (RedisKey) $"${clusteringOptions.Value.KeyPrefix}${clusterOptions.Value.ClusterId}";
            _db = connectionMultiplexer.GetDatabase(clusteringOptions.Value.Database);
            _logger = logger;
            MaxStaleness = gatewayOptions.Value.GatewayListRefreshPeriod;
        }

        public TimeSpan MaxStaleness { get; }

        public bool IsUpdatable => true;

        public Task InitializeGatewayListProvider() => Task.CompletedTask;

        public async Task<IList<Uri>> GetGateways()
        {
            var membershipTableData = await RedisMembershipTable.ReadAll(_clusterKey, _db, _logger);
            if (membershipTableData == null) return new List<Uri>();

            return membershipTableData.Members.Select(e => e.Item1)
                .Where(m => m.Status == SiloStatus.Active && m.ProxyPort != 0).Select(m =>
                {
                    var endpoint = new IPEndPoint(m.SiloAddress.Endpoint.Address, m.ProxyPort);
                    var gatewayAddress = SiloAddress.New(endpoint, m.SiloAddress.Generation);
                    return gatewayAddress.ToGatewayUri();
                }).ToList();
        }
    }
}