using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Orleans.Configuration;
using Orleans.Redis.Clustering.Options;
using Orleans.Redis.Serialization;
using Orleans.Runtime;
using Orleans.Serialization;
using StackExchange.Redis;

namespace Orleans.Redis.Clustering.Messaging
{
    public class RedisMembershipTable : IMembershipTable
    {
        private readonly RedisKey _clusterKey;
        private readonly IOptions<RedisClusteringSiloOptions> _clusteringOptions;
        private readonly IDatabase _db;
        private readonly ILogger<RedisMembershipTable> _logger;

        private static readonly Lazy<JsonSerializerSettings> SerializationSettings = new Lazy<JsonSerializerSettings>(
            () =>
            {
                var settings = new JsonSerializerSettings();
                settings.Converters.Add(new MembershipEntryConverter());
                settings.Converters.Add(new MembershipTableDataConverter());
                settings.Converters.Add(new TableVersionConverter());
                settings.Converters.Add(new SiloAddressConverter());
                settings.Converters.Add(new StringEnumConverter());
                return settings;
            });

        public RedisMembershipTable(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<RedisClusteringSiloOptions> clusteringOptions,
            IConnectionMultiplexer connectionMultiplexer,
            ILogger<RedisMembershipTable> logger
        )
        {
            _clusterKey = (RedisKey) $"${clusteringOptions.Value.KeyPrefix}${clusterOptions.Value.ClusterId}";
            _clusteringOptions = clusteringOptions;
            _db = connectionMultiplexer.GetDatabase(clusteringOptions.Value.Database);
            _logger = logger;
        }

        public Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace("RedisMembershipTable.InitializeMembershipTable called.");
            }

            return Task.CompletedTask;
        }

        public async Task DeleteMembershipTableEntries(string clusterId)
        {
            var clusterKey = (RedisKey) $"${_clusteringOptions.Value.KeyPrefix}${clusterId}";
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace(
                    $"RedisMembershipTable.DeleteMembershipTableEntries called with clusterId {clusterId}.");
            }

            try
            {
                await _db.KeyDeleteAsync(clusterKey);
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisMembershipTable.DeleteMembershipTableEntries failed: {0}", ex);
                }

                throw;
            }
        }

        public Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace("RedisMembershipTable.CleanupDefunctSiloEntries called.");
            }

            throw new NotImplementedException();
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace($"RedisMembershipTable.ReadRow called with key: {key}.");
            }

            var hashKeyData = $"{key.ToParsableString()}-data";
            var hashKeyAlive = $"{key.ToParsableString()}-alive";
            RedisValue[] redisFields = {hashKeyData, hashKeyAlive};
            RedisValue[] result;
            try
            {
                result = await _db.HashGetAsync(_clusterKey, redisFields);
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisMembershipTable.ReadRow failed: {0}", ex);
                }

                throw;
            }

            if (string.IsNullOrEmpty(result[0]) || string.IsNullOrEmpty(result[1]))
            {
                return null;
            }

            var data = JsonConvert.DeserializeObject<MembershipTableData>(result[0], SerializationSettings.Value);
            var alive = JsonConvert.DeserializeObject<DateTime>(result[1], SerializationSettings.Value);
            data.Members[0].Item1.IAmAliveTime = alive;
            return data;
        }

        public Task<MembershipTableData> ReadAll() => ReadAll(_clusterKey, _db, _logger);

        internal static async Task<MembershipTableData> ReadAll(RedisKey clusterKey, IDatabase db, ILogger logger)
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.Trace("RedisMembershipTable.ReadAll called.");
            }

            HashEntry[] results;
            try
            {
                results = await db.HashGetAllAsync(clusterKey);
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    logger.Debug("RedisMembershipTable.ReadAll failed: {0}", ex);
                }

                throw;
            }

            var version = new TableVersion(0, "0");
            var memberMap = new Dictionary<string, Tuple<MembershipEntry, string>>();
            var aliveMap = new Dictionary<string, DateTime>();
            var deleteHashKeysList = new List<string>();
            foreach (var result in results)
            {
                var resultName = result.Name.ToString();
                var resultJson = result.Value.ToString();
                if (resultName.EndsWith("-data"))
                {
                    var member =
                        JsonConvert.DeserializeObject<MembershipTableData>(resultJson, SerializationSettings.Value);
                    
                    if (version == null || member.Version.Version > version.Version)
                    {
                        version = member.Version;
                    }

                    if (member.Members[0].Item1.Status == SiloStatus.Dead)
                    {
                        deleteHashKeysList.Add($"{resultName}-data");
                        deleteHashKeysList.Add($"{resultName}-alive");
                        deleteHashKeysList.Add($"{resultName}-etag");
                    }
                    else
                    {
                        memberMap[resultName.Substring(0, resultName.Length - "-data".Length)] = member.Members[0];
                    }
                }
                else if (resultName.EndsWith("-alive"))
                {
                    var alive = JsonConvert.DeserializeObject<DateTime>(resultJson, SerializationSettings.Value);
                    aliveMap[resultName.Substring(0, resultName.Length - "-alive".Length)] = alive;
                }
            }

            if (memberMap.Count == 0)
            {
                return new MembershipTableData(new List<Tuple<MembershipEntry, string>>(), version);
            }

            foreach (var entry in memberMap)
            {
                entry.Value.Item1.IAmAliveTime = aliveMap[$"{entry.Key}"];
            }

            if (deleteHashKeysList.Count > 0)
            {
                try
                {
                    var deleteHashKeys = deleteHashKeysList.Select(m => (RedisValue) m).ToArray();
                    await db.HashDeleteAsync(clusterKey, deleteHashKeys);
                }
                catch (Exception ex)
                {
                    if (logger.IsEnabled(LogLevel.Debug))
                    {
                        logger.Debug("RedisMembershipTable.ReadAll failed to delete stale hash keys: {0}", ex);
                    }

                    throw;
                }
            }

            return new MembershipTableData(memberMap.Values.ToList(), version);
        }

        public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace(
                    $"RedisMembershipTable.InsertRow called with entry {entry} and tableVersion {tableVersion}.");
            }

            return UpsertRow(entry, tableVersion, true, null);
        }

        public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace(
                    $"RedisMembershipTable.UpdateRow called with entry {entry}, etag {etag} and tableVersion {tableVersion}.");
            }

            return UpsertRow(entry, tableVersion, false, etag);
        }

        private async Task<bool> UpsertRow(MembershipEntry entry, TableVersion tableVersion, bool insert,
            string checkEtag)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace(
                    $"RedisMembershipTable.UpsertRow called with entry {entry} and tableVersion {tableVersion}.");
            }

            if (entry == null)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug(
                        "RedisMembershipTable.UpsertRow aborted due to null check. MembershipEntry is null.");
                }

                throw new ArgumentNullException(nameof(entry));
            }

            if (tableVersion == null)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisMembershipTable.UpsertRow aborted due to null check. TableVersion is null ");
                }

                throw new ArgumentNullException(nameof(tableVersion));
            }

            var hashKeyData = $"{entry.SiloAddress.ToParsableString()}-data";
            var hashKeyAlive = $"{entry.SiloAddress.ToParsableString()}-alive";
            var hashKeyEtag = $"{entry.SiloAddress.ToParsableString()}-etag";
            var etag = Guid.NewGuid().ToString();
            var mtd = new MembershipTableData(new Tuple<MembershipEntry, string>(entry, etag), tableVersion);
            var data = JsonConvert.SerializeObject(mtd, SerializationSettings.Value);
            var alive = JsonConvert.SerializeObject(entry.IAmAliveTime, SerializationSettings.Value);

            try
            {
                var txn = _db.CreateTransaction();
// async calls in Redis Transaction do not have to be awaited
#pragma warning disable 4014
                txn.AddCondition(insert
                    ? Condition.HashNotExists(_clusterKey, hashKeyData)
                    : Condition.HashEqual(_clusterKey, hashKeyEtag, checkEtag));
                txn.HashSetAsync(_clusterKey, new[]
                {
                    new HashEntry(hashKeyData, data),
                    new HashEntry(hashKeyAlive, alive),
                    new HashEntry(hashKeyEtag, etag)
                });
#pragma warning restore 4014
                return await txn.ExecuteAsync();
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisMembershipTable.UpsertRow failed: {0}", ex);
                }

                throw;
            }
        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace($"RedisMembershipTable.UpdateIAmAlive called with entry {entry}.");
            }

            var hashKeyAlive = $"{entry.SiloAddress.ToParsableString()}-alive";
            var alive = JsonConvert.SerializeObject(entry.IAmAliveTime, SerializationSettings.Value);
            try
            {
                await _db.HashSetAsync(_clusterKey, new[]
                {
                    new HashEntry(hashKeyAlive, alive)
                });
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisMembershipTable.InsertRow failed: {0}", ex);
                }

                throw;
            }
        }
    }
}