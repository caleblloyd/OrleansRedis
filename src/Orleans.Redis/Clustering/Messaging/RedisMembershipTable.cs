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
        
        private static readonly Lazy<JsonSerializerSettings> SerializationSettings = new Lazy<JsonSerializerSettings>(() =>
        {
            var settings = new JsonSerializerSettings();
            settings.Converters.Add(new SiloAddressConverter());
            settings.Converters.Add(new MembershipEntryConverter());
            settings.Converters.Add(new MembershipTableDataConverter());
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

            var dataJson = result[0].ToString();
            var aliveJson = result[1].ToString();
            if (string.IsNullOrEmpty(dataJson) || string.IsNullOrEmpty(aliveJson))
            {
                return null;
            }

            var alive = JsonConvert.DeserializeObject<DateTime>(result[0].ToString(), SerializationSettings.Value);
            var data = JsonConvert.DeserializeObject<MembershipTableData>(result[1].ToString(), SerializationSettings.Value);
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

            var version = new TableVersion(0, "");
            var memberMap = new Dictionary<string, Tuple<MembershipEntry, string>>();
            var aliveMap = new Dictionary<string, DateTime>();
            var deleteHashKeysList = new List<string>();
            foreach (var result in results)
            {
                var resultName = result.Name.ToString();
                var resultJson = result.Value.ToString();
                if (resultName.EndsWith("-data"))
                {
                    var member = JsonConvert.DeserializeObject<MembershipTableData>(resultJson, SerializationSettings.Value);
                    if (version == null || member.Version.Version > version.Version)
                    {
                        if (memberMap.Count > 0)
                        {
                            deleteHashKeysList.AddRange(memberMap.Keys.Select(m => $"{m}-data"));
                            deleteHashKeysList.AddRange(memberMap.Keys.Select(m => $"{m}-alive"));
                            memberMap.Clear();
                        }

                        version = member.Version;
                    }

                    if (member.Version.Version == version.Version)
                    {
                        memberMap[resultName.Substring(0, resultName.Length - "-data".Length)] = member.Members[0];
                    }
                    else
                    {
                        deleteHashKeysList.Add($"{resultName}-data");
                        deleteHashKeysList.Add($"{resultName}-alive");
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

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace(
                    $"RedisMembershipTable.InsertRow called with entry {entry} and tableVersion {tableVersion}.");
            }

            if (entry == null)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug(
                        "RedisMembershipTable.InsertRow aborted due to null check. MembershipEntry is null.");
                }

                throw new ArgumentNullException(nameof(entry));
            }

            if (tableVersion == null)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisMembershipTable.InsertRow aborted due to null check. TableVersion is null ");
                }

                throw new ArgumentNullException(nameof(tableVersion));
            }

            var hashKeyData = $"{entry.SiloAddress.ToParsableString()}-data";
            var hashKeyAlive = $"{entry.SiloAddress.ToParsableString()}-alive";
            var data = JsonConvert.SerializeObject(ConvertToMembershipTableData(new[]
            {
                new Tuple<MembershipEntry, int>(entry, tableVersion.Version)
            }), SerializationSettings.Value);
            var alive = JsonConvert.SerializeObject(entry.IAmAliveTime, SerializationSettings.Value);

            try
            {
                await _db.HashSetAsync(_clusterKey, new[]
                {
                    new HashEntry(hashKeyData, data),
                    new HashEntry(hashKeyAlive, alive)
                });
                return true;
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

        public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace(
                    $"RedisMembershipTable.UpdateRow called with entry {entry}, etag {etag} and tableVersion {tableVersion}.");
            }

            return InsertRow(entry, tableVersion);
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

        private static MembershipTableData ConvertToMembershipTableData(IEnumerable<Tuple<MembershipEntry, int>> ret)
        {
            var retList = ret.ToList();
            var tableVersionEtag = retList[0].Item2;
            var membershipEntries = new List<Tuple<MembershipEntry, string>>();
            if (retList[0].Item1 != null)
            {
                membershipEntries.AddRange(
                    retList.Select(i => new Tuple<MembershipEntry, string>(i.Item1, string.Empty)));
            }

            return new MembershipTableData(membershipEntries,
                new TableVersion(tableVersionEtag, tableVersionEtag.ToString()));
        }
    }
}