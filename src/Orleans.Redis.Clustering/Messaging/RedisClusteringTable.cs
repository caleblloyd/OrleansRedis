using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Runtime;
using StackExchange.Redis;

namespace Orleans.Redis.Clustering.Messaging
{
    public class RedisClusteringTable : IMembershipTable
    {
        private readonly IDatabase _db;
        private readonly string _clusterId;
        private readonly ILogger<RedisClusteringTable> _logger;

        public RedisClusteringTable(
            IConnectionMultiplexer connectionMultiplexer,
            IOptions<ClusterOptions> clusterOptions,
            ILogger<RedisClusteringTable> logger
        )
        {
            _db = connectionMultiplexer.GetDatabase();
            _clusterId = clusterOptions.Value.ClusterId;
            _logger = logger;
        }

        public Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace("RedisClusteringTable.InitializeMembershipTable called.");
            }

            return Task.CompletedTask;
        }

        public async Task DeleteMembershipTableEntries(string clusterId)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace(
                    $"RedisClusteringTable.DeleteMembershipTableEntries called with clusterId {clusterId}.");
            }

            try
            {
                await _db.KeyDeleteAsync((RedisKey) _clusterId);
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisClusteringTable.DeleteMembershipTableEntries failed: {0}", ex);
                }

                throw;
            }
        }

        public Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace("RedisClusteringTable.CleanupDefunctSiloEntries called.");
            }

            throw new NotImplementedException();
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace($"RedisClusteringTable.ReadRow called with key: {key}.");
            }

            var hashKeyData = $"{key.ToParsableString()}-data";
            var hashKeyAlive = $"{key.ToParsableString()}-alive";
            RedisValue[] redisFields = {hashKeyData, hashKeyAlive};
            RedisValue[] result;
            try
            {
                result = await _db.HashGetAsync((RedisKey) _clusterId, redisFields);
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisClusteringTable.ReadRow failed: {0}", ex);
                }

                throw;
            }

            var dataJson = result[0].ToString();
            var aliveJson = result[1].ToString();
            if (string.IsNullOrEmpty(dataJson) || string.IsNullOrEmpty(aliveJson))
            {
                return null;
            }

            var alive = JsonConvert.DeserializeObject<DateTime>(result[0].ToString());
            var data = JsonConvert.DeserializeObject<MembershipTableData>(result[1].ToString());
            data.Members[0].Item1.IAmAliveTime = alive;
            return data;
        }

        public async Task<MembershipTableData> ReadAll()
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace("RedisClusteringTable.ReadAll called.");
            }

            HashEntry[] results;
            try
            {
                results = await _db.HashGetAllAsync((RedisKey) _clusterId);
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisClusteringTable.ReadAll failed: {0}", ex);
                }

                throw;
            }

            TableVersion version = null;
            var memberMap = new Dictionary<string, Tuple<MembershipEntry, string>>();
            var aliveMap = new Dictionary<string, DateTime>();
            var deleteHashKeysList = new List<string>();
            foreach (var result in results)
            {
                var resultName = result.Name.ToString();
                var resultJson = result.Value.ToString();
                if (resultName.EndsWith("-data"))
                {
                    var member = JsonConvert.DeserializeObject<MembershipTableData>(resultJson);
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
                        memberMap[resultName.Substring(0, "-data".Length)] = member.Members[0];
                    }
                    else
                    {
                        deleteHashKeysList.Add($"{resultName}-data");
                        deleteHashKeysList.Add($"{resultName}-alive");
                    }
                }
                else if (resultName.EndsWith("-alive"))
                {
                    var alive = JsonConvert.DeserializeObject<DateTime>(resultJson);
                    aliveMap[resultName.Substring(0, "-alive".Length)] = alive;
                }
            }

            if (memberMap.Count == 0)
            {
                return null;
            }

            foreach (var entry in memberMap)
            {
                entry.Value.Item1.IAmAliveTime = aliveMap[$"{entry.Key}-alive"];
            }

            if (deleteHashKeysList.Count > 0)
            {
                try
                {
                    var deleteHashKeys = deleteHashKeysList.Select(m => (RedisValue) m).ToArray();
                    await _db.HashDeleteAsync(_clusterId, deleteHashKeys);
                }
                catch (Exception ex)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.Debug("RedisClusteringTable.ReadAll failed to delete stale hash keys: {0}", ex);
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
                    $"RedisClusteringTable.InsertRow called with entry {entry} and tableVersion {tableVersion}.");
            }

            if (entry == null)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug(
                        "RedisClusteringTable.InsertRow aborted due to null check. MembershipEntry is null.");
                }

                throw new ArgumentNullException(nameof(entry));
            }

            if (tableVersion == null)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisClusteringTable.InsertRow aborted due to null check. TableVersion is null ");
                }

                throw new ArgumentNullException(nameof(tableVersion));
            }

            var hashKeyData = $"{entry.SiloAddress.ToParsableString()}-data";
            var hashKeyAlive = $"{entry.SiloAddress.ToParsableString()}-alive";
            var data = JsonConvert.SerializeObject(ConvertToMembershipTableData(new[]
            {
                new Tuple<MembershipEntry, int>(entry, tableVersion.Version)
            }));
            var alive = JsonConvert.SerializeObject(entry.IAmAliveTime);

            try
            {
                await _db.HashSetAsync(_clusterId, new[]
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
                    _logger.Debug("RedisClusteringTable.InsertRow failed: {0}", ex);
                }

                throw;
            }
        }

        public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace(
                    $"RedisClusteringTable.UpdateRow called with entry {entry}, etag {etag} and tableVersion {tableVersion}.");
            }

            return InsertRow(entry, tableVersion);
        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.Trace($"RedisClusteringTable.UpdateIAmAlive called with entry {entry}.");
            }

            var hashKeyAlive = $"{entry.SiloAddress.ToParsableString()}-alive";
            var alive = JsonConvert.SerializeObject(entry.IAmAliveTime);
            try
            {
                await _db.HashSetAsync(_clusterId, new[]
                {
                    new HashEntry(hashKeyAlive, alive)
                });
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Debug("RedisClusteringTable.InsertRow failed: {0}", ex);
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