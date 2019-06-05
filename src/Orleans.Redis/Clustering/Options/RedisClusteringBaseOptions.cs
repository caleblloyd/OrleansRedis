using System;
using System.IO;
using StackExchange.Redis;

namespace Orleans.Redis.Clustering.Options
{
    public abstract class RedisClusteringBaseOptions
    {
        private IConnectionMultiplexer _connectionMultiplexer;

        public IConnectionMultiplexer ConnectionMultiplexer
        {
            get
            {
                if (_connectionMultiplexer != null)
                {
                    return _connectionMultiplexer;
                }

                if (Configuration == null && ConfigurationOptions == null)
                {
                    throw new MissingFieldException(
                        "ConnectionMultiplexer, Configuration, or ConfigurationOptions must be set");
                }

                if (Configuration != null && ConfigurationOptions != null)
                {
                    throw new MissingFieldException(
                        "Either Configuration or ConfigurationOptions should be set, but not both");
                }

                if (Configuration != null)
                {
                    _connectionMultiplexer = StackExchange.Redis.ConnectionMultiplexer.Connect(Configuration, Log);
                }
                else if (ConfigurationOptions != null)
                {
                    _connectionMultiplexer =
                        StackExchange.Redis.ConnectionMultiplexer.Connect(ConfigurationOptions, Log);
                }

                return _connectionMultiplexer;
            }
            set => _connectionMultiplexer = value;
        }

        public string Configuration { get; set; }

        public ConfigurationOptions ConfigurationOptions { get; set; }

        public TextWriter Log { get; set; }

        public int Database { get; set; } = -1;

        public string KeyPrefix { get; set; } = "orleans-";
    }
}