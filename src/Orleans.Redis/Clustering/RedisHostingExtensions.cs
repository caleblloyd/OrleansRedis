using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Messaging;
using Orleans.Redis.Clustering.Messaging;
using Orleans.Redis.Clustering.Options;

namespace Orleans.Redis.Clustering
{
    public static class RedisHostingExtensions
    {
        /// <summary>
        /// Configures the silo to use Redis for clustering.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloHostBuilder"/>.
        /// </returns>
        public static ISiloHostBuilder UseRedisClustering(
            this ISiloHostBuilder builder,
            Action<RedisClusteringSiloOptions> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    if (configureOptions != null)
                    {
                        services.Configure(configureOptions);
                    }

                    services.AddSingleton<IMembershipTable, RedisMembershipTable>();
                });
        }

        /// <summary>
        /// Configures the silo to use Redis for clustering.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloHostBuilder"/>.
        /// </returns>
        public static ISiloHostBuilder UseRedisClustering(
            this ISiloHostBuilder builder,
            Action<OptionsBuilder<RedisClusteringSiloOptions>> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    configureOptions?.Invoke(services.AddOptions<RedisClusteringSiloOptions>());
                    services.AddSingleton<IMembershipTable, RedisMembershipTable>();
                });
        }

        /// <summary>
        /// Configures the silo to use Redis for clustering.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloBuilder"/>.
        /// </returns>
        public static ISiloBuilder UseRedisClustering(
            this ISiloBuilder builder,
            Action<RedisClusteringSiloOptions> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    if (configureOptions != null)
                    {
                        services.Configure(configureOptions);
                    }

                    services.AddSingleton<IMembershipTable, RedisMembershipTable>();
                });
        }

        /// <summary>
        /// Configures the silo to use Redis for clustering.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloBuilder"/>.
        /// </returns>
        public static ISiloBuilder UseRedisClustering(
            this ISiloBuilder builder,
            Action<OptionsBuilder<RedisClusteringSiloOptions>> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    configureOptions?.Invoke(services.AddOptions<RedisClusteringSiloOptions>());
                    services.AddSingleton<IMembershipTable, RedisMembershipTable>();
                });
        }

        /// <summary>
        /// Configures the client to use Redis for clustering.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="IClientBuilder"/>.
        /// </returns>
        public static IClientBuilder UseRedisClustering(
            this IClientBuilder builder,
            Action<RedisClusteringClientOptions> configureOptions)
        {
            return builder.ConfigureServices(services =>
            {
                if (configureOptions != null)
                {
                    services.Configure(configureOptions);
                }

                services.AddSingleton<IGatewayListProvider, RedisGatewayListProvider>();
            });
        }

        /// <summary>
        /// Configures the client to use Redis for clustering.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="IClientBuilder"/>.
        /// </returns>
        public static IClientBuilder UseRedisClustering(
            this IClientBuilder builder,
            Action<OptionsBuilder<RedisClusteringClientOptions>> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    configureOptions?.Invoke(services.AddOptions<RedisClusteringClientOptions>());
                    services.AddSingleton<IGatewayListProvider, RedisGatewayListProvider>();
                });
        }
    }
}