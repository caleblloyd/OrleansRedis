namespace Orleans.Redis.Clustering.Options
{
    public class RedisClusteringSiloOptions
    {
        public int Database { get; set; } = -1;

        public string KeyPrefix { get; set; } = "orleans-";
    }
}