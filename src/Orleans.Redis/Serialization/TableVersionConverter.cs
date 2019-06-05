using System;
using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Runtime;

namespace Orleans.Redis.Serialization
{
    public class TableVersionConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(TableVersion);
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var tv = (TableVersion) value;
            writer.WriteStartObject();
            writer.WritePropertyName("Version");
            serializer.Serialize(writer, tv.Version);
            writer.WritePropertyName("VersionEtag");
            serializer.Serialize(writer, tv.VersionEtag);
            writer.WriteEndObject();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
            JsonSerializer serializer)
        {
            var jo = JObject.Load(reader);
            return new TableVersion(
                jo["Version"].ToObject<int>(),
                jo["VersionEtag"].ToObject<string>()
            );
        }
    }
}