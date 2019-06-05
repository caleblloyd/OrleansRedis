using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Runtime;

namespace Orleans.Redis.Serialization
{
    public class MembershipTableDataConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(MembershipTableData);
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var mtd = (MembershipTableData) value;
            writer.WriteStartObject();
            writer.WritePropertyName("Members");
            serializer.Serialize(writer, mtd.Members);
            writer.WritePropertyName("Version");
            serializer.Serialize(writer, mtd.Version);
            writer.WriteEndObject();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
            JsonSerializer serializer)
        {
            var jo = JObject.Load(reader);
            var members = jo["Members"].ToObject<List<Tuple<MembershipEntry, string>>>(serializer);
            var version = jo["Version"].ToObject<TableVersion>(serializer);
            var mtd = new MembershipTableData(members, version);
            return mtd;
        }
    }
}