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
            return (objectType == typeof(MembershipTableData));
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var mtd = (MembershipTableData) value;

            writer.WriteStartObject();
            writer.WritePropertyName("Members");
            writer.WriteStartArray();
            foreach (var member in mtd.Members)
            {
                serializer.Serialize(writer, member.Item1);
            }

            writer.WriteEndArray();

            writer.WritePropertyName("Version");
            serializer.Serialize(writer, mtd.Version);
            writer.WriteEndObject();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
            JsonSerializer serializer)
        {
            var jo = JObject.Load(reader);

            var members = jo["Members"].ToObject<List<MembershipEntry>>(serializer);
            var version = jo["Version"].ToObject<TableVersion>(serializer);

            var membershipEntries = new List<Tuple<MembershipEntry, string>>();
            if (members != null)
            {
                membershipEntries.AddRange(
                    members.Select(i => new Tuple<MembershipEntry, string>(i, string.Empty)));
            }

            return new MembershipTableData(membershipEntries, version);
        }
    }
}