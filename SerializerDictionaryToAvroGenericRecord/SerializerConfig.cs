
namespace SerializerDictionaryToAvroGenericRecord
{
    public class SerializerConfig
    {
        public string id { get; set; }
        public string name { get; set; }
        public Mssgtype[] mssgtypes { get; set; }
    }

    public class Mssgtype
    {
        public string id { get; set; }
        public string name { get; set; }
        public string schemaRegistryUrls { get; set; }
        public string schemaId { get; set; }
        public string schemaSubject { get; set; }
        public Recipient[] recipients { get; set; }
    }

    public class Recipient
    {
        public string publisher_id { get; set; }
        public string recipient { get; set; }
    }

}
