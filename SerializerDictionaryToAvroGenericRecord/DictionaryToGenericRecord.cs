using Definitions;
using InterfaceLibraryConfigurator;
using InterfaceLibrarySerializer;
using InterfaceLibraryPublisher;
using System;
using System.Text.Json.Serialization;
using System.Collections.Generic;
using System.Threading;
using InterfaceLibraryLogger;
using NLog;
using System.Text;
using Newtonsoft.Json;
using Avro.Generic;
using Avro;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace SerializerDictionaryToAvroGenericRecord
{
    public class MssgTypeStruct
    {
        public string id = null;
        public string name = null;
        public string strSchemaRegistry = null;
        public List<DestinationStruct> destinations = new List<DestinationStruct>();
    }
    public class DestinationStruct
    {
        public string recipent = null;
        public string publisherId = null;
        public IPublisher publisher;

        public void SetPublisher(IPublisher publisher)
        {
            this.publisher = publisher;
        }
    }
    
    public class DictionaryToGenericRecord : ISerializer
    {
        private IConfigurator _configurator;
        private Logger _logger;
        private string _id;
        private Dictionary<string, MssgTypeStruct> _dicMssgTypes = new Dictionary<string, MssgTypeStruct>();

        private string GetSchemaStr (string schemaRegistryList, string schemaId, string schemaSubject)
        {
            _logger.Trace("Inicio");
            string strOut = null;
            try
            {
                if (schemaRegistryList == null || schemaId == null || schemaSubject == null) 
                {
                    throw new Exception($"Listado de con Urls de Schemas vacio o Id del schema no espesificado");
                }
                var schemaRegistryConfig = new SchemaRegistryConfig
                {
                    Url = schemaRegistryList
                };
                var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
                var taskSchema = schemaRegistry.GetSchemaAsync(Convert.ToInt32(schemaId));
                taskSchema.Wait();

                strOut = taskSchema.Result.SchemaString;
                _logger.Trace("Fin");
                return strOut;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        private RecordSchema GetSubSchemaFromField(RecordSchema schema, string fieldName, ref bool error)
        {
            try
            {
                if (schema == null)
                {
                    error = true;
                    return null;
                }

                if (schema.TryGetField(fieldName, out var field))
                {
                    List<object> fields = System.Text.Json.JsonSerializer.Deserialize<List<object>>(field.Schema.ToString());
                    Dictionary<string, object> obj = null;

                    for (int i = 0; i < fields.Count; i++)
                    {
                        if (fields[i] != null)
                        {
                            obj = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(fields[i].ToString());
                        }
                    }

                    if (obj != null)
                    {
                        string strField = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(obj);

                        return (RecordSchema)RecordSchema.Parse(strField);
                    }
                }
                error = true;
                return null;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        private GenericRecord MakeSimpleGenericRecordFromDictionary(Dictionary<string, object> dic, RecordSchema schema, ref bool error)
        {
            try
            {

                bool subError = false;

                GenericRecord record = new GenericRecord(schema);
                foreach (var kvp in dic)
                {
                    if (kvp.Value is Dictionary<string, object>)
                    {
                        bool getSchemaError = false;
                        var subRecordSchema = GetSubSchemaFromField(schema, kvp.Key, ref getSchemaError);

                        if (!getSchemaError)
                        {
                            var subRecord = MakeSimpleGenericRecordFromDictionary((Dictionary<string, object>)dic[kvp.Key], subRecordSchema, ref subError);
                            if (!subError)
                            {
                                record.Add(kvp.Key, subRecord);
                            }
                            else
                            {
                                _logger.Error($"Error consrtuyendo el Record para el atributo {kvp.Key}");
                                error = true;
                                return null;
                            }
                        }
                        else
                        {
                            _logger.Error($"Error obteniendo el subesquema");
                            error = true;
                            return null;
                        }
                    }
                    /*else if (kvp.Value is List<object>)
                    {
                        bool getSchemaError = false;
                        var subRecordSchema = GetSubSchemaFromField(schema, kvp.Key, ref getSchemaError);

                        if (!getSchemaError)
                        {
                            List<object> list = (List<object>)kvp.Value;
                            if (list.Count > 0)
                            {
                                if (list.First() is string)
                                {
                                    List<string> listRecord = new List<string>();
                                    foreach (object item in list)
                                    {
                                        listRecord.Add((string)item);
                                    }
                                    record.Add(kvp.Key, listRecord);
                                }
                                else if (list.First() is int)
                                {
                                    List<int> listRecord = new List<int>();
                                    foreach (object item in list)
                                    {
                                        listRecord.Add((int)item);
                                    }
                                    record.Add(kvp.Key, listRecord);
                                }
                                else if (list.First() is bool)
                                {
                                    List<bool> listRecord = new List<bool>();
                                    foreach (object item in list)
                                    {
                                        listRecord.Add((bool)item);
                                    }
                                    record.Add(kvp.Key, listRecord);
                                }
                                else if (list.First() is long)
                                {
                                    List<long> listRecord = new List<long>();
                                    foreach (object item in list)
                                    {
                                        listRecord.Add((long)item);
                                    }
                                    record.Add(kvp.Key, listRecord);
                                }
                                else if (list.First() is double)
                                {
                                    List<double> listRecord = new List<double>();
                                    foreach (object item in list)
                                    {
                                        listRecord.Add((double)item);
                                    }
                                    record.Add(kvp.Key, listRecord);
                                }
                                else if (list.First() is Dictionary<string, object>)
                                {
                                    List<GenericRecord> listRecord = new List<GenericRecord>();
                                    foreach (Dictionary<string, object> item in list)
                                    {
                                        foreach (var subKvp in item)
                                        {
                                            bool getSubSchemaError = false;
                                            var subSubRecordSchema = GetSubSchemaFromField(subRecordSchema, subKvp.Key, ref getSubSchemaError);

                                            if (!getSubSchemaError)
                                            {
                                                bool itemSubError = false;
                                                var itemSubRecord = MakeSimpleGenericRecordFromDictionary(, itemSubRecordSchema, ref itemSubError);
                                                if (!itemSubError)
                                                {
                                                    listRecord.Add(itemSubRecord);
                                                }
                                                else
                                                {
                                                    error = true;
                                                    return null;
                                                }
                                            }
                                            record.Add(kvp.Key, listRecord);
                                        }
                                    }
                                }
                            }
                            else
                            {
                                //error: Tipo de dato no admitido
                                error = true;
                                return null;
                            }
                        }
                        else
                        {
                            //error: Error obteniendo el subesquema
                            error = true;
                            return null;
                        }
                    }*/
                    else
                    {
                        record.Add(kvp.Key, kvp.Value);
                        _logger.Debug($"'{kvp.Key}': {kvp.Value}    type: {kvp.Value.GetType().FullName}");
                    }
                }
                return record;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        /*private GenericArray<GenericRecord> MakeSimpleGenericArrayFromList(List<object> list, RecordSchema schema, ref bool error)
        {
            if (list == null)
            {
                error = true;
                return null;
            }

        }*/
        
        public void addPublisher(string id, IPublisher publisher)
        {
            try
            {
                _logger.Trace("Inicio");
                bool added = false;

                foreach (KeyValuePair<string, MssgTypeStruct> entry in _dicMssgTypes)
                {
                    for (int index = 0; index < entry.Value.destinations.Count; index++)
                    {
                        if (entry.Value.destinations[index].publisherId == id)
                        {
                            entry.Value.destinations[index].SetPublisher(publisher);
                            added = true;
                        }
                    }
                }
                if (!added)
                    _logger.Error($"No se encontro un publicador con id '{id}' en la configuracion de la serializacion");
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _id = id;
                _configurator = configurator;
                _logger = (Logger)logger.init("SerializerDictionaryToAvroGenericRecord");
                this.GetConfig();
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        private void GetConfig()
        {
            try
            {
                string strConfig = _configurator.getValue("serializers", _id);
                var serializersConfig = JsonConvert.DeserializeObject<List<SerializerConfig>>(strConfig);
                if (serializersConfig != null)
                {
                    for (int iSerializer = 0; iSerializer < serializersConfig.Count; iSerializer++)
                    {
                        SerializerConfig serializerConfig = serializersConfig[iSerializer];
                        if (serializerConfig.mssgtypes.Length > 0)
                        {
                            for (int i = 0; i < serializerConfig.mssgtypes.Length; i++)
                            {
                                MssgTypeStruct newMssgType = new MssgTypeStruct();
                                newMssgType.id = serializerConfig.mssgtypes[i].id;
                                newMssgType.name = serializerConfig.mssgtypes[i].name;
                                newMssgType.strSchemaRegistry = GetSchemaStr(serializerConfig.mssgtypes[i].schemaRegistryUrls,
                                                                            serializerConfig.mssgtypes[i].schemaId, 
                                                                            serializerConfig.mssgtypes[i].schemaSubject);
                                if (serializerConfig.mssgtypes[i].recipients.Length > 0)
                                {
                                    for (int j = 0; j < serializerConfig.mssgtypes[i].recipients.Length; j++)
                                    {
                                        DestinationStruct destination = new DestinationStruct();
                                        destination.publisherId = serializerConfig.mssgtypes[i].recipients[j].publisher_id;
                                        destination.recipent = serializerConfig.mssgtypes[i].recipients[j].recipient;
                                        newMssgType.destinations.Add(destination);
                                    }
                                }
                                _dicMssgTypes.Add(newMssgType.id, newMssgType);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void serialize(string mssgType, object payload, string priority)
        {
            try
            {
                _logger.Trace("Inicio");
                Dictionary<string, object> dicPayload = (Dictionary<string, object>)payload;

                var recordSchema = (RecordSchema)RecordSchema.Parse(_dicMssgTypes[mssgType].strSchemaRegistry);
                _logger.Debug($"Esquema: \n {_dicMssgTypes[mssgType].strSchemaRegistry}");
                bool error = false;
                var record = MakeSimpleGenericRecordFromDictionary(dicPayload, recordSchema, ref error);

                if (!error)
                {
                    if (priority is null)
                    {
                        priority = "NORMAL";
                    }

                    if (_dicMssgTypes.ContainsKey(mssgType))
                    {
                        if (_dicMssgTypes[mssgType].destinations.Count > 0)
                        {
                            for (int index = 0; index < _dicMssgTypes[mssgType].destinations.Count; index++)
                            {
                                _dicMssgTypes[mssgType].destinations[index].publisher.publish(_dicMssgTypes[mssgType].destinations[index].recipent, record, priority);
                            }
                        }
                    }
                }

                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}