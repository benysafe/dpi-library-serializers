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

namespace SerializerJsonToJson
{
    public class MssgTypeStruct
    {
        public string id = null;
        public string name = null;
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

    public class SerializerJsonToJson : ISerializer
    {
        private IConfigurator _configurator;
        private Logger _logger;
        private string _id;
        private Dictionary<string, MssgTypeStruct> _dicMssgTypes = new Dictionary<string, MssgTypeStruct>();

        public void addPublisher(string id, IPublisher publisher)
        {
            try
            {
                _logger.Trace("Inicio");
                bool added = false;

                DestinationStruct tempDestination = new DestinationStruct();

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
                _logger = (Logger)logger.init("SerializerDictionaryToJson");
                this.GetConfig();
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public void serialize(string mssgType, object payload, string priority)
        {
            try
            {
                _logger.Trace("Inicio");

                if (_configurator.hasNewConfig(_id))
                {
                    GetConfig();
                    _logger.Debug("Reconfiguracion exitosa");
                }

                string strJson = payload.ToString();

                byte[] outPayload = Encoding.UTF8.GetBytes(strJson);    //string to byte[]

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
                            _dicMssgTypes[mssgType].destinations[index].publisher.publish(_dicMssgTypes[mssgType].destinations[index].recipent, outPayload, priority);
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
        private void GetConfig()
        {
            try
            {
                _dicMssgTypes.Clear(); 
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
    }
}
