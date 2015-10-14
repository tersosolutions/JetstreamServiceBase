using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AE = TersoSolutions.Jetstream.SDK.Application.Messages.AggregateEvent;
using CCE = TersoSolutions.Jetstream.SDK.Application.Messages.CommandCompletionEvent;
using CQE = TersoSolutions.Jetstream.SDK.Application.Messages.CommandQueuedEvent;
using HE = TersoSolutions.Jetstream.SDK.Application.Messages.HeartbeatEvent;
using LEE = TersoSolutions.Jetstream.SDK.Application.Messages.LogEntryEvent;
using OE = TersoSolutions.Jetstream.SDK.Application.Messages.ObjectEvent;
using SRE = TersoSolutions.Jetstream.SDK.Application.Messages.SensorReadingEvent;

namespace TersoSolutions.Jetstream.ServiceBase.Tests
{
    [TestClass]
    public class WindowUnitTest 
    {
        public static string LogicalDeviceId;

        public WindowUnitTest()
        {
            if (String.IsNullOrEmpty(LogicalDeviceId))
            {
                LogicalDeviceId = JetstreamConfiguration.GetLogicalDeviceId();
            }

        }
        
        # region Events methods
        /// <summary>
        /// method is used to test AggregateEvent
        /// </summary>
        [TestMethod]
        public void AggregateEventTest()
        {
            try
            {
                AE.JetstreamHeader agHeader = new AE.JetstreamHeader();
                AE.Jetstream aggregateEvent = new AE.Jetstream();
                AE.JetstreamAggregateEvent evnt = new AE.JetstreamAggregateEvent();
                AE.JetstreamAggregateEventDeviceExtensionList agEvntDevList = new AE.JetstreamAggregateEventDeviceExtensionList();
                AE.JetstreamAggregateEventActionEPCLists agEvntActEpcList = new AE.JetstreamAggregateEventActionEPCLists();

                AE.JetstreamAggregateEventDeviceExtensionListDeviceExtension[] deExtn = new AE.JetstreamAggregateEventDeviceExtensionListDeviceExtension[1];
                deExtn[0] = new AE.JetstreamAggregateEventDeviceExtensionListDeviceExtension { Name = "WindName" + LogicalDeviceId, Value = "Value" + LogicalDeviceId };

                AE.JetstreamAggregateEventActionEPCListsActionEPCListEPC[] epclst = new AE.JetstreamAggregateEventActionEPCListsActionEPCListEPC[1];
                epclst[0] = new AE.JetstreamAggregateEventActionEPCListsActionEPCListEPC { Value = "Value" + LogicalDeviceId };

                // System.Xml.XmlAttribute[] AnyAttr =new Xml.XmlAttribute();
                AE.JetstreamAggregateEventActionEPCListsActionEPCListType typ;
                typ = AE.JetstreamAggregateEventActionEPCListsActionEPCListType.ADD;

                AE.JetstreamAggregateEventActionEPCListsActionEPCList[] epcActn = new AE.JetstreamAggregateEventActionEPCListsActionEPCList[1];
                epcActn[0] = new AE.JetstreamAggregateEventActionEPCListsActionEPCList { EPC = epclst, Type = typ };


                # region Window Service Code

                agHeader.LogicalDeviceId = LogicalDeviceId;
                agHeader.EventTime = DateTime.Now;
                aggregateEvent.Header = agHeader;
                aggregateEvent.AggregateEvent = evnt;
                aggregateEvent.AggregateEvent.DeviceExtensionList = agEvntDevList;
                aggregateEvent.AggregateEvent.ActionEPCLists = agEvntActEpcList;
                aggregateEvent.AggregateEvent.DeviceExtensionList.DeviceExtension = deExtn;
                aggregateEvent.AggregateEvent.ActionEPCLists.ActionEPCList = epcActn;

                string passRfid = "";
                string logicalDeviceId = aggregateEvent.Header.LogicalDeviceId;
                DateTime receiveDate = aggregateEvent.Header.EventTime;

                // collect and record the PassRfid value associated with this event
                if (aggregateEvent.AggregateEvent.DeviceExtensionList.DeviceExtension != null)
                {
                    foreach (var deviceExtension in aggregateEvent.AggregateEvent.DeviceExtensionList.DeviceExtension)
                    {
                        if (deviceExtension.Name == "PassRfid") passRfid = deviceExtension.Value;
                    }
                }

                // process each of the EPC actions
                if (aggregateEvent.AggregateEvent.ActionEPCLists.ActionEPCList != null)
                {
                    foreach (var epcList in aggregateEvent.AggregateEvent.ActionEPCLists.ActionEPCList)
                    {
                        if (epcList.Type == AE.JetstreamAggregateEventActionEPCListsActionEPCListType.ADD)
                        {
                            // collect and record each of the ADD operations 
                            foreach (var epc in epcList.EPC)
                            {
                                //JetstreamEventRepository.RecordInventory(logicalDeviceId, epc.Value, "In", receiveDate, passRfid);
                            }
                        }
                        else
                        {
                            // collect and record each of the REMOVE operations
                            foreach (var epc in epcList.EPC)
                            {
                                //JetstreamEventRepository.RecordInventory(logicalDeviceId, epc.Value, "Out", receiveDate, passRfid);
                            }
                        }
                    }
                }

                #endregion
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.ToString());
            }
        }

        /// <summary>
        /// method is used to test HeartbeatEvent
        /// </summary>
        [TestMethod]
        public void HeartbeatEventTest()
        {
            try
            {
                HE.JetstreamHeader hbHeader = new HE.JetstreamHeader();
                HE.Jetstream heartbeatEvent = new HE.Jetstream();

                //Assign value
                hbHeader.LogicalDeviceId = LogicalDeviceId;
                hbHeader.ReceivedTime = DateTime.Now;
                heartbeatEvent.Header = hbHeader;
                // record the heartbeat event
                //JetstreamEventRepository.RecordHeartbeat(heartbeatEvent);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.ToString());
            }
        }

        /// <summary>
        /// method is used to test CommandQueuedEvent
        /// </summary>
        [TestMethod]
        public void CommandQueuedEventTest()
        {
            try
            {
                CQE.JetstreamHeader cqHeader = new CQE.JetstreamHeader();
                CQE.Jetstream commandQueuedEvent = new CQE.Jetstream();
                CQE.JetstreamCommandQueuedEvent cmdQ = new CQE.JetstreamCommandQueuedEvent();
                //Assign value
                commandQueuedEvent.Header = cqHeader;
                commandQueuedEvent.Header.LogicalDeviceId = LogicalDeviceId;
                commandQueuedEvent.CommandQueuedEvent = cmdQ;
                commandQueuedEvent.CommandQueuedEvent.CommandId = Convert.ToString(Guid.NewGuid());
                commandQueuedEvent.CommandQueuedEvent.CommandName = "WindowTestCommand";
                commandQueuedEvent.CommandQueuedEvent.UserName = "FarhanTestWindow";

                // record the heartbeat event
               //JetstreamEventRepository.RecordCommandQueuedEvent(commandQueuedEvent.CommandQueuedEvent.CommandId, commandQueuedEvent.CommandQueuedEvent.CommandName, commandQueuedEvent.CommandQueuedEvent.UserName);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.ToString());
            }
        }

        /// <summary>
        /// method is used to test CommandCompletionEvent
        /// </summary>
        [TestMethod]
        public void CommandCompletionEventTest()
        {
            CCE.JetstreamHeader cqHeader = new CCE.JetstreamHeader();
            CCE.Jetstream commandCompletionEvent = new CCE.Jetstream();
            CCE.JetstreamCommandCompletionEvent ccEvent = new CCE.JetstreamCommandCompletionEvent();
            ccEvent.CommandId = Convert.ToString(Guid.NewGuid());
            CCE.JetstreamCommandCompletionEventDeviceExtensionList ccDevExtEvent = new CCE.JetstreamCommandCompletionEventDeviceExtensionList();
            CCE.JetstreamCommandCompletionEventExceptionList ccEvntExpLst = new CCE.JetstreamCommandCompletionEventExceptionList();

            CCE.JetstreamCommandCompletionEventDeviceExtensionListDeviceExtension[] devEpt = new CCE.JetstreamCommandCompletionEventDeviceExtensionListDeviceExtension[1];
            devEpt[0] = new CCE.JetstreamCommandCompletionEventDeviceExtensionListDeviceExtension { Name = "NameWindCCEvt" + LogicalDeviceId, Value = "ValCC" + LogicalDeviceId };

            CCE.JetstreamCommandCompletionEventOutputParameterList ccCompoutParamLst = new CCE.JetstreamCommandCompletionEventOutputParameterList();

            CCE.JetstreamCommandCompletionEventExceptionListException[] ccEvtExplstExp = new CCE.JetstreamCommandCompletionEventExceptionListException[1];
            ccEvtExplstExp[0] = new CCE.JetstreamCommandCompletionEventExceptionListException { Message = "WindMsg" + LogicalDeviceId, Name = "Name" + LogicalDeviceId };

            CCE.JetstreamCommandCompletionEventOutputParameterListOutputParameter[] outparam = new CCE.JetstreamCommandCompletionEventOutputParameterListOutputParameter[1];

            outparam[0] = new CCE.JetstreamCommandCompletionEventOutputParameterListOutputParameter { Name = "WindName" + LogicalDeviceId, Value = "Val" + LogicalDeviceId };
            commandCompletionEvent.CommandCompletionEvent = ccEvent;
            commandCompletionEvent.CommandCompletionEvent.DeviceExtensionList = ccDevExtEvent;
            commandCompletionEvent.CommandCompletionEvent.DeviceExtensionList.DeviceExtension = devEpt;

            commandCompletionEvent.CommandCompletionEvent.ExceptionList = ccEvntExpLst;
            commandCompletionEvent.CommandCompletionEvent.ExceptionList.Exception = ccEvtExplstExp;

            commandCompletionEvent.CommandCompletionEvent.OutputParameterList = ccCompoutParamLst;
            commandCompletionEvent.CommandCompletionEvent.OutputParameterList.OutputParameter = outparam;

            # region window service

            string commandId = commandCompletionEvent.CommandCompletionEvent.CommandId;

            if (commandCompletionEvent.CommandCompletionEvent.DeviceExtensionList.DeviceExtension != null)
            {
                // collect and record each of the device extension name/value pairs
                foreach (var deviceExtension in commandCompletionEvent.CommandCompletionEvent.DeviceExtensionList.DeviceExtension)
                {
                    //JetstreamEventRepository.RecordCommandCompletionEvent(commandId, "DeviceExtension", deviceExtension.Name, deviceExtension.Value);
                }
            }

            if (commandCompletionEvent.CommandCompletionEvent.ExceptionList.Exception != null)
            {
                // collect and record each of the exception name/value pairs
                foreach (var exception in commandCompletionEvent.CommandCompletionEvent.ExceptionList.Exception)
                {
                    //JetstreamEventRepository.RecordCommandCompletionEvent(commandId, "Exception", exception.Name, exception.Message);
                }
            }

            if (commandCompletionEvent.CommandCompletionEvent.OutputParameterList.OutputParameter != null)
            {
                // collect and record each of the output parameter name/value pairs
                foreach (var outputParameter in commandCompletionEvent.CommandCompletionEvent.OutputParameterList.OutputParameter)
                {
                    //JetstreamEventRepository.RecordCommandCompletionEvent(CommandId, "OutputParameter", outputParameter.Name, outputParameter.Value);
                }
            }

            #endregion
        }

        /// <summary>
        /// method is used to test LogEntryEvent
        /// </summary>
        [TestMethod]
        public void LogEntryEventTest()
        {
            try
            {
                LEE.JetstreamHeader logHeader = new LEE.JetstreamHeader();
                LEE.Jetstream logEntryEvent = new LEE.Jetstream();
                logHeader.LogicalDeviceId = LogicalDeviceId;

                LEE.JetstreamLogEntryEvent lgEntEvt = new LEE.JetstreamLogEntryEvent();
                LEE.JetstreamLogEntryEventLogEntryList lgEntEvtLst = new LEE.JetstreamLogEntryEventLogEntryList();
                LEE.JetstreamLogEntryEventLogEntryListLogEntryLevel level;
                level = LEE.JetstreamLogEntryEventLogEntryListLogEntryLevel.Trace;

                LEE.JetstreamLogEntryEventLogEntryListLogEntryParameterList paramLst = new LEE.JetstreamLogEntryEventLogEntryListLogEntryParameterList();
                LEE.JetstreamLogEntryEventLogEntryListLogEntryParameterListParameter[] param = new LEE.JetstreamLogEntryEventLogEntryListLogEntryParameterListParameter[1];
                param[0] = new LEE.JetstreamLogEntryEventLogEntryListLogEntryParameterListParameter { Name = "WinName" + LogicalDeviceId, Value = "Val" + LogicalDeviceId };
                paramLst.Parameter = param;

                LEE.JetstreamLogEntryEventLogEntryListLogEntry[] enty = new LEE.JetstreamLogEntryEventLogEntryListLogEntry[1];
                enty[0] = new LEE.JetstreamLogEntryEventLogEntryListLogEntry { Level = level, LogTime = DateTime.Now, Message = "WindMsg" + LogicalDeviceId, Type = "WinType", ParameterList = paramLst };

                logEntryEvent.Header = logHeader;
                logEntryEvent.LogEntryEvent = lgEntEvt;
                logEntryEvent.LogEntryEvent.LogEntryList = lgEntEvtLst;
                logEntryEvent.LogEntryEvent.LogEntryList.LogEntry = enty;

                #region window

                StringBuilder sb = new StringBuilder();

                sb.AppendLine("ProcessLogEntryEvent for Logical Device " + logEntryEvent.Header.LogicalDeviceId);
                foreach (var entry in logEntryEvent.LogEntryEvent.LogEntryList.LogEntry)
                {
                    sb.AppendFormat("Level: {0}\r\n", entry.Level);
                    sb.AppendFormat("Log Time: {0} {1}\r\n", entry.LogTime.ToLongDateString(), entry.LogTime.ToLongTimeString());
                    sb.AppendFormat("Message: {0}\r\n", entry.Message);
                    sb.AppendFormat("Type: {0}\r\n", entry.Type);
                    if (entry.ParameterList.Parameter != null)
                    {
                        // collect the name/value pairs associated with the log entry event
                        foreach (var parm in entry.ParameterList.Parameter)
                        {
                            sb.AppendFormat("Parameter: {0} = {1}\r\n", parm.Name, parm.Value);
                        }
                    }
                }

                #endregion
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.ToString());
            }

        }

        /// <summary>
        /// method is used test objectevent
        /// </summary>
        [TestMethod]
        public void ObjectEventTest()
        {
            try
            {
                OE.JetstreamHeader objHeader = new OE.JetstreamHeader();
                OE.Jetstream objectEvent = new OE.Jetstream();
                OE.JetstreamObjectEventActionEPCList objEvntActList = new OE.JetstreamObjectEventActionEPCList();
                OE.JetstreamObjectEventActionEPCListEPC[] objEvntActListEpc = new OE.JetstreamObjectEventActionEPCListEPC[1];
                OE.JetstreamObjectEvent evnt = new OE.JetstreamObjectEvent();

                //
                objEvntActListEpc[0] = new OE.JetstreamObjectEventActionEPCListEPC { Value = "Value" + LogicalDeviceId };
                objHeader.LogicalDeviceId = LogicalDeviceId;
                objHeader.EventTime = DateTime.Now;

                objectEvent.Header = objHeader;

                objectEvent.ObjectEvent = evnt;

                objectEvent.ObjectEvent.ActionEPCList = objEvntActList;

                objectEvent.ObjectEvent.ActionEPCList.EPC = objEvntActListEpc;

                if (objectEvent.ObjectEvent.ActionEPCList != null)
                {
                    if (objectEvent.ObjectEvent.ActionEPCList.EPC != null)
                    {
                        // collect and record the baseline reading
                        foreach (var epc in objectEvent.ObjectEvent.ActionEPCList.EPC)
                        {
                            //JetstreamEventRepository.RecordInventory(objectEvent.Header.LogicalDeviceId, epc.Value, "In", objectEvent.Header.EventTime, "BASELINE");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.ToString());
            }
        }

        /// <summary>
        /// method is used to test SensorReadingEvent
        /// </summary>
        [TestMethod]
        public void SensorReadingEventTest()
        {
            try
            {
                SRE.JetstreamHeader srHeader = new SRE.JetstreamHeader();
                SRE.Jetstream sensorReadingEvent = new SRE.Jetstream();
                SRE.JetstreamSensorReadingEvent srEvent = new SRE.JetstreamSensorReadingEvent();
                SRE.JetstreamSensorReadingEventReadingList srEvtlist = new SRE.JetstreamSensorReadingEventReadingList();
                SRE.JetstreamSensorReadingEventReadingListReading[] SrReading = new SRE.JetstreamSensorReadingEventReadingListReading[1];
                SrReading[0] = new SRE.JetstreamSensorReadingEventReadingListReading { Name = "WinTestTemprature A", Value = "28", ReadingTime = DateTime.Now };
                srHeader.LogicalDeviceId = LogicalDeviceId;
                sensorReadingEvent.Header = srHeader;
                sensorReadingEvent.SensorReadingEvent = srEvent;
                sensorReadingEvent.SensorReadingEvent.ReadingList = srEvtlist;
                sensorReadingEvent.SensorReadingEvent.ReadingList.Reading = SrReading;
                if (sensorReadingEvent.SensorReadingEvent.ReadingList.Reading != null)
                {
                    // collect and record the sensor reading name, value and read time
                    foreach (var reading in sensorReadingEvent.SensorReadingEvent.ReadingList.Reading)
                    {
                        //JetstreamEventRepository.RecordSensorReading(sensorReadingEvent.Header.LogicalDeviceId, reading.Name, reading.Value, reading.ReadingTime);
                    }
                }
                
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.ToString());
            }

        }

        #endregion

    }
}
