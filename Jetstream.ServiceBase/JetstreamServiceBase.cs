﻿/*
     Copyright 2016 Terso Solutions, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Threading;
using TersoSolutions.Jetstream.SDK.Application.Messages;
using TersoSolutions.Jetstream.SDK.Application.Model;
using AE = TersoSolutions.Jetstream.SDK.Application.Messages.AggregateEvent;
using CCE = TersoSolutions.Jetstream.SDK.Application.Messages.CommandCompletionEvent;
using CQE = TersoSolutions.Jetstream.SDK.Application.Messages.CommandQueuedEvent;
using DFE = TersoSolutions.Jetstream.SDK.Application.Messages.DeviceFailureEvent;
using DRE = TersoSolutions.Jetstream.SDK.Application.Messages.DeviceRestoreEvent;
using HE = TersoSolutions.Jetstream.SDK.Application.Messages.HeartbeatEvent;
using LDAE = TersoSolutions.Jetstream.SDK.Application.Messages.LogicalDeviceAddedEvent;
using LDRE = TersoSolutions.Jetstream.SDK.Application.Messages.LogicalDeviceRemovedEvent;
using LEE = TersoSolutions.Jetstream.SDK.Application.Messages.LogEntryEvent;
using OE = TersoSolutions.Jetstream.SDK.Application.Messages.ObjectEvent;
using SRE = TersoSolutions.Jetstream.SDK.Application.Messages.SensorReadingEvent;

namespace TersoSolutions.Jetstream.ServiceBase
{
    /// <summary>
    /// An abstract windows service class that pops messages from Jetstream.
    /// </summary>
    public class JetstreamServiceBase : System.ServiceProcess.ServiceBase
    {

        #region Data

        private readonly Object _newWindowLock = new Object();
        private Timer _windowTimer;
        private readonly Object _windowLock = new Object();
        private CancellationTokenSource _cts;
        private readonly Object _setLock = new Object();
        private readonly SortedSet<JetstreamEvent> _set = new SortedSet<JetstreamEvent>(new JetstreamEventTimeStampComparer()); // use a Red-Black tree for the producer-consumer data store
        private volatile bool _isWindowing;
        private event EventHandler<NewWindowEventArgs> NewWindow;
        private const string _eventLogSource = "JetstreamServiceBase";

        #endregion

        #region Properties

        /// <summary>
        /// The TimeSpan that all results Jetstream Ground are windowed 
        /// and sorted to before firing the NewWindow event.
        /// </summary>
        protected TimeSpan MessageCheckWindow
        {
            get
            {
                TimeSpan result;
                if (ConfigurationManager.AppSettings["MessageCheckWindow"] == null || !TimeSpan.TryParse(ConfigurationManager.AppSettings["MessageCheckWindow"], out result))
                {
                    return new TimeSpan(0, 1, 0);
                }

                if (result.TotalDays <= 1 && result.TotalSeconds >= 1)
                {
                    return result;
                }

                if (result.TotalDays > 1)
                {
                    return new TimeSpan(1, 0, 0, 0);
                }

                // default to a 1 minute window
                return new TimeSpan(0, 0, 1);
            }
        }

        /// <summary>
        /// The Jetstream Ground subscribe url.
        /// </summary>
        /// 
        protected string JetstreamUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["JetstreamUrl"];
            }
        }

        /// <summary>
        /// An access key used to authenticate to Jetstream Ground.
        /// </summary>
        /// 
        protected string UserAccessKey
        {
            get
            {
                return ConfigurationManager.AppSettings["UserAccessKey"];
            }
        }

        /// <summary>
        /// Indicates if this Events service is currently polling/windowing events.
        /// </summary>
        public bool IsWindowing
        {
            get
            {
                return _isWindowing;
            }
            private set
            {
                _isWindowing = value;
            }
        }

        #endregion

        #region Service Events

        /// <summary>
        /// Dispose of objects that need it here.
        /// </summary>
        /// <param name="disposing">Whether or not disposing is going on.</param>
        protected override void Dispose(bool disposing)
        {
            _cts.Dispose();
            _windowTimer.Dispose();
            base.Dispose(disposing);
        }

        /// <summary>
        /// override the OnStart for the windows service to hook the NewWindow event.
        /// </summary>
        /// <param name="args"></param>
        /// <exception cref="ConfigurationErrorsException">There is no UserAccesskey in the appSettings</exception>
        /// <exception cref="ConfigurationErrorsException">There is no JetstreamUrl in the appSettings</exception>
        protected override void OnStart(string[] args)
        {
            // validate that the service is configured correctly
            if (String.IsNullOrEmpty(UserAccessKey)) throw new ConfigurationErrorsException("There is no UserAccesskey in the appSettings");
            if (String.IsNullOrEmpty(JetstreamUrl)) throw new ConfigurationErrorsException("There is no JetstreamUrl in the appSettings");

            // start all of the background processing
            StartProcesses();

            // hook an event handler to the NewWindow event
            NewWindow += JetstreamServiceNewWindowHandler;

            // call the base
            base.OnStart(args);
        }

        /// <summary>
        /// Override of the WindowsService Stop.
        /// </summary>
        protected override void OnStop()
        {
            // stop all of the background processing
            StopProcesses();

            base.OnStop();
        }

        /// <summary>
        /// Override of the Pause.
        /// </summary>
        protected override void OnPause()
        {
            // stop all of the background processing
            StopProcesses();

            base.OnPause();
        }

        /// <summary>
        /// Override of the Continue.
        /// </summary>
        protected override void OnContinue()
        {
            // start all of the background processing
            StartProcesses();

            base.OnContinue();
        }

        /// <summary>
        /// Override of the Power Event.
        /// </summary>
        /// <param name="powerStatus"></param>
        /// <returns></returns>
        protected override bool OnPowerEvent(PowerBroadcastStatus powerStatus)
        {
            if (powerStatus == PowerBroadcastStatus.BatteryLow || powerStatus == PowerBroadcastStatus.Suspend)
            {
                // stop the background processing
                StopProcesses();
            }

            if (powerStatus == PowerBroadcastStatus.ResumeAutomatic || powerStatus == PowerBroadcastStatus.ResumeCritical || powerStatus == PowerBroadcastStatus.ResumeSuspend)
            {
                // start the background processing
                StartProcesses();
            }
            return base.OnPowerEvent(powerStatus);
        }

        /// <summary>
        /// Override of the Shutdown.
        /// </summary>
        protected override void OnShutdown()
        {
            // stop the background processing
            StopProcesses();

            base.OnShutdown();
        }

        #endregion

        #region Methods

        /// <summary>
        /// Task for Receiving messages from Jetstream Ground.
        /// </summary>
        private string ReceiveTask(CancellationToken ct)
        {
            try
            {
                JetstreamServiceClient client = new JetstreamServiceClient(JetstreamUrl, UserAccessKey);
                GetEventsRequest request = new GetEventsRequest();
                GetEventsResponse response = client.GetEvents(request);
                string currentBatchId = response.BatchId;
                List<JetstreamEvent> messages = response.Events.ToList();

                // now add the Messages to the data store for the window thread
                if (ct.IsCancellationRequested)
                {
                    return currentBatchId;
                }

                lock (_setLock)
                {
                    foreach (JetstreamEvent t in messages)
                    {
                        if (ct.IsCancellationRequested)
                        {
                            break;
                        }
                        _set.Add(t);
                    }
                }
                return currentBatchId;
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry(_eventLogSource, ex.Message + "\n" + ex.StackTrace, EventLogEntryType.Error);
            }
            return String.Empty;
        }

        /// <summary>
        /// Task for deleting messages from Jetstream.
        /// <param name="batchId">The events batch ID to delete</param>
        /// </summary>
        private void DeleteTask(string batchId)
        {
            try
            {
                JetstreamServiceClient client = new JetstreamServiceClient(JetstreamUrl, UserAccessKey);
                RemoveEventsRequest request = new RemoveEventsRequest { BatchId = batchId };
                client.RemoveEvents(request);
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry(_eventLogSource, ex.Message + "\n" + ex.StackTrace, EventLogEntryType.Error);
            }
        }

        /// <summary>
        /// Callback for windowing the messages ordered by time queued.
        /// </summary>
        /// <param name="state"></param>
        private void WindowCallback(Object state)
        {
            try
            {
                CancellationToken ct = (CancellationToken)state;
                List<JetstreamEvent> messages = new List<JetstreamEvent>();
                string batchId = String.Empty;

                // synchronize the processing of windows
                if (!ct.IsCancellationRequested && Monitor.TryEnter(_windowLock, 1000))
                {
                    try
                    {
                        DateTime windowTime = (DateTime.UtcNow.Subtract(MessageCheckWindow));
                        batchId = ReceiveTask(ct);

                        // ok so all messages have been received and ordered or no more messages can be popped
                        if (!ct.IsCancellationRequested)
                        {
                            // get all messages less than the time windows from the Red-Black tree.
                            lock (_setLock)
                            {
                                messages.AddRange(_set.Where(m => m.EventTime < windowTime));
                                _set.RemoveWhere(m => m.EventTime < windowTime);
                            }

                            // remove duplicates
                            // TODO Find a better way to dedup that doesn't destroy the order of the array
                            IEnumerable<JetstreamEvent> dedupMessages = messages.Distinct(new JetstreamEventEqualityComparer());

                            // distinct doesn't presever order so we need to reorder the window
                            IEnumerable<JetstreamEvent> orderedMessages = dedupMessages.OrderBy(m => m, new JetstreamEventTimeStampComparer());

                            // raise the WindowEvent with the results
                            if (!ct.IsCancellationRequested)
                            {
                                OnNewWindow(orderedMessages);
                            }
                        }

                    }
                    finally
                    {
                        Monitor.Exit(_windowLock);
                    }
                }

                // check if we should delete the messages
                if (!ct.IsCancellationRequested && messages.Count > 0)
                {
                    DeleteTask(batchId);
                }
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry(_eventLogSource, ex.Message + "\n" + ex.StackTrace, EventLogEntryType.Error);
            }
        }

        /// <summary>
        /// Starts all of the background processing.
        /// </summary>
        public void StartProcesses()
        {
            // create a new cancellationtoken source
            _cts = new CancellationTokenSource();

            // start the window timer
            _windowTimer = new Timer(WindowCallback, _cts.Token, TimeSpan.Zero, MessageCheckWindow);
            IsWindowing = true;
        }

        /// <summary>
        /// Cancels all of the background processing.
        /// </summary>
        private void StopProcesses()
        {
            // signal a cancel to the background threads
            _cts.Cancel();

            // give the background threads a bit of time to cancel
            Thread.Sleep(100);

            // now dispose of them
            if (_windowTimer != null)
            {
                _windowTimer.Dispose();
            }

            IsWindowing = false;
        }

        /// <summary>
        /// On* event raising pattern implementation for the NewWindow event.
        /// </summary>
        /// <param name="messages"></param>
        private void OnNewWindow(IEnumerable<JetstreamEvent> messages)
        {
            if (NewWindow != null)
            {
                NewWindow(this, new NewWindowEventArgs(messages));
            }
        }

        /// <summary>
        /// Event handler for the NewWindow event.
        /// </summary>
        /// <param name="sender">Events Service</param>
        /// <param name="e">The NewWindow event args</param>
        private void JetstreamServiceNewWindowHandler(object sender, NewWindowEventArgs e)
        {
            // lock so we process all events in order
            lock (_newWindowLock)
            {
                foreach (JetstreamEvent m in e.Messages)
                {
                    try
                    {
                        // now we can deserialize the XML message into the appropriate message
                        switch (m.EventType.Trim().ToLower())
                        {
                            case "aggregateevent":
                                {
                                    AE.Jetstream message = (AE.Jetstream)m;
                                    ProcessAggregateEvent(message);
                                    break;
                                }
                            case "commandcompletionevent":
                                {
                                    CCE.Jetstream message = (CCE.Jetstream)m;
                                    ProcessCommandCompletionEvent(message);
                                    break;
                                }
                            case "commandqueuedevent":
                                {
                                    CQE.Jetstream message = (CQE.Jetstream)m;
                                    ProcessCommandQueuedEvent(message);
                                    break;
                                }
                            case "devicefailureevent":
                                {
                                    DFE.Jetstream message = (DFE.Jetstream)m;
                                    ProcessDeviceFailureEvent(message);
                                    break;
                                }
                            case "devicerestoreevent":
                                {
                                    DRE.Jetstream message = (DRE.Jetstream)m;
                                    ProcessDeviceRestoreEvent(message);
                                    break;
                                }
                            case "heartbeatevent":
                                {
                                    HE.Jetstream message = (HE.Jetstream)m;
                                    ProcessHeartbeatEvent(message);
                                    break;
                                }
                            case "logentryevent":
                                {
                                    LEE.Jetstream message = (LEE.Jetstream)m;
                                    ProcessLogEntryEvent(message);
                                    break;
                                }
                            case "logicaldeviceaddedevent":
                                {
                                    LDAE.Jetstream message = (LDAE.Jetstream)m;
                                    ProcessLogicalDeviceAddedEvent(message);
                                    break;
                                }
                            case "logicaldeviceremovedevent":
                                {
                                    LDRE.Jetstream message = (LDRE.Jetstream)m;
                                    ProcessLogicalDeviceRemovedEvent(message);
                                    break;
                                }
                            case "objectevent":
                                {
                                    OE.Jetstream message = (OE.Jetstream)m;
                                    ProcessObjectEvent(message);
                                    break;
                                }
                            case "sensorreadingevent":
                                {
                                    SRE.Jetstream message = (SRE.Jetstream)m;
                                    ProcessSensorReadingEvent(message);
                                    break;
                                }
                            default:
                                {
                                    ProcessUnknownMessage(m);
                                    break;
                                }

                        }
                    }
                    catch (Exception ex)
                    {
                        EventLog.WriteEntry(_eventLogSource, ex.Message + "\n" + ex.StackTrace, EventLogEntryType.Error);
                    }
                }
            }
        }

        /// <summary>
        /// Method for processing a new <paramref name="aggregateEvent"/>
        /// </summary>
        /// <param name="aggregateEvent">
        /// Deserialized AggregateEvent message in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessAggregateEvent(AE.Jetstream aggregateEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="commandCompletionEvent"/>
        /// </summary>
        /// <param name="commandCompletionEvent">
        /// Deserialized CommandCompletionEvent message in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessCommandCompletionEvent(CCE.Jetstream commandCompletionEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="commandQueuedEvent"/>
        /// </summary>
        /// <param name="commandQueuedEvent">
        /// Deserialized CommandQueuedEvent message in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessCommandQueuedEvent(CQE.Jetstream commandQueuedEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="deviceFailureEvent"/>
        /// </summary>
        /// <param name="deviceFailureEvent">
        /// Deserialized DeviceFailureEvent message in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessDeviceFailureEvent(DFE.Jetstream deviceFailureEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="deviceRestoreEvent"/>
        /// </summary>
        /// <param name="deviceRestoreEvent">
        /// Deserialized DeviceRestoreEvent message in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessDeviceRestoreEvent(DRE.Jetstream deviceRestoreEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="heartbeatEvent"/>
        /// </summary>
        /// <param name="heartbeatEvent">
        /// Deserialized HeartbeatEvent message in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessHeartbeatEvent(HE.Jetstream heartbeatEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="logEntryEvent"/>
        /// </summary>
        /// <param name="logEntryEvent">
        /// Deserialized LogEntryEvent message in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessLogEntryEvent(LEE.Jetstream logEntryEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="logicalDeviceAddedEvent"/>
        /// </summary>
        /// <param name="logicalDeviceAddedEvent">
        /// Deserialized LogicalDeviceAddedEvent in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessLogicalDeviceAddedEvent(LDAE.Jetstream logicalDeviceAddedEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="logicalDeviceRemovedEvent"/>
        /// </summary>
        /// <param name="logicalDeviceRemovedEvent">
        /// Deserialized LogicalDeviceRemovedEvent in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessLogicalDeviceRemovedEvent(LDRE.Jetstream logicalDeviceRemovedEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="objectEvent"/>
        /// </summary>
        /// <param name="objectEvent">
        /// Deserialized ObjectEvent in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessObjectEvent(OE.Jetstream objectEvent) { }

        /// <summary>
        /// Method for processing a new <paramref name="sensorReadingEvent"/>
        /// </summary>
        /// <param name="sensorReadingEvent">
        /// Deserialized SensorReadingEvent in the xsd.exe object model.
        /// </param>
        protected virtual void ProcessSensorReadingEvent(SRE.Jetstream sensorReadingEvent) { }

        /// <summary>
        /// Method for processing unknown messages.
        /// </summary>
        /// <param name="message">
        /// The unknown message body
        /// </param>
        protected virtual void ProcessUnknownMessage(JetstreamEvent message) { }

        #endregion

    }
}
