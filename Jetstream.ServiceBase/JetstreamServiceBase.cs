using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TersoSolutions.Jetstream.Sdk;
using TersoSolutions.Jetstream.Sdk.Objects;
using TersoSolutions.Jetstream.Sdk.Objects.Events;
using TersoSolutions.Jetstream.ServiceBase.OldDto;

namespace TersoSolutions.Jetstream.ServiceBase
{
    /// <summary>
    /// An abstract service class that pops events from Jetstream
    /// </summary>
    public abstract class JetstreamServiceBase : IHostedService, IDisposable
    {

        #region Data

        private readonly object _newWindowLock = new object();
        private readonly object _windowLock = new object();
        private readonly object _setLock = new object();

        private readonly IDisposable _optionsChangeToken;
        private readonly ILogger _logger;
        private readonly SortedSet<EventDto> _sortedSet = new SortedSet<EventDto>(new EventComparer());

        private event EventHandler<NewWindowEventArgs> NewWindow;
        private Timer _windowTimer;
        private CancellationTokenSource _cts;
        private TimeSpan _messageCheckWindow;
        private Uri _jetstreamUrl;
        private string _userAccessKey;

        private volatile bool _isWindowing;
        private int? _getEventsLimit;

        private const string CategoryBaseService = "JetstreamServiceBase";

        #endregion

        #region Properties

        /// <summary>
        /// Indicates if currently polling/windowing events
        /// </summary>
        public bool IsWindowing
        {
            get => _isWindowing;
            private set => _isWindowing = value;
        }

        #endregion

        /// <summary>
        /// Constructor for the service base
        /// </summary>
        /// <param name="loggerFactory">LoggerFactory object</param>
        /// <param name="options">Jetstream options</param>
        /// <exception cref="T:System.ArgumentNullException">Logging factory cannot be null <paramref name="loggerFactory"/></exception>
        /// <exception cref="T:System.ArgumentNullException">Options object cannot be null <paramref name="options"/></exception>
        protected JetstreamServiceBase(ILoggerFactory loggerFactory, IOptionsMonitor<JetstreamServiceOptions> options)
        {
            // Validate input
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory), "Logging factory cannot be null");
            if (options == null) throw new ArgumentNullException(nameof(options), "Options object cannot be null");

            // Handles Updates
            _optionsChangeToken = options.OnChange(UpdateOptions);

            // Assign settings
            _logger = loggerFactory.CreateLogger(CategoryBaseService);
            UpdateOptions(options.CurrentValue);
        }

        /// <summary>
        /// Handles the updating of options
        /// </summary>
        /// <param name="options">Jetstream options object</param>
        private void UpdateOptions(JetstreamServiceOptions options)
        {
            _jetstreamUrl = options.JetstreamUrl;
            _userAccessKey = options.UserAccessKey;
            _getEventsLimit = options.GetEventsLimit;
            _messageCheckWindow = options.MessageCheckWindow;
        }

        #region Service Events

        /// <summary>
        /// Entry point to start event processing
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual Task StartAsync(CancellationToken cancellationToken)
        {
            // start all of the background processing
            StartProcesses();

            // hook an event handler to the NewWindow event
            NewWindow += JetstreamServiceNewWindowHandler;

            return Task.CompletedTask;
        }

        /// <summary>
        /// Exit point to stop event processing
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual Task StopAsync(CancellationToken cancellationToken)
        {
            // stop all of the background processing
            StopProcesses();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Dispose of necessary objects
        /// </summary>
        public virtual void Dispose()
        {
            _cts?.Dispose();
            _windowTimer?.Dispose();
            _optionsChangeToken?.Dispose();
        }

        #endregion

        #region Jetstream event popping

        /// <summary>
        /// Task for receiving events from Jetstream
        /// </summary>
        /// <param name="ct">Token to cancel processing</param>
        private string ReceiveTask(CancellationToken ct)
        {
            var currentBatchId = string.Empty;

            try
            {
                _logger.LogTrace("Starting to retrieve Jetstream events...", CategoryBaseService);

                // Build the Jetstream client for the access key and get events
                var client = new JetstreamClient(_userAccessKey, _jetstreamUrl);
                var response = client.GetEvents(_getEventsLimit ?? 500);
                currentBatchId = response.BatchId;

                _logger.LogTrace("Got " + response.Count + " events in batch " + currentBatchId, CategoryBaseService);

                if (ct.IsCancellationRequested) return currentBatchId;

                // Now add the events to the data store for the window thread
                lock (_setLock)
                {
                    // Step through the received events and add them to processing set
                    foreach (EventDto t in response.Events)
                    {
                        if (ct.IsCancellationRequested)
                        {
                            break;
                        }
                        _sortedSet.Add(t);
                    }
                }
                return currentBatchId;
            }
            catch (JetstreamException jsException)
            {
                // Log and pass exception to event handler for any processing
                _logger.LogError(JsonConvert.SerializeObject(jsException), CategoryBaseService);
                HandleJetstreamGetEventsException(jsException, currentBatchId);
            }

            return string.Empty;
        }

        /// <summary>
        /// Task for deleting events from Jetstream
        /// </summary>
        /// <param name="batchId">The events batch ID to delete</param>
        private void DeleteTask(string batchId)
        {
            try
            {
                _logger.LogTrace("Deleting batch " + batchId, CategoryBaseService);

                var client = new JetstreamClient(_userAccessKey, _jetstreamUrl);
                client.DeleteEvents(new DeleteEventsDto { BatchId = batchId });
            }
            catch (JetstreamException jsException)
            {
                _logger.LogError(JsonConvert.SerializeObject(jsException), CategoryBaseService);
                HandleJetstreamDeleteEventsException(jsException, batchId);
            }
        }

        /// <summary>
        /// Callback for windowing the events ordered by time queued
        /// </summary>
        /// <param name="state"></param>
        private void WindowCallback(object state)
        {
            try
            {
                _logger.LogTrace("Window callback execution starting...", CategoryBaseService);

                var ct = (CancellationToken)state;
                var events = new List<EventDto>();

                // Synchronize the processing of windows
                if (ct.IsCancellationRequested || !Monitor.TryEnter(_windowLock, 1000)) return;

                _logger.LogTrace("Getting events for application...", CategoryBaseService);

                try
                {
                    var batchId = ReceiveTask(ct);

                    // All events have been received and ordered or no more events are available
                    if (!ct.IsCancellationRequested)
                    {
                        // Get all events
                        lock (_setLock)
                        {
                            _logger.LogTrace("Moving events in set", CategoryBaseService);

                            events.AddRange(_sortedSet);
                            _sortedSet.Clear();
                        }

                        // Group the events by event id and grab one from each group
                        // effectively removing any chance of a duplicate event. GroupBy
                        // preserves the original order in the list
                        var orderedEvents = events.GroupBy(x => x.EventId).Select(group => group.First());

                        // raise the WindowEvent with the results
                        if (!ct.IsCancellationRequested)
                        {
                            _logger.LogTrace("Sending events to be processed", CategoryBaseService);
                            OnNewWindow(orderedEvents);
                        }
                    }

                    // Check if events can be deleted
                    if (!ct.IsCancellationRequested && events.Any())
                    {
                        DeleteTask(batchId);
                    }
                }
                finally
                {
                    Monitor.Exit(_windowLock);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, CategoryBaseService);
            }
        }

        /// <summary>
        /// Starts all of the background processing
        /// </summary>
        private void StartProcesses()
        {
            // create a new cancellation token source
            _cts = new CancellationTokenSource();

            // start the window timer
            _windowTimer = new Timer(WindowCallback, _cts.Token, new TimeSpan(0, 1, 0), _messageCheckWindow);
            IsWindowing = true;
        }

        /// <summary>
        /// Cancels all of the background processing
        /// </summary>
        private void StopProcesses()
        {
            // signal a cancel to the background threads
            _cts?.Cancel();

            // now dispose of them
            _windowTimer?.Dispose();
            _cts?.Dispose();

            IsWindowing = false;
        }

        /// <summary>
        /// On event raising pattern implementation for the NewWindow event
        /// </summary>
        /// <param name="events">Collection of events to begin processing</param>
        private void OnNewWindow(IEnumerable<EventDto> events)
        {
            NewWindow?.Invoke(this, new NewWindowEventArgs(events));
        }

        /// <summary>
        /// Event handler for the NewWindow event
        /// </summary>
        /// <param name="sender">Events Service</param>
        /// <param name="e">The NewWindow event object</param>
        private void JetstreamServiceNewWindowHandler(object sender, NewWindowEventArgs e)
        {
            // lock so we process all events in order
            lock (_newWindowLock)
            {
                // Step through the events in the list
                foreach (var m in e.Events)
                {
                    try
                    {
                        // Deserialize the JSON string into the appropriate event
                        switch (m.Type.Trim().ToLower())
                        {
                            case "aggregateevent":
                                {
                                    // Cast and process aggregate event
                                    var message = (AggregateEventDto)m;
                                    ProcessAggregateEvent(message);
                                    break;
                                }
                            case "commandcompletionevent":
                                {
                                    // Cast and process command completion event
                                    var message = (CommandCompletionEventDto)m;
                                    ProcessCommandCompletionEvent(message);
                                    break;
                                }
                            case "commandqueuedevent":
                                {
                                    // Cast and process command queued event
                                    var message = (CommandQueuedEventDto)m;
                                    ProcessCommandQueuedEvent(message);
                                    break;
                                }
                            case "heartbeatevent":
                                {
                                    // Cast and process heartbeat event
                                    var message = (HeartbeatEventDto)m;
                                    ProcessHeartbeatEvent(message);
                                    break;
                                }
                            case "logentryevent":
                                {
                                    // Cast and process log entry event
                                    var message = (LogEntryEventDto)m;
                                    ProcessLogEntryEvent(message);
                                    break;
                                }
                            case "logicaldeviceaddedevent":
                                {
                                    // v2 and under event only
                                    var json = JsonConvert.SerializeObject(m);
                                    var message = JsonConvert.DeserializeObject<LogicalDeviceAddedEventDto>(json);
                                    ProcessLogicalDeviceAddedEvent(message);
                                    break;
                                }
                            case "deviceaddedevent":
                                {
                                    // Cast and process device added event
                                    var message = (DeviceAddedEventDto)m;
                                    ProcessDeviceAddedEvent(message);
                                    break;
                                }
                            case "logicaldeviceremovedevent":
                                {
                                    // v2 and under event only
                                    var json = JsonConvert.SerializeObject(m);
                                    var message = JsonConvert.DeserializeObject<LogicalDeviceRemovedEventDto>(json);
                                    ProcessLogicalDeviceRemovedEvent(message);
                                    break;
                                }
                            case "deviceremovedevent":
                                {
                                    // Cast and process device removed event
                                    var message = (DeviceRemovedEventDto)m;
                                    ProcessDeviceRemovedEvent(message);
                                    break;
                                }
                            case "devicemodifiedevent":
                                {
                                    // Cast and process device modified event
                                    var message = (DeviceModifiedEventDto)m;
                                    ProcessDeviceModifiedEvent(message);
                                    break;
                                }
                            case "objectevent":
                                {
                                    // Cast and process object event
                                    var message = (ObjectEventDto)m;
                                    ProcessObjectEvent(message);
                                    break;
                                }
                            case "sensorreadingevent":
                                {
                                    // Cast and process sensor reading event
                                    var message = (SensorReadingEventDto)m;
                                    ProcessSensorReadingEvent(message);
                                    break;
                                }
                            case "aliasaddedevent":
                                {
                                    // Cast and process alias added event
                                    var message = (AliasAddedEventDto)m;
                                    ProcessAliasAddedEvent(message);
                                    break;
                                }
                            case "aliasremovedevent":
                                {
                                    // Cast and process alias removed event
                                    var message = (AliasRemovedEventDto)m;
                                    ProcessAliasRemovedEvent(message);
                                    break;
                                }
                            case "aliasmodifiedevent":
                                {
                                    // Case and process alias modified event
                                    var message = (AliasModifiedEventDto)m;
                                    ProcessAliasModifiedEvent(message);
                                    break;
                                }
                            case "devicecredentialsaddedevent":
                                {
                                    // Cast and process device credentials added event
                                    var message = (DeviceCredentialsAddedEventDto)m;
                                    ProcessDeviceCredentialsAddedEvent(message);
                                    break;
                                }
                            case "devicecredentialsremovedevent":
                                {
                                    // Cast and process device credentials removed event
                                    var message = (DeviceCredentialsRemovedEventDto)m;
                                    ProcessDeviceCredentialsRemovedEvent(message);
                                    break;
                                }
                            case "devicecredentialsmodifiedevent":
                                {
                                    // Cast and process device credentials modified event
                                    var message = (DeviceCredentialsModifiedEventDto)m;
                                    ProcessDeviceCredentialsModifiedEvent(message);
                                    break;
                                }
                            case "businessprocessevent":
                                {
                                    // Cast and process business process event
                                    var message = (BusinessProcessEventDto)m;
                                    ProcessBusinessProcessEvent(message);
                                    break;
                                }
                            default:
                                {
                                    // Process unknown event
                                    ProcessUnknownMessage(m);
                                    break;
                                }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, CategoryBaseService);
                    }
                }
            }
        }

        #endregion

        #region Jetstream exception handlers

        /// <summary>
        /// Method to do some extra handling when an exception occurs on a Get Events
        /// </summary>
        /// <param name="jsException">Jetstream exception</param>
        /// <param name="batchId">Event batch ID</param>
        protected abstract void HandleJetstreamGetEventsException(JetstreamException jsException, string batchId);

        /// <summary>
        /// Method to do some extra handling when an exception occurs on a Delete Events
        /// </summary>
        /// <param name="jsException">Jetstream exception</param>
        /// <param name="batchId">Event batch ID</param>
        protected abstract void HandleJetstreamDeleteEventsException(JetstreamException jsException, string batchId);

        #endregion

        #region Jetstream event handlers

        /// <summary>
        /// Method for processing a new <paramref name="aggregateEvent"/>
        /// </summary>
        /// <param name="aggregateEvent">Deserialized AggregateEvent object</param>
        protected abstract void ProcessAggregateEvent(AggregateEventDto aggregateEvent);

        /// <summary>
        /// Method for processing a new <paramref name="commandCompletionEvent"/>
        /// </summary>
        /// <param name="commandCompletionEvent">Deserialized CommandCompletionEvent object</param>
        protected abstract void ProcessCommandCompletionEvent(CommandCompletionEventDto commandCompletionEvent);

        /// <summary>
        /// Method for processing a new <paramref name="commandQueuedEvent"/>
        /// </summary>
        /// <param name="commandQueuedEvent">Deserialized CommandQueuedEvent object</param>
        protected abstract void ProcessCommandQueuedEvent(CommandQueuedEventDto commandQueuedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="heartbeatEvent"/>
        /// </summary>
        /// <param name="heartbeatEvent">Deserialized HeartbeatEvent object</param>
        protected abstract void ProcessHeartbeatEvent(HeartbeatEventDto heartbeatEvent);

        /// <summary>
        /// Method for processing a new <paramref name="logEntryEvent"/>
        /// </summary>
        /// <param name="logEntryEvent">Deserialized LogEntryEvent object</param>
        protected abstract void ProcessLogEntryEvent(LogEntryEventDto logEntryEvent);

        /// <summary>
        /// Method for processing a new <paramref name="logicalDeviceAddedEvent"/>
        /// </summary>
        /// <param name="logicalDeviceAddedEvent">Deserialized LogicalDeviceAdded object</param>
        protected abstract void ProcessLogicalDeviceAddedEvent(LogicalDeviceAddedEventDto logicalDeviceAddedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceAddedEventDto"/>
        /// </summary>
        /// <param name="deviceAddedEventDto">Deserialized LogicalDeviceAddedEvent object</param>
        protected abstract void ProcessDeviceAddedEvent(DeviceAddedEventDto deviceAddedEventDto);

        /// <summary>
        /// Method for processing a new <paramref name="logicalDeviceRemovedEvent"/>
        /// </summary>
        /// <param name="logicalDeviceRemovedEvent">Deserialized LogicalDeviceRemovedEvent object</param>
        protected abstract void ProcessLogicalDeviceRemovedEvent(LogicalDeviceRemovedEventDto logicalDeviceRemovedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceRemovedEvent"/>
        /// </summary>
        /// <param name="deviceRemovedEvent">Deserialized LogicalDeviceRemovedEvent</param>
        protected abstract void ProcessDeviceRemovedEvent(DeviceRemovedEventDto deviceRemovedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceModifiedEvent"/>
        /// </summary>
        /// <param name="deviceModifiedEvent">Deserialized DeviceModifiedEvent object</param>
        protected abstract void ProcessDeviceModifiedEvent(DeviceModifiedEventDto deviceModifiedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="objectEvent"/>
        /// </summary>
        /// <param name="objectEvent">Deserialized ObjectEvent object</param>
        protected abstract void ProcessObjectEvent(ObjectEventDto objectEvent);

        /// <summary>
        /// Method for processing a new <paramref name="sensorReadingEvent"/>
        /// </summary>
        /// <param name="sensorReadingEvent">Deserialized SensorReadingEvent object</param>
        protected abstract void ProcessSensorReadingEvent(SensorReadingEventDto sensorReadingEvent);

        /// <summary>
        /// Method for processing a new <paramref name="aliasAddedEvent"/>
        /// </summary>
        /// <param name="aliasAddedEvent">Deserialized AliasAddedEvent object</param>
        protected abstract void ProcessAliasAddedEvent(AliasAddedEventDto aliasAddedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="aliasRemovedEvent"/>
        /// </summary>
        /// <param name="aliasRemovedEvent">Deserialized AliasRemovedEvent object</param>
        protected abstract void ProcessAliasRemovedEvent(AliasRemovedEventDto aliasRemovedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="aliasModifiedEvent"/>
        /// </summary>
        /// <param name="aliasModifiedEvent">Deserialized AliasModifiedEvent object</param>
        protected abstract void ProcessAliasModifiedEvent(AliasModifiedEventDto aliasModifiedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceCredentialsAddedEvent"/>
        /// </summary>
        /// <param name="deviceCredentialsAddedEvent">Deserialized DeviceCredentialsAddedEvent</param>
        protected abstract void ProcessDeviceCredentialsAddedEvent(DeviceCredentialsAddedEventDto deviceCredentialsAddedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceCredentialsRemovedEvent"/>
        /// </summary>
        /// <param name="deviceCredentialsRemovedEvent">Deserialized DeviceCredentialsRemovedEvent</param>
        protected abstract void ProcessDeviceCredentialsRemovedEvent(DeviceCredentialsRemovedEventDto deviceCredentialsRemovedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceCredentialsModifiedEvent"/>
        /// </summary>
        /// <param name="deviceCredentialsModifiedEvent">Deserialized DeviceCredentialsModifiedEvent object</param>
        protected abstract void ProcessDeviceCredentialsModifiedEvent(DeviceCredentialsModifiedEventDto deviceCredentialsModifiedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="businessProcessEventDto"/>
        /// </summary>
        /// <param name="businessProcessEventDto">Deserialized BusinessProcessEventDto object</param>
        protected abstract void ProcessBusinessProcessEvent(BusinessProcessEventDto businessProcessEventDto);

        /// <summary>
        /// Method for processing unknown events
        /// </summary>
        /// <param name="message">The unknown event</param>
        protected abstract void ProcessUnknownMessage(EventDto message);

        #endregion

    }
}
