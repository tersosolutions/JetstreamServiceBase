![Terso Solutions Logo](https://cdn.tersosolutions.com/github/TersoHorizontal_BlackGreen.png "Terso Solutions, Inc.")

## Jetstream Service Base
[Jetstream Documentation - https://jetstreamrfid.com/documentation/applicationapi/3](https://jetstreamrfid.com/documentation/applicationapi/3)
 
### Microsoft .NET Standard 2.0
The service base includes the ability to leverage the [Jetstream SDK](https://github.com/tersosolutions/JetstreamSDK-.NET) to get and remove events from Jetstream. Use those events to process inventory transactions in an application.

### Use the application or device API in a project
Add a reference to the `Jetstream.Sdk.dll` or search for Jetstream.ServiceBase on NuGet

### Change History
* v3.4.0 - June 23, 2022
  * Update async methods to be task driven
  * Update base service to use IHostedService better
  * Remove .net Framework 4.7.2 support since netstandard can be used in those projects
  * Add unit test project stub
* v3.2.0 - July 24, 2020
  * Asynchrynous processing of events
* v3.1.0 - July 2, 2019
  * Update to support Jetstream v3 SDK
  * Target .NET standard 2.0 and Framework 4.7.2
  * Added support for deploying to NuGet and GitHub from VSTS
  * Created license file
* v1.5.2 - April 19, 2017 - Update Jetstream SDK to v1.5.2
* v1.5 - October 14, 2015 - Added to Github