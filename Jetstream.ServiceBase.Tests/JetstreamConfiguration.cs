using System;

namespace TersoSolutions.Jetstream.ServiceBase.Tests
{
    public static class JetstreamConfiguration
    {
        #region Data

        private static string _logicalDeviceId = "";
        private static readonly Random Random = new Random();
        private static readonly object SyncLock = new object();

        #endregion
       
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static string GetLogicalDeviceId() {
            if (_logicalDeviceId == "" || _logicalDeviceId != null)
            {
                return _logicalDeviceId = "WindTest_" + GetNumber(11111, 999999);
            }

            return _logicalDeviceId;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static int GetNumber(int min, int max)
        {
            lock (SyncLock)
            { // synchronize
                return Random.Next(min, max);
            }
        }
    }
}