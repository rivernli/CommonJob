using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Flex.Job
{
    public enum JobStatus
    {
        None = 0,
        Running = 1,
        Success = 9,
        Failed = 2
    }
    public class JobConfig
    {
        public string JobId { get; set; }
        public string JobName { get; set; }
        public string JobArgs { get; set; }
        public string JobPath { get; set; }
        public JobStatus JobStatus { get; set; }
        public List<string> JobSiteCodes { get; private set; }

        public JobConfig()
        {
            JobSiteCodes = new List<string>();
        }
    }
}
