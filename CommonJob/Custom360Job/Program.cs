using Flex.Job;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Custom360Job
{
    class Program
    {
        static void Main(string[] args)
        {
            string site = string.Empty;
            if (args != null && args.Length > 0)
                site = args[0];

            BaseJob job = new GetOTDData();

            if (String.IsNullOrEmpty(site))
            {
                job.Execute();
            }
            else
            {
                job.Execute(site);
            }

            //把处理log和处理error文件发邮件给FCCP team member
            string logFile = JOBTool.LOG_FILE;
            string errFile = JOBTool.ERR_FILE;
            string logMail = ConfigurationManager.AppSettings["LOG_MAIL"];
            if (!String.IsNullOrEmpty(logMail) && File.Exists(logFile))
            {
                List<string> files = new List<string>();
                files.Add(logFile);
                files.Add(errFile);
                JOBTool.SendDotEmail(string.Format("Get OTDData - Log,机器名：{0}", System.Environment.MachineName), logMail, "LOG", files);
            }
        }
    }
}
