using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.IO;
using System.Diagnostics;
using System.Configuration;
using Flex.Data;
using System.Data;
using Flex.FCCP;
using System.Threading;

namespace Flex.Job
{
    public abstract class BaseJob
    {
        public const string MAIL_SUBJECT = "BI Interface - Warn";

        /// <summary>
        /// JOB配置信息
        /// </summary>
        public JobConfig JobInfo { get; private set; }

        #region Load SiteInfo configuration

        private static List<SiteInfo> _sites = new List<SiteInfo>();

        public BaseJob()
        {
            JobInfo = new JobConfig();
        }

        static BaseJob()
        {

            string configPath = Path.Combine(Environment.CurrentDirectory, "SiteInfo.xml");
            if (!File.Exists(configPath))
            {
                JOBTool.LogError("Can't find 'SiteInfo.xml' file.");
                return;
            }

            try
            {
                XmlDocument doc = new XmlDocument();
                doc.Load(configPath);

                XmlNodeList nodeList = doc.SelectNodes("/sites/site");
                // 开始解析
                foreach (XmlNode node in nodeList)
                {
                    SiteInfo info = new SiteInfo();
                    foreach (XmlNode childNode in node.ChildNodes)
                    {
                        string val = childNode.InnerText.Trim();
                        switch (childNode.Name.ToLower())
                        {
                            case "sitecode":
                                info.SiteCode = val;
                                break;
                            case "baanserver":
                                info.BaaN_Server = val;
                                break;
                            case "connectionconfig":
                                //info.ConnectionConfig = val;
                                info.ConnectionConfig = ConfigurationManager.ConnectionStrings[val].ConnectionString;
                                break;
                            case "reportmail":
                                info.ReportMail = val;
                                break;
                            case "infomail":
                                info.InfoMail = val;
                                break;
                            case "webaddress":
                                info.WebAddress = val;
                                break;
                            case "sqlserverconnectionconfig":
                                info.SqlServerConnectionConfig = ConfigurationManager.ConnectionStrings[val].ConnectionString;
                                break;
                            default:
                                info.AddValue(childNode.Name, val);
                                break;
                        }
                    }
                    _sites.Add(info);
                }
            }
            catch (XmlException ex)
            {
                //XML 中有加载或分析错误。这种情况下，文档保持为空。
                JOBTool.LogError("datetime:" + DateTime.Now.ToString() + " 加载info.xml的时候发生错误!" + ex.Message);
            }
            catch (System.Xml.XPath.XPathException ex)
            {
                //XPath 表达式包含 XmlNamespaceManager 中没有定义的前缀。
                JOBTool.LogError("datetime:" + DateTime.Now.ToString() + " 分析info.xml的时候发生错误!" + ex.Message);
            }
            catch (Exception ex)
            {
                JOBTool.LogError(ex);
            }

        }

        public SiteInfo GetSiteInfo(string site)
        {
            foreach (SiteInfo info in _sites)
            {
                if (info.SiteCode == site)
                    return info;
            }

            return null;
        }

        #endregion

        public void Execute()
        {
            foreach (SiteInfo siteInfo in _sites)
            {
                DoExecute(siteInfo, null);
            }
        }

        public void Execute(string site)
        {
            Execute(site, null);
        }

        public void Execute(string site, object tag)
        {
            SiteInfo info = GetSiteInfo(site);
            if (info == null)
            {
                JOBTool.LogError(String.Format("请在配置文件SiteInfo.xml中配置该site：{0}", site));
                return;
            }

            DoExecute(info, tag);
        }

        private Stopwatch timer = new Stopwatch();

        //Job主线程是否正在运行，如主线程已经结束，则监听线程也结束
        protected bool mainThreadIsRunning = true;

        private void DoExecute(SiteInfo siteInfo, object tag)
        {
            try { RecordJob(); }
            catch { }

            //UpdateJobCompletedStatus();


            StringBuilder log = new StringBuilder();
            try
            {
                timer.Start();
                Execute(siteInfo, log, tag);
                timer.Stop();

                string timeString = (timer.ElapsedMilliseconds / 1000.0 / 60).ToString("F3");
                log.Append("总时间：").Append(timeString).AppendLine(" 分钟");

                JOBTool.LogMessage(log.ToString(), siteInfo);

                JobInfo.JobStatus = JobStatus.Success;
            }
            catch (Exception ex)
            {
                timer.Stop();
                JobInfo.JobStatus = JobStatus.Failed;

                if (log.Length > 0)
                    JOBTool.LogMessage(log.ToString(), siteInfo);

                JOBTool.LogError(ex, siteInfo);
            }


            try { UpdateJobCompletedStatus(); }
            catch { }
        }

        protected abstract void Execute(SiteInfo siteInfo, StringBuilder log, object tag);


        /// <summary>
        /// 启动监听线程
        /// </summary>
        private void StartListenThread()
        {
            Thread thr = new Thread(delegate()
            {
                while (mainThreadIsRunning)
                {
                    KeepListen();
                    Thread.Sleep(1000 * 10);
                }
            });

            thr.Start();
        }

        /// <summary>
        /// 更新监听时间
        /// </summary>
        private void KeepListen()
        {
            try
            {
                string sql = string.Format("UPDATE JOB_LIST SET LAST_LISTEN_TIME=SYSDATE WHERE JOB_ID='{0}'"
                   , JobInfo.JobId);
                DataTable dt = DataAccess.InitDB.CustomSql(sql).ToDataTable();
            }
            catch { }
        }

        /// <summary>
        /// 记录JOB运行
        /// </summary>
        private void RecordJob()
        {
            JobInfo.JobStatus = JobStatus.Running;
            //JobInfo.JobName = AppDomain.CurrentDomain.DomainManager.EntryAssembly.ManifestModule.Name;
            JobInfo.JobName = Thread.GetDomain().FriendlyName;
            JobInfo.JobPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, JobInfo.JobName);
            //if (AppDomain.CurrentDomain.SetupInformation.ActivationArguments != null)
            {
                //    JobInfo.JobArgs = AppDomain.CurrentDomain.SetupInformation.ActivationArguments.ToString();
            }

            //载入或创建JOB到LIST表
            LoadOrCreateJobInfo();

            //启动监听线程
            StartListenThread();

        }


        /// <summary>
        /// 载入或创建JOB信息
        /// </summary>
        private void LoadOrCreateJobInfo()
        {
            string sql = string.Format("SELECT * FROM JOB_LIST WHERE JOB_NAME='{0}' AND JOB_ARGS {1} "
                , JobInfo.JobName
                , string.IsNullOrEmpty(JobInfo.JobArgs) ? " = ''" : string.Format(" ='{0}'", JobInfo.JobArgs));

            DataTable dt = DataAccess.InitDB.CustomSql(sql).ToDataTable();

            //创建新的JOB信息
            if (dt == null || dt.Rows.Count <= 0)
            {
                JobInfo.JobId = GenerateJobId(JobInfo.JobName);
                sql = string.Format(@"
INSERT INTO JOB_LIST (JOB_NAME,JOB_ARGS,JOB_ID,JOB_PATH,CREATE_TIME)
SELECT '{0}','{1}','{2}','{3}',getdate()"
                    , JobInfo.JobName, JobInfo.JobArgs, JobInfo.JobId, JobInfo.JobPath);
                DataAccess.InitDB.CustomSql(sql).ExecuteNonQuery();
            }
            else
            {
                JobInfo.JobId = dt.Rows[0]["JOB_ID"].ToString();
            }

            sql = string.Format("UPDATE JOB_LIST SET CURRENT_STATUS='{1}' WHERE JOB_ID='{0}'"
                , JobInfo.JobId, JobInfo.JobStatus);
            DataAccess.InitDB.CustomSql(sql).ExecuteNonQuery();
        }

        /// <summary>
        /// 更新运行后状态
        /// </summary>
        private void UpdateJobCompletedStatus()
        {
            //将主线程状态变更为不运行
            this.mainThreadIsRunning = false;

            string sql = string.Format(@"
UPDATE JOB_LIST 
SET LAST_EXECUTE_TIME=getdate(),LAST_EXECUTE_STATUS='{1}',TIME_ELAPSED_LAST={2},EXECUTE_COUNT=ISNULL(EXECUTE_COUNT,0)+1
,CURRENT_STATUS=NULL
WHERE JOB_ID='{0}'"
               , JobInfo.JobId, JobInfo.JobStatus, timer.Elapsed.TotalSeconds);

            DataAccess.InitDB.CustomSql(sql).ExecuteNonQuery();
        }

        /// <summary>
        /// 产生JOB ID
        /// </summary>
        /// <param name="jobName"></param>
        /// <returns></returns>
        private string GenerateJobId(string jobName)
        {
            for (int i = 1; i <= 30; i++)
            {
                string jobId = string.Format("{0}_{1}", jobName, i);
                string sql = string.Format("SELECT * FROM JOB_LIST WHERE JOB_ID='{0}'", jobId);
                int count = DataAccess.InitDB.CustomSql(sql).ExecuteNonQuery();
                if (count <= 0) return jobId;
            }
            return string.Format("{0}_{1}", jobName, DateTime.Now.ToString("yyyyMMddHHmmss"));
        }


    }
}
