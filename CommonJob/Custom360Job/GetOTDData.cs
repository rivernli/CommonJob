using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Flex.Job;
using System.Data;
using Flex.FCCP;
using Flex.Data;
using System.Diagnostics;
using System.IO;
using System.Configuration;
using BI.SGP.BLL.Export;
using System.Globalization;

namespace Custom360Job
{
    /// <summary>
    /// desc: 从Excel中获取NPI， OTD数据，保存到数据库中
    /// author: sen
    /// date: 2015/9/2
    /// </summary>
    public class GetOTDData : BaseJob
    {
        /// <summary>
        /// job 执行 @sen
        /// </summary>
        /// <param name="siteInfo"> 配置文件</param>
        /// <param name="log">记录日志</param>
        /// <param name="tag"></param>
        protected override void Execute(SiteInfo siteInfo, StringBuilder log, object tag)
        {

            DataAccess.DefaultDB = new Database(DatabaseType.SqlServer, siteInfo.ConnectionConfig);
            ConnectionStringSettings sqlServerConnectionInit = ConfigurationManager.ConnectionStrings["db_mcn801multekbi"];
            Database fwDB = new Database(DatabaseType.SqlServer, sqlServerConnectionInit.ConnectionString);
            // 获取文件所在路径
            // 路径中没有文件则需要提示，读完文件后需要备份文件
            string filePathOTD = ConfigurationManager.AppSettings["FILE_PATH_OTD"];
            string filePathNPI = ConfigurationManager.AppSettings["FILE_PATH_NPI"];
            string filePathBakup = ConfigurationManager.AppSettings["FILE_PATH_BAKUP"];
            string fileNameOTD = ConfigurationManager.AppSettings["FILE_NAME_OTD"];
            string fileNameNPI = ConfigurationManager.AppSettings["FILE_NAME_NPI"];
            string sourceFile = System.IO.Path.Combine(filePathOTD, fileNameOTD);
            string sourceFileNPI = Path.Combine(filePathNPI, fileNameNPI);
            DirectoryInfo dirInfo = new DirectoryInfo(filePathOTD);
            string userMail = ConfigurationManager.AppSettings["USER_MAIL"];
            if (dirInfo.GetFiles().Length + dirInfo.GetDirectories().Length == 0)
            {//目录为空, 则给用户发邮件提示
                log.Append("The File isn't exsit!;");
            }
            else if (!File.Exists(sourceFile))
            {//OTD文件不存在
                log.Append(string.Format("The OTD Data File '{0}' can't find! please check!;", fileNameOTD));
            }
            else if (!File.Exists(sourceFileNPI))
            {//NPI文件不存在
                log.Append(string.Format("The NPI Data File '{0}' can't find! please check!", fileNameNPI));
            }
            else
            {
                int maxRow = Convert.ToInt32(ConfigurationManager.AppSettings["MAX_ROW"]);
                string[] charFlag = new string[] { "BY UNITS", "BY AMOUNT" };
                DataSet ds = ExcelHelper.ReadExcel(sourceFile);
                DataSet dsNPI = ExcelHelper.ReadExcel(sourceFileNPI);
                DataTable dtByItem = new DataTable();
                DataTable dtByLeaTime = new DataTable();
                DataTable dtNPI = new DataTable();
                foreach (DataTable dt in ds.Tables)
                {
                    if (dt.TableName.Trim().ToUpper() == "Customer request OTD".ToUpper())
                    {// 搜索 By Items
                        dtByItem = dt.Clone();
                        dtByItem.TableName = dt.TableName;
                        bool isByItemRow = false;
                        foreach (DataRow dr in dt.Rows)
                        {
                            if (dr[0].ToString().ToUpper() == "By Items".ToUpper())
                            {
                                isByItemRow = true;
                                continue;
                            }
                            if (isByItemRow == true && !charFlag.Contains(dr[0].ToString().ToUpper()))
                            {
                                dtByItem.Rows.Add(dr.ItemArray);
                            }
                            else if (isByItemRow == true && charFlag.Contains(dr[0].ToString().ToUpper()))
                            {
                                isByItemRow = false;
                            }

                        }
                    }

                    else if (dt.TableName.Trim().ToUpper() == "Average Leadtime".ToUpper())
                    {// 读取Average Leadtime sheet 的数据
                        int currRow = 0;
                        bool isCustomData = false;
                        dtByLeaTime = dt.Clone();
                        dtByLeaTime.TableName = dt.TableName;
                        foreach (DataRow dr in dt.Rows)
                        {
                            if ((dr[0].ToString().ToUpper() == "Customer".ToUpper() || isCustomData == true) && currRow <= maxRow)
                            {
                                isCustomData = true;
                                currRow += 1;
                                dtByLeaTime.Rows.Add(dr.ItemArray);
                            }
                        }
                    }
                    else { continue; }
                }
                // 获取当前的财务周，取上一个财务周的数据插入更新
                string getFweekSql = @"select FiscalYear, FiscalWeek from calendar where calenderDate = CONVERT(varchar(100), GETDATE(), 23)";
                DataTable dtFw = fwDB.CustomSql(getFweekSql).ToDataTable();
                if (dtFw.Rows.Count > 0)
                {// 获取当前FiscalWeek
                    string fiscalWeek = dtFw.Rows[0]["FiscalWeek"].ToString();
                    string fiscalYear = dtFw.Rows[0]["FiscalYear"].ToString();
                    // dtByItem: type = 1
                    InsertCumstomOTD(dtByItem, 1, fiscalWeek, fiscalYear, log);
                    // dtByLeaTime: type = 2
                    InsertCumstomOTD(dtByLeaTime, 2, fiscalWeek, fiscalYear, log);

                    // bakup file
                    BakupFile(fileNameOTD, filePathOTD, filePathBakup);
                }
                else
                {// 当前财务周找不到，则发邮件提醒维护数据
                    log.Append("the current fiacal week can't find in the database, plasee check it!;");
                }

                if (dsNPI.Tables.Count > 0)
                {// 插入Npi 数据
                    dtNPI = dsNPI.Tables[0];
                    dtNPI.TableName = dsNPI.Tables[0].TableName;
                    Dictionary<string, string> dic = CreateMonthToHead();
                    InsertCumstomNPI(dtNPI, log, dic);
                    // bakup file
                    BakupFile(fileNameNPI, filePathNPI, filePathBakup);
                }
            }

            // send mail
            SendEmailTo(log.ToString(), userMail);
        }

        /// <summary>
        /// 创建月份与表头的映射关系
        /// </summary>
        private Dictionary<string, string> CreateMonthToHead()
        {
            string tempYear = DateTime.Now.ToString("yy");
            string year = "";
            string month = DateTime.Now.ToString("MM");
            if(Convert.ToInt32(month) <= 3)
            {
                 year = tempYear;
            }
            else
            {
                year = (Convert.ToInt32(tempYear) + 1).ToString();
            }
            Dictionary<string, string> dic = new Dictionary<string, string>();
            dic.Add("04", string.Format("Apr(P1) FY{0}",year));
            dic.Add("05", string.Format("May(P2) FY{0}", year));
            dic.Add("06", string.Format("Jun(P3) FY{0}", year));
            dic.Add("07", string.Format("Jul(P4) FY{0}", year));
            dic.Add("08", string.Format("Aug(P5) FY{0}", year));
            dic.Add("09", string.Format("Sep(P6) FY{0}", year));
            dic.Add("10", string.Format("Oct(P7) FY{0}", year));
            dic.Add("11", string.Format("Nov(P8) FY{0}", year));
            dic.Add("12", string.Format("Dec(P9) FY{0}", year));
            dic.Add("01", string.Format("Jan(P10) FY{0}", year));
            dic.Add("02", string.Format("Feb(P11) FY{0}", year));
            dic.Add("03", string.Format("Mar(P12) FY{0}", year));
            dic.Add("4", string.Format("Apr(P1) FY{0}", year));
            dic.Add("5", string.Format("May(P2) FY{0}", year));
            dic.Add("6", string.Format("Jun(P3) FY{0}", year));
            dic.Add("7", string.Format("Jul(P4) FY{0}", year));
            dic.Add("8", string.Format("Aug(P5) FY{0}", year));
            dic.Add("9", string.Format("Sep(P6) FY{0}", year));
            dic.Add("1", string.Format("Jan(P10) FY{0}", year));
            dic.Add("2", string.Format("Feb(P11) FY{0}", year));
            dic.Add("3", string.Format("Mar(P12) FY{0}", year));
            return dic;
        }

        /// <summary>
        /// 插入NPI数据到表 sgp_cumstomer_npi
        /// </summary>
        /// <param name="dsNPI"></param>
        /// <param name="log"></param>
        private void InsertCumstomNPI(DataTable dt, StringBuilder log, Dictionary<string, string> dic)
        {
            string insertSql = @"insert into sgp_customer_npi (customer, month, total_npi, successful_npi, npi_otd, create_date) 
                             select '{0}', '{1}', '{2}', '{3}', '{4}', getdate()";

            string selectSql = @"select count(1) from sgp_customer_npi where customer = '{0}'";
            string selectSqlByMonth = @"select count(1) from sgp_customer_npi where customer = '{0}' and month = '{1}'";
            DataRow drTotalRow = null;
            DataRow drSuccessfulRow = null;
            DataRow drNpiOtd = null;
            for (int i = 1; i < dt.Rows.Count; i++)
            {
                // 取出行数据
                if (dt.Rows[i][0].ToString().ToUpper() == "Total NPI".ToUpper())
                {
                    drTotalRow = dt.Rows[i];
                }
                if (dt.Rows[i][0].ToString().ToUpper() == "Successful NPI".ToUpper())
                {
                    drSuccessfulRow = dt.Rows[i];

                }
                if (dt.Rows[i][0].ToString().ToUpper() == "NPI OTD".ToUpper())
                {
                    drNpiOtd = dt.Rows[i];
                }
            }
            int totalColumn = dt.Columns.Count;
            int count = DataAccess.DefaultDB.CustomSql(string.Format(selectSql, dt.Columns[0].ColumnName))
                                    .ToScalar<int>();

            for (int j = 1; j < totalColumn; j++)
            {
                // 先检查是否为该类型的数据在数据库中是否为空，判断是否为第一次插入数据，第一次插入数据需要把所有数据插入
                // 以后每次只获取上个月的数据插入

                if (count == 0)
                {// 首次插入，插入所有
                    if (CheckData(drTotalRow[j].ToString()) && CheckData(drSuccessfulRow[j].ToString())
                        && CheckData(drNpiOtd[j].ToString()))
                    {// 数据正确
                        DataAccess.DefaultDB.CustomSql(string.Format(insertSql,
                                                              dt.Columns[0].ColumnName,
                                                              dt.Rows[0][j].ToString(),
                                                              drTotalRow[j].ToString(),
                                                              drSuccessfulRow[j].ToString(),
                                                              drNpiOtd[j].ToString())).ExecuteNonQuery();
                    }
                    else
                    {// 数据错误
                        log.Append(string.Format("sheet name: {0}, the npi data is error, column: {1}! please check!;", dt.TableName, j+1));
                    }
                }
                else
                { // 非首次插入，插入最新的数据
                    string currenMonth = DateTime.Now.ToString("MM");
                    string lastMonth = (Convert.ToInt32(currenMonth) - 1 == 0 ? 12 : Convert.ToInt32(currenMonth) - 1).ToString();
                    if (dic[lastMonth].ToUpper() == dt.Rows[0][j].ToString().ToUpper())
                    {// 获取当月或上个月的数据
                        if (CheckData(drTotalRow[j].ToString()) && CheckData(drSuccessfulRow[j].ToString())
                        && CheckData(drNpiOtd[j].ToString()))
                        {// 数据正确
                            // 检查数据库中是否已存在
                            int countMonth = DataAccess.DefaultDB.CustomSql(string.Format(selectSqlByMonth,
                                                                dt.Columns[0].ColumnName,
                                                                dt.Rows[0][j].ToString())).ToScalar<int>();
                            if (countMonth == 0)
                            {
                                DataAccess.DefaultDB.CustomSql(string.Format(insertSql,
                                                              dt.Columns[0].ColumnName,
                                                              dt.Rows[0][j].ToString(),
                                                              drTotalRow[j].ToString(),
                                                              drSuccessfulRow[j].ToString(),
                                                              drNpiOtd[j].ToString())).ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            log.Append(string.Format("sheet name: {0}, the npi data is error, column:{1}! please check!;", dt.TableName, j+1));
                        }
                    }

                }

            }
        }

        /// <summary>
        /// var keys = dict.Where(q => q.Value == "123").Select(q => q.Key);
        /// </summary>
        /// <returns></returns>
        private bool IsAfterMonth(string monthStr, Dictionary<string,string> dict)
        {
            int currentMonth = Convert.ToInt32(DateTime.Now.ToString("MM"));
            var keys = dict.Where(q => q.Value == monthStr).Select(q => q.Key);
            if (currentMonth <= 3)
            {//

 
            }
            throw new NotImplementedException();
        }

        /// <summary>
        /// 检查数据是否正确
        /// </summary>
        /// <param name="p"></param>
        private bool CheckData(string source)
        {
            double rsl = -1;
            return double.TryParse(source, NumberStyles.Number, null, out rsl);
        }
        /// <summary>
        /// 向数据库中插入数据 @sen
        /// </summary>
        /// <param name="dt">插入的数据</param>
        /// <param name="type">数据表类型</param>
        /// <param name="fiscalWeek">当前财务周</param>>
        public void InsertCumstomOTD(DataTable dt, int type, string fiscalWeek, string fiscalYear, StringBuilder log)
        {
            string selectSqlByType = @"select count(1) from sgp_customer_otd where customer = '{0}' and type = '{1}'";
            string selectSqlByWeek = @"select count(1) from sgp_customer_otd where customer = '{0}' and week = '{1}' and type = '{2}' and year = '{3}'";
            string insertSql = @"insert into sgp_customer_otd (customer, week, data, type, create_date, year) select
                                  '{0}', '{1}', '{2}', '{3}', getdate(), '{4}'";

            string lastFiscalWeek = GetLastFicalWeek(fiscalWeek);
            string lastFiscalYear = GetLastFicalYear(fiscalWeek, fiscalYear);

            for (int i = 1; i < dt.Rows.Count; i++)
            {
                int totalColumn = dt.Columns.Count;

                int countType = DataAccess.DefaultDB.CustomSql(string.Format(selectSqlByType,
                                                                dt.Rows[i][0].ToString(),
                                                                type)).ToScalar<int>();

                for (int j = 1; j < totalColumn; j++)
                {
                    // 先检查是否为该类型的数据在数据库中是否为空，判断是否为第一次插入数据，第一次插入数据需要把所有数据插入
                    // 以后每次只获取上个财务周的数据插入

                    if (countType == 0)
                    {// 首次插入数据, 插入所有
                        if (IsAfterFiscalWeek(dt.Rows[0][j].ToString(), fiscalWeek) == false)
                        {
                            double tryOut2 = -1;
                            if (double.TryParse(dt.Rows[i][j].ToString(), NumberStyles.Number, null, out tryOut2))
                            {// 数据正确
                                DataAccess.DefaultDB.CustomSql(string.Format(insertSql,
                                                                      dt.Rows[i][0].ToString(),
                                                                      dt.Rows[0][j].ToString(),
                                                                      dt.Rows[i][j].ToString(),
                                                                      type,fiscalYear)).ExecuteNonQuery();
                            }
                            else
                            {// 数据错误
                                log.Append(string.Format("sheet name: {1}, the fiscal week {0} data is error! please check!;", dt.Rows[0][j].ToString(),dt.TableName));
                            }
                        }
                    }
                    else
                    {// 非首次插入数据, 仅获取上个财务周的数据插入
                        
                        if (dt.Rows[0][j].ToString() == lastFiscalWeek)
                        {
                            int countWeek = DataAccess.DefaultDB.CustomSql(string.Format(selectSqlByWeek,
                                dt.Rows[i][0].ToString(),
                                dt.Rows[0][j].ToString(),
                                type,
                                lastFiscalYear)).ToScalar<int>();

                            if (countWeek == 0)
                            {
                                double tryOut = -1;
                                if (double.TryParse(dt.Rows[i][j].ToString(), NumberStyles.Number, null, out tryOut))
                                {// 数据正确，则插入
                                    DataAccess.DefaultDB.CustomSql(string.Format(insertSql,
                                                                      dt.Rows[i][0].ToString(),
                                                                      dt.Rows[0][j].ToString(),
                                                                      dt.Rows[i][j].ToString(),
                                                                      type,
                                                                      lastFiscalYear)).ExecuteNonQuery();
                                }
                                else
                                {// 数据错误， 则发邮件提醒
                                    log.Append(string.Format("sheet name: {0},the last fiscal week data is error! please check!;",dt.TableName));
                                }
                            }
                        }
                    }
                }
            }
        }

        private string GetLastFicalYear(string fiscalWeek, string fiscalYear)
        {
            int currentWeek = Convert.ToInt32(fiscalWeek);
            if (currentWeek == 1)
            {// 如果
                return (Convert.ToInt32(fiscalYear) - 1).ToString();
            }
            else
            {
                return fiscalYear;
            }
        }

        /// <summary>
        /// 获取上个财务周的周数
        /// </summary>
        /// <param name="fiscalWeek"></param>
        private string GetLastFicalWeek(string fiscalWeek)
        {
            int currentWeek = Convert.ToInt32(fiscalWeek);
            if (currentWeek == 1)
            {// 如果
                return "W" + (currentWeek - 1).ToString();
            }
            else
            {
                return "W" + (currentWeek - 1).ToString();
            }
        }

        /// <summary>
        /// 判断是否在当前财务周之后,这些数据不需要插入
        /// </summary>
        /// <param name="p"></param>
        /// <param name="fiscalWeek"></param>
        private bool IsAfterFiscalWeek(string week, string currFiscalWeek)
        {
            int fiscalWeek = 0;
            if (week.Length >= 2)
            {
                if (int.TryParse(week.Substring(1, week.Length - 1), out fiscalWeek))
                {
                    if (fiscalWeek >= Convert.ToInt32(currFiscalWeek)) { return true; }
                    else { return false; }
                }
                else { return true; }
            }
            else { return true; }
        }

        /// <summary>
        /// excel读取完成后备份文件
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="sourcePath"></param>
        /// <param name="targetPath"></param>
        public void BakupFile(string fileName, string sourcePath, string targetPath)
        {
            // Use Path class to manipulate file and directory paths.
            string sourceFile = Path.Combine(sourcePath, fileName);
            string dateNow = DateTime.Now.ToString("yyyyMMddHHmmss");
            string destFileName = dateNow + "_" + fileName;
            string destFile = Path.Combine(targetPath, destFileName);

            // To copy a folder's contents to a new location:
            // Create a new target folder, if necessary.
            if (!Directory.Exists(targetPath))
            {
                Directory.CreateDirectory(targetPath);
            }

            // To copy a file to another location and 
            // overwrite the destination file if it already exists.
            File.Copy(sourceFile, destFile, true);
        }
       
        /// <summary>
        /// 根据不同的错误类型发送不同的邮件内容 @sen
        /// </summary>
        /// <param name="Type">0,文件成功读取; 1,共享目录没有文件 2,财务周数据没有找到</param>
        public void SendEmailTo(string logContent, string mails)
        {
            string subject = "";
            string content = "";
            string hContent = @"<div style ='font-size:16;color:red;'>Hi All:</div>";
            if (string.IsNullOrWhiteSpace(logContent))
            {
                subject = "Auto Read OTD Data successfully!";
                content = hContent + "<h3>Auto Read OTD Data successfully!</h3>";
            }
            else
            {
                string str = "";
                List<string> contentList = logContent.Split(new char[] { ';' }).ToList<string>();
                foreach (string c1 in contentList)
                {
                    string s = string.Format(@"<h3 style = 'font-size:16;color:red;'>{0}</h3>", c1);
                    str = str + s;
                }

                content = hContent + str;

                subject = "Get OTD Data Error! Please Check!";
            }

            if (subject != "" || content != "")
            {
                JOBTool.SendDotEmail(subject, mails, content);
            }
        }   
    }
}
