using Flex.Data;
using Flex.Job;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Custom360FromDbJob
{
    /// <summary>
    /// desc: 从DataBase中获取Custom360数据，保存进数据库
    /// author: sen
    /// date: 2015/9/2
    /// </summary>
    class GetDataFromDb : BaseJob
    {
        /// <summary>
        /// 执行job
        /// </summary>
        /// <param name="siteInfo"></param>
        /// <param name="log"></param>
        /// <param name="tag"></param>
        protected override void Execute(SiteInfo siteInfo, StringBuilder log, object tag)
        {// 读取xml中的sql语句执行
            XmlDocument xmlSql = new XmlDocument();
            xmlSql.Load(AppDomain.CurrentDomain.BaseDirectory + "SqlCommandText.xml");

            Database db = new Database(DatabaseType.SqlServer, siteInfo.ConnectionConfig);

            //创建multek_801 Multek_Apps数据库的连接
            ConnectionStringSettings sqlServerConnectionApps = ConfigurationManager.ConnectionStrings["db_mcn801multekapp"];
            Database dbMultekApps = new Database(DatabaseType.SqlServer, sqlServerConnectionApps.ConnectionString);

            //创建multek_801 Multek_Apps数据库的连接
            ConnectionStringSettings sqlServerConnectionBI = ConfigurationManager.ConnectionStrings["db_mcn801multekbi"];
            Database dbMultekBI = new Database(DatabaseType.SqlServer, sqlServerConnectionBI.ConnectionString);

            //获取carPm数据
            GetCarPmData(siteInfo, log, tag, xmlSql, db, dbMultekApps);

            //获取ForeCast, period_amount, bklog_amount数据
            GetPeriodDataByCustom(siteInfo, log, tag, xmlSql, db, dbMultekBI);
        }

        /// <summary>
        /// 获取ForeCast数据
        /// </summary>
        /// <param name="siteInfo"></param>
        /// <param name="log"></param>
        /// <param name="tag"></param>
        /// <param name="xmlSql"></param>
        /// <param name="db"></param>
        /// <param name="dbMultekApps"></param>
        private void GetPeriodDataByCustom(SiteInfo siteInfo, StringBuilder log, object tag, XmlDocument xmlSql, Database db, Database dbMultekApps)
        {
            // 从配置文件中获取，以分号隔开
            string customerStr = xmlSql.SelectSingleNode("/Root/CarPm/Customers").InnerText.Trim();

            // 读取xml中的sql语句
            string insertSql = xmlSql.SelectSingleNode("/Root/Period/Insert").InnerText.Trim();
            string deleteSql = xmlSql.SelectSingleNode("/Root/Period/Delete").InnerText.Trim();

            // 拆分用户字段
            string[] customs = customerStr.Split(new char[] { ';', ',', '；', '，' });
            foreach (string custom in customs)
            {
                if (!string.IsNullOrWhiteSpace(custom))
                {
                    using (DbTransaction tran = db.BeginTransaction())
                    {
                        db.CustomSql(string.Format(deleteSql, custom))
                            .SetTransaction(tran)
                            .ExecuteNonQuery();

                        string insertSqlNew = string.Format(insertSql, custom);

                        db.CustomSql(insertSqlNew)
                            .SetTransaction(tran)
                            .ExecuteNonQuery();

                        tran.Commit();
                    }
                }
            }
        }

       

        /// <summary>
        /// 获取carPm数据
        /// </summary>
        /// <param name="siteInfo"></param>
        /// <param name="log"></param>
        /// <param name="tag"></param>
        /// <param name="xmlSql"></param>
        private void GetCarPmData(SiteInfo siteInfo, StringBuilder log, object tag, XmlDocument xmlSql, Database db, Database dbMultekApps)
        {
            // 从配置文件中获取，以分号隔开
            string customerStr = xmlSql.SelectSingleNode("/Root/CarPm/Customers").InnerText.Trim();

            string querySql = xmlSql.SelectSingleNode("/Root/CarPm/Select").InnerText;
            string insertSql = xmlSql.SelectSingleNode("/Root/CarPm/Insert").InnerText;
            string countSql = xmlSql.SelectSingleNode("/Root/CarPm/QueryCount").InnerText;
            string updateSql = xmlSql.SelectSingleNode("/Root/CarPm/Update").InnerText;
            
            // 拆分用户字段
            string[] customs = customerStr.Split(new char[] {';', ',', '；', '，'});

            foreach (string custom in customs)
            {
                if (!string.IsNullOrWhiteSpace(custom))
                {
                    DataTable dtCarPm = dbMultekApps.CustomSql(string.Format(querySql, custom)).ToDataTable();

                    foreach (DataRow drCarPm in dtCarPm.Rows)
                    {// 插入每行数据, 如存在则更新,否则插入
                        int count = db.CustomSql(string.Format(countSql, drCarPm["rpm"].ToString())).ToScalar<int>();
                        if (count > 0)
                        {//存在，则更新
                            string updateSqlNew = string.Format(updateSql,
                                drCarPm["pdesc"].ToString().Replace("'", "''"),
                                drCarPm["compelet_date"].ToString(),
                                drCarPm["resolution"].ToString().Replace("'", "''"),
                                drCarPm["delay_date"].ToString(),
                                 drCarPm["rpm"].ToString());

                            db.CustomSql(updateSqlNew).ExecuteNonQuery();
                        }
                        else
                        {//不存在,插入
                            string insertSqlNew = string.Format(insertSql,
                                drCarPm["rpm"].ToString(),
                                custom,
                                drCarPm["pdesc"].ToString().Replace("'", "''"),
                                drCarPm["occurr_date"].ToString(),
                                drCarPm["compelet_date"].ToString(),
                                drCarPm["resolution"].ToString().Replace("'", "''"),
                                drCarPm["delay_date"].ToString()
                                );

                            db.CustomSql(insertSqlNew).ExecuteNonQuery();
                        }
                    }
                }
            }
            
        }
    }
}