using System;
using System.Collections.Generic;
using System.Text;
using System.Net.Mail;
using System.IO;
using System.Configuration;

namespace Flex.Job
{
    public static class JOBTool
    {
        public static string LOG_FILE;
        public static string ERR_FILE;

        static JOBTool()
        {
            string fileName = String.Format("BIJob({0}).log", DateTime.Now.ToString("yyyy-MM-dd HHmmss"));
            string logDir = ConfigurationManager.AppSettings["LOG_PATH"];
            if (String.IsNullOrEmpty(logDir))
            {
                LOG_FILE = Path.Combine(Environment.CurrentDirectory, fileName);                
            }
            else
            {
                LOG_FILE = Path.Combine(logDir, fileName);
            }
            ERR_FILE = LOG_FILE.Replace("BIJob", "BIJob-error");
        }

        #region Logging

        public static void LogError(Exception ex, SiteInfo siteInfo)
        {
            StringBuilder err = new StringBuilder(Environment.NewLine);
            err.Append("=== EXCEPTION ===\r\n")
                .Append("SITE:\t").Append(siteInfo.SiteCode).Append("\r\n")
                .Append("DATE:\t").Append(DateTime.Now.ToString()).Append("\r\n")
                .Append("MESSAGE:").Append(ex.Message).Append("\r\n")
                .Append("STACKTRACE:").Append("\r\n")
                .Append(ex.StackTrace);

            File.AppendAllText(LOG_FILE, err.ToString(), Encoding.UTF8);
        }

        public static void LogError(Exception ex)
        {
            StringBuilder err = new StringBuilder(Environment.NewLine);
            err.Append("=== EXCEPTION ===\r\n")
                .Append("DATE:\t").Append(DateTime.Now.ToString()).Append("\r\n")
                .Append("MESSAGE:").Append(ex.Message).Append("\r\n")
                .Append("STACKTRACE:").Append("\r\n")
                .Append(ex.StackTrace);

            //File.AppendAllText(LOG_FILE, err.ToString(), Encoding.UTF8);
            File.AppendAllText(ERR_FILE, err.ToString(), Encoding.UTF8);
        }

        public static void LogError(string error)
        {
            if (String.IsNullOrEmpty(error))
                return;

            StringBuilder err = new StringBuilder(Environment.NewLine);
            err.Append("---   ERROR   ---\r\n")
                .Append("CONTENT:").Append(error).Append("\r\n");

            File.AppendAllText(ERR_FILE, err.ToString(), Encoding.UTF8);
        }

        public static void LogMessage(string message, SiteInfo siteInfo)
        {
            StringBuilder msg = new StringBuilder(Environment.NewLine);
            msg.Append("***  MESSAGE  ***\r\n")
                .Append("SITE:\t").Append(siteInfo.SiteCode).Append("\r\n")
                .Append("DATE:\t").Append(DateTime.Now.ToString()).Append("\r\n")
                .Append("CONTENT:\t").Append(message).Append("\r\n");

            File.AppendAllText(LOG_FILE, msg.ToString(), Encoding.UTF8);
        }

        public static void LogMessage(string message, bool isRecordDatetime = false)
        {
            if (String.IsNullOrEmpty(message))
                return;

            if (isRecordDatetime)
            {
                message = string.Format("[ {0} ]  {1} ", DateTime.Now, message);
            }

            StringBuilder msg = new StringBuilder(Environment.NewLine);
            msg.Append("***  MESSAGE  ***\r\n")
                .Append("CONTENT:\t").Append(message).Append("\r\n");

            File.AppendAllText(LOG_FILE, msg.ToString(), Encoding.UTF8);
        }

        #endregion

        #region Send ASP.NET Mail
        public static void SendDotEmail(string mailSubject, string mailAddress, string mailContent)
        {
            SendDotEmail(mailSubject, mailAddress, mailContent, null);
        }

        public static void SendDotEmail(string mailSubject, string mailAddress, string mailContent, List<string> mailAttachment)
        {
            SendDotEmail(mailSubject, new List<string>(mailAddress.Split(';', '；', ',')), mailContent, mailAttachment);
        }

        public static void SendDotEmail(string mailSubject, List<string> mailAddress, string mailContent)
        {
            SendDotEmail(mailSubject, mailAddress, mailContent, null);
        }

        public static void SendDotEmail(string mailSubject, List<string> mailAddress, string mailContent, List<string> mailAttachment)
        {
            try
            {
                SmtpClient client = new SmtpClient(System.Configuration.ConfigurationManager.AppSettings["SMTPServer"]);
                //MailAddress中的邮件地址任意，如sha.ccp@cn.flextronics.com,kyle.chen@cn.flextronics.com
                MailAddress from = new MailAddress("no-reply@cn.flextronics.com", "BI", System.Text.Encoding.UTF8);
                MailMessage message = new MailMessage();
                
                message.From = from;

                foreach (string strAddr in mailAddress)
                {
                    if (!String.IsNullOrEmpty(strAddr) && strAddr.IndexOf('@') > 0) {
                        LogMessage("Mail Addr:" + strAddr);
                        message.To.Add(strAddr);
                    }
                }
                message.Body = mailContent;
                message.BodyEncoding = System.Text.Encoding.UTF8;
                message.IsBodyHtml = true;
                message.Subject = mailSubject;
                message.SubjectEncoding = System.Text.Encoding.UTF8;
                message.Priority = MailPriority.High;

                if (mailAttachment != null && mailAttachment.Count > 0)
                {
                    foreach (string strAtt in mailAttachment)
                    {
                        if (File.Exists(strAtt))
                        {
                            Attachment attachment = new Attachment(strAtt);
                            message.Attachments.Add(attachment);
                        }
                    }
                }

                client.Send(message);
            }
            catch (Exception ex)
            {
                LogError(ex);
            }

        }

        #endregion


        #region Send ASP.NET Mail and CC
        public static void SendDotEmailCC(string mailSubject, string mailAddress, string mailAddressCC, string mailContent)
        {
            SendDotEmailCC(mailSubject, mailAddress, mailAddressCC, mailContent, null);
        }

        public static void SendDotEmailCC(string mailSubject, string mailAddress, string mailAddressCC, string mailContent, List<string> mailAttachment)
        {
            SendDotEmailCC(mailSubject, new List<string>(mailAddress.Split(';', '；', ',')), new List<string>(mailAddressCC.Split(';', '；', ',')), mailContent, mailAttachment);
        }

        public static void SendDotEmailCC(string mailSubject, List<string> mailAddress, List<string> mailAddressCC, string mailContent)
        {
            SendDotEmailCC(mailSubject, mailAddress, mailAddressCC, mailContent, null);
        }

        public static void SendDotEmailCC(string mailSubject, List<string> mailAddress, List<string> mailAddressCC, string mailContent, List<string> mailAttachment)
        {
            //try
            //{
                SmtpClient client = new SmtpClient(System.Configuration.ConfigurationManager.AppSettings["SMTPServer"]);
                //MailAddress中的邮件地址任意，如sha.ccp@cn.flextronics.com,kyle.chen@cn.flextronics.com
                MailAddress from = new MailAddress("no-reply@cn.flextronics.com", "BI", System.Text.Encoding.UTF8);
                MailMessage message = new MailMessage();

                message.From = from;

                foreach (string strAddr in mailAddress)
                {
                    if (!String.IsNullOrEmpty(strAddr) && strAddr.IndexOf('@') > 0)
                    {
                        LogMessage("Mail Addr:" + strAddr);
                        message.To.Add(strAddr);
                    }
                }

                foreach (string strAddrCC in mailAddressCC)
                {
                    if (!String.IsNullOrEmpty(strAddrCC) && strAddrCC.IndexOf('@') > 0)
                    {
                        LogMessage("Mail Addr CC:" + strAddrCC);
                        message.CC.Add(strAddrCC);
                    }
                }
                message.Body = mailContent;
                message.BodyEncoding = System.Text.Encoding.UTF8;
                message.IsBodyHtml = true;
                message.Subject = mailSubject;
                message.SubjectEncoding = System.Text.Encoding.UTF8;
                message.Priority = MailPriority.High;

                if (mailAttachment != null && mailAttachment.Count > 0)
                {
                    foreach (string strAtt in mailAttachment)
                    {
                        if (File.Exists(strAtt))
                        {
                            Attachment attachment = new Attachment(strAtt);
                            message.Attachments.Add(attachment);
                        }
                    }
                }

                client.Send(message);
            //}
            //catch (Exception ex)
            //{
            //    LogError(ex);
            //}

        }

        #endregion
    }
}
