using System;
using System.Collections.Generic;
using System.Text;
//using Flex.Data;

namespace Flex.Job
{
    public class SiteInfo
    {
        public string SiteCode { get; set; }

        /// <summary>
        /// BaaN Database的DB Link名称，如：baan_am3
        /// </summary>
        public string BaaN_Server { get; set; }

        public string ConnectionConfig { get; set; }

        public string ReportMail { get; set; }
        public string InfoMail { get; set; }
        public string WebAddress { get; set; }

        public string SqlServerConnectionConfig{get;set;}//新增属性 连接SQLSERVER数据库
        // 存放自定义元素
        private Dictionary<string, string> _values = new Dictionary<string, string>();
        public void AddValue(string key, string value)
        {
            if (_values.ContainsKey(key))
                _values[key] = value;
            else
                _values.Add(key, value);
        }

        public string GetValue(string key)
        {
            if (_values.ContainsKey(key))
                return _values[key];

            return null;
        }

        public T GetValue<T>(string key)
        {
            string valueString = this.GetValue(key);
            if (String.IsNullOrEmpty(valueString))
                throw new InvalidOperationException("Configuration is null with key:" + key);
            try
            {
                return (T)Convert.ChangeType(valueString, typeof(T));
            }
            catch
            {
                return default(T);
            }
        }

    }
}
