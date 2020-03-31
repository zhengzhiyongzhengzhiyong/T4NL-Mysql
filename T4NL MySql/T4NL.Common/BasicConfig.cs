using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace T4NL.Common
{
    public  class BasicConfig
    {
        public static string ConnectionString = System.Configuration.ConfigurationManager.AppSettings["MySQLConnString"];


    }
}
