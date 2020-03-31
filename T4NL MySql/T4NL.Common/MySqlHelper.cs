using System;
using System.Collections;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace T4NL.Common
{
    public  class MySqlHelper
    {


        /// <summary>
        /// 批量操作每批次记录数
        /// </summary>
        public static int BatchSize = 500;

        /// <summary>
        /// 超时时间
        /// </summary>
        public static int CommandTimeOut = 900;

        //Get the database connectionstring, which are static variables and readonly, all project documents can be used directly, but can not modify it 

        #region the database connectionString 


        public static string ConnectionString { get; } = BasicConfig.ConnectionString;
        //   public static string ConnectionString { get; } = "Server=192.168.1.222;Database=MT4;Uid=root;Pwd=000000;pooling=false;charset=utf8;";
        #endregion
        public static string ConnectionMain { get; } = BasicConfig.ConnectionString;

        //This connectionString for the local test
        //ConfigurationManager.ConnectionStrings["MySQLConnString"].ConnectionString;

        //hashtable to store the parameter information, the hash table can store any type of argument 
        //Here the hashtable is static types of static variables, since it is static, that is a definition of global use.
        //All parameters are using this hash table, how to ensure that others in the change does not affect their time to read it
        //Before ,the method can use the lock method to lock the table, does not allow others to modify.when it has readed then  unlocked table.
        //Now .NET provides a HashTable's Synchronized methods to achieve the same function, no need to manually lock, completed directly by the system framework 
        private static readonly Hashtable parmCache = Hashtable.Synchronized(new Hashtable());

        #region ExecuteNonQuery



        /// <summary>
        /// Execute a SqlCommand command that does not return value, by appointed and specified connectionstring 
        /// The parameter list using parameters that in array forms
        /// </summary>
        /// <remarks>
        /// Usage example: 
        /// int result = ExecuteNonQuery(connString, CommandType.StoredProcedure,
        /// "PublishOrders", new MySqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid database connectionstring</param>
        /// <param name="cmdType">MySqlCommand command type (stored procedures, T-SQL statement, and so on.) </param>
        /// <param name="cmdText">stored procedure name or T-SQL statement</param>
        /// <param name="commandParameters">MySqlCommand to provide an array of parameters used in the list</param>
        /// <returns>Returns a value that means number of rows affected</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            MySqlCommand cmd = new MySqlCommand();

            using (MySqlConnection conn = new MySqlConnection(connectionString))
            {
                PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
                int val = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                return val;
            }
        }

        /// <summary>
        /// Execute a SqlCommand command that does not return value, by appointed and specified connectionstring 
        /// The parameter list using parameters that in array forms
        /// </summary>
        /// <remarks>
        /// Usage example: 
        /// int result = ExecuteNonQuery(connString, CommandType.StoredProcedure,
        /// "PublishOrders", new MySqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="cmdType">MySqlCommand command type (stored procedures, T-SQL statement, and so on.) </param>
        /// <param name="connectionString">a valid database connectionstring</param>
        /// <param name="cmdText">stored procedure name or T-SQL statement</param>
        /// <param name="commandParameters">MySqlCommand to provide an array of parameters used in the list</param>
        /// <returns>Returns true or false </returns>
        public static bool ExecuteNonQuery(CommandType cmdType, string connectionString, string cmdText, params MySqlParameter[] commandParameters)
        {
            MySqlCommand cmd = new MySqlCommand();

            using (MySqlConnection conn = new MySqlConnection(connectionString))
            {
                PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
                try
                {
                    cmd.ExecuteNonQuery();
                    return true;
                }
                catch
                {
                    return false;
                }
                finally
                {
                    cmd.Parameters.Clear();
                }
            }
        }
        /// <summary>
        /// Execute a SqlCommand command that does not return value, by appointed and specified connectionstring 
        /// Array of form parameters using the parameter list 
        /// </summary>
        /// <param name="conn">connection</param>
        /// <param name="cmdType">MySqlCommand command type (stored procedures, T-SQL statement, and so on.)</param>
        /// <param name="cmdText">stored procedure name or T-SQL statement</param>
        /// <param name="commandParameters">MySqlCommand to provide an array of parameters used in the list</param>
        /// <returns>Returns a value that means number of rows affected</returns>
        public static int ExecuteNonQuery(MySqlConnection conn, CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
            int val = cmd.ExecuteNonQuery();
            cmd.Parameters.Clear();
            return val;
        }

        public static object ExecuteScalar(string cmdText, params MySqlParameter[] commandParameters)
        {
            MySqlCommand cmd = new MySqlCommand();
            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
            {
                PrepareCommand(cmd, connection, null, CommandType.Text, cmdText, commandParameters);
                object val = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                return val;
            }
        }

        public static MySqlDataReader ExecuteReader(string cmdText, params MySqlParameter[] commandParameters)
        {
            MySqlCommand cmd = new MySqlCommand();
            MySqlConnection conn = new MySqlConnection(ConnectionString);
            try
            {
                PrepareCommand(cmd, conn, null, CommandType.Text, cmdText, commandParameters);
                MySqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                cmd.Parameters.Clear();
                return rdr;
            }
            catch (Exception ex)
            {
                conn.Close();
                throw;
            }
        }

        /// <summary>
        /// Execute a SqlCommand command that does not return value, by appointed and specified connectionstring 
        /// Array of form parameters using the parameter list 
        /// </summary>
        /// <param name="trans">sql Connection that has transaction</param>
        /// <param name="cmdType">SqlCommand command type (stored procedures, T-SQL statement, and so on.)</param>
        /// <param name="cmdText">stored procedure name or T-SQL statement</param>
        /// <param name="commandParameters">MySqlCommand to provide an array of parameters used in the list</param>
        /// <returns>Returns a value that means number of rows affected </returns>
        public static int ExecuteNonQuery(MySqlTransaction trans, CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, trans.Connection, trans, cmdType, cmdText, commandParameters);
            int val = cmd.ExecuteNonQuery();
            cmd.Parameters.Clear();
            return val;
        }


        #endregion



        #region ExecuteReader




        /// <summary>
        /// Call method of sqldatareader to read data
        /// </summary>
        /// <param name="connectionString">connectionstring</param>
        /// <param name="cmdType">command type, such as using stored procedures: CommandType.StoredProcedure</param>
        /// <param name="cmdText">stored procedure name or T-SQL statement</param>
        /// <param name="commandParameters">parameters</param>
        /// <returns>SqlDataReader type of data collection</returns>
        public static MySqlDataReader ExecuteReader(string connectionString, CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            MySqlCommand cmd = new MySqlCommand();
            MySqlConnection conn = new MySqlConnection(connectionString);

            // we use a try/catch here because if the method throws an exception we want to 
            // close the connection throw code, because no datareader will exist, hence the 
            // commandBehaviour.CloseConnection will not work
            try
            {
                PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
                MySqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                cmd.Parameters.Clear();
                return rdr;
            }
            catch
            {
                conn.Close();
                throw;
            }
        }

        #endregion




        #region ExecuteScalar




        /// <summary>
        /// use the ExectueScalar to read a single result
        /// </summary>
        /// <param name="connectionString">connectionstring</param>
        /// <param name="cmdType">command type, such as using stored procedures: CommandType.StoredProcedure</param>
        /// <param name="cmdText">stored procedure name or T-SQL statement</param>
        /// <param name="commandParameters">parameters</param>
        /// <returns>a value in object type</returns>
        public static object ExecuteScalar(string connectionString, CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            MySqlCommand cmd = new MySqlCommand();

            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                PrepareCommand(cmd, connection, null, cmdType, cmdText, commandParameters);
                object val = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                return val;
            }
        }

        #endregion






        #region ExecuteDataSet

        public static DataSet GetDataSet(string connectionString, string cmdText, params MySqlParameter[] commandParameters)
        {
            DataSet retSet = new DataSet();
            using (MySqlDataAdapter msda = new MySqlDataAdapter(cmdText, connectionString))
            {
                msda.Fill(retSet);
            }
            return retSet;
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(string connectionString, string commandText, params MySqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                return ExecuteDataSet(connection, commandType, commandText, parms);
            }
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            return ExecuteDataSet(connection, null, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            return ExecuteDataSet(transaction.Connection, transaction, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        private static DataSet ExecuteDataSet(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            MySqlCommand command = new MySqlCommand();

            PrepareCommand(command, connection, transaction, commandType, commandText, parms);
            MySqlDataAdapter adapter = new MySqlDataAdapter(command);

            DataSet ds = new DataSet();
            adapter.Fill(ds);
            if (commandText.IndexOf("@", StringComparison.Ordinal) > 0)
            {
                commandText = commandText.ToLower();
                int index = commandText.IndexOf("where ", StringComparison.Ordinal);
                if (index < 0)
                {
                    index = commandText.IndexOf("\nwhere", StringComparison.Ordinal);
                }
                if (index > 0)
                {
                    ds.ExtendedProperties.Add("SQL", commandText.Substring(0, index - 1));  //将获取的语句保存在表的一个附属数组里，方便更新时生成CommandBuilder
                }
                else
                {
                    ds.ExtendedProperties.Add("SQL", commandText);  //将获取的语句保存在表的一个附属数组里，方便更新时生成CommandBuilder
                }
            }
            else
            {
                ds.ExtendedProperties.Add("SQL", commandText);  //将获取的语句保存在表的一个附属数组里，方便更新时生成CommandBuilder
            }

            foreach (DataTable dt in ds.Tables)
            {
                dt.ExtendedProperties.Add("SQL", ds.ExtendedProperties["SQL"]);
            }

            command.Parameters.Clear();
            return ds;
        }

        #endregion ExecuteDataSet


        #region ExecuteDataRow

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>,返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(string connectionString, string commandText, params MySqlParameter[] parms)
        {
            DataTable dt = ExecuteDataTable(connectionString, CommandType.Text, commandText, parms);
            return dt.Rows.Count > 0 ? dt.Rows[0] : null;
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>,返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            DataTable dt = ExecuteDataTable(connectionString, commandType, commandText, parms);
            return dt.Rows.Count > 0 ? dt.Rows[0] : null;
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>,返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            DataTable dt = ExecuteDataTable(connection, commandType, commandText, parms);
            return dt.Rows.Count > 0 ? dt.Rows[0] : null;
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>,返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            DataTable dt = ExecuteDataTable(transaction, commandType, commandText, parms);
            return dt.Rows.Count > 0 ? dt.Rows[0] : null;
        }

        #endregion ExecuteDataRow


        #region ExecuteDataTable

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(string connectionString, string commandText, params MySqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, CommandType.Text, commandText, parms).Tables[0];
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, commandType, commandText, parms).Tables[0];
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            return ExecuteDataSet(connection, commandType, commandText, parms).Tables[0];
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] parms)
        {
            return ExecuteDataSet(transaction, commandType, commandText, parms).Tables[0];
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="tableName">数据表名称</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteEmptyDataTable(string connectionString, string tableName)
        {
            return ExecuteDataSet(connectionString, CommandType.Text, string.Format("select * from {0} where 1=-1", tableName)).Tables[0];
        }

        #endregion ExecuteDataTable


        /// <summary>
        /// cache the parameters in the HashTable
        /// </summary>
        /// <param name="cacheKey">hashtable key name</param>
        /// <param name="commandParameters">the parameters that need to cached</param>
        public static void CacheParameters(string cacheKey, params MySqlParameter[] commandParameters)
        {
            parmCache[cacheKey] = commandParameters;
        }

        /// <summary>
        /// get parameters in hashtable by cacheKey
        /// </summary>
        /// <param name="cacheKey">hashtable key name</param>
        /// <returns>the parameters</returns>
        public static MySqlParameter[] GetCachedParameters(string cacheKey)
        {
            MySqlParameter[] cachedParms = (MySqlParameter[])parmCache[cacheKey];

            if (cachedParms == null)
                return null;

            MySqlParameter[] clonedParms = new MySqlParameter[cachedParms.Length];

            for (int i = 0, j = cachedParms.Length; i < j; i++)
                clonedParms[i] = (MySqlParameter)((ICloneable)cachedParms[i]).Clone();

            return clonedParms;
        }

        /// <summary>
        ///Prepare parameters for the implementation of the command
        /// </summary>
        /// <param name="cmd">mySqlCommand command</param>
        /// <param name="conn">database connection that is existing</param>
        /// <param name="trans">database transaction processing </param>
        /// <param name="cmdType">SqlCommand command type (stored procedures, T-SQL statement, and so on.) </param>
        /// <param name="cmdText">Command text, T-SQL statements such as Select * from Products</param>
        /// <param name="cmdParms">return the command that has parameters</param>
        private static void PrepareCommand(MySqlCommand cmd, MySqlConnection conn, MySqlTransaction trans, CommandType cmdType, string cmdText, MySqlParameter[] cmdParms)
        {
            if (conn.State != ConnectionState.Open)
                conn.Open();

            cmd.Connection = conn;
            cmd.CommandTimeout = CommandTimeOut;
            cmd.CommandText = cmdText;

            if (trans != null)
                cmd.Transaction = trans;

            cmd.CommandType = cmdType;

            if (cmdParms != null)
                foreach (MySqlParameter parm in cmdParms)
                    cmd.Parameters.Add(parm);
        }


        #region parameters
        /// <summary>
        /// Set parameters
        /// </summary>
        /// <param name="ParamName">parameter name</param>
        /// <param name="DbType">data type</param>
        /// <param name="Size">type size</param>
        /// <param name="Direction">input or output</param>
        /// <param name="Value">set the value</param>
        /// <returns>Return parameters that has been assigned</returns>
        public static MySqlParameter CreateParam(string ParamName, MySqlDbType DbType, Int32 Size, ParameterDirection Direction, object Value)
        {
            MySqlParameter param;


            if (Size > 0)
            {
                param = new MySqlParameter(ParamName, DbType, Size);
            }
            else
            {

                param = new MySqlParameter(ParamName, DbType);
            }


            param.Direction = Direction;
            if (!(Direction == ParameterDirection.Output && Value == null))
            {
                param.Value = Value;
            }


            return param;
        }

        /// <summary>
        /// set Input parameters
        /// </summary>
        /// <param name="ParamName">parameter names, such as:@ id </param>
        /// <param name="DbType">parameter types, such as: MySqlDbType.Int</param>
        /// <param name="Size">size parameters, such as: the length of character type for the 100</param>
        /// <param name="Value">parameter value to be assigned</param>
        /// <returns>Parameters</returns>
        public static MySqlParameter CreateInParam(string ParamName, MySqlDbType DbType, int Size, object Value)
        {
            return CreateParam(ParamName, DbType, Size, ParameterDirection.Input, Value);
        }

        /// <summary>
        /// Output parameters 
        /// </summary>
        /// <param name="ParamName">parameter names, such as:@ id</param>
        /// <param name="DbType">parameter types, such as: MySqlDbType.Int</param>
        /// <param name="Size">size parameters, such as: the length of character type for the 100</param>
        /// <returns>Parameters</returns>
        public static MySqlParameter CreateOutParam(string ParamName, MySqlDbType DbType, int Size)
        {
            return CreateParam(ParamName, DbType, Size, ParameterDirection.Output, null);
        }

        /// <summary>
        /// Set return parameter value 
        /// </summary>
        /// <param name="ParamName">parameter names, such as:@ id</param>
        /// <param name="DbType">parameter types, such as: MySqlDbType.Int</param>
        /// <param name="Size">size parameters, such as: the length of character type for the 100</param>
        /// <returns>Parameters</returns>
        public static MySqlParameter CreateReturnParam(string ParamName, MySqlDbType DbType, int Size)
        {
            return CreateParam(ParamName, DbType, Size, ParameterDirection.ReturnValue, null);
        }

        /// <summary>
        /// Generate paging storedProcedure parameters
        /// </summary>
        /// <param name="CurrentIndex">CurrentPageIndex</param>
        /// <param name="PageSize">pageSize</param>
        /// <param name="WhereSql">query Condition</param>
        /// <param name="TableName">tableName</param>
        /// <param name="Columns">columns to query</param>
        /// <param name="Sort">sort</param>
        /// <returns>MySqlParameter collection</returns>
        public static MySqlParameter[] GetPageParm(int CurrentIndex, int PageSize, string WhereSql, string TableName, string Columns, Hashtable Sort)
        {
            MySqlParameter[] parm = {
                                   MySqlHelper.CreateInParam("@CurrentIndex",  MySqlDbType.Int32,      4,      CurrentIndex    ),
                                   MySqlHelper.CreateInParam("@PageSize",      MySqlDbType.Int32,      4,      PageSize        ),
                                   MySqlHelper.CreateInParam("@WhereSql",      MySqlDbType.VarChar,  2500,    WhereSql        ),
                                   MySqlHelper.CreateInParam("@TableName",     MySqlDbType.VarChar,  20,     TableName       ),
                                   MySqlHelper.CreateInParam("@Column",        MySqlDbType.VarChar,  2500,    Columns         ),
                                   MySqlHelper.CreateInParam("@Sort",          MySqlDbType.VarChar,  50,     GetSort(Sort)   ),
                                   MySqlHelper.CreateOutParam("@RecordCount",  MySqlDbType.Int32,      4                       )
                                   };
            return parm;
        }
        /// <summary>
        /// Statistics data that in table
        /// </summary>
        /// <param name="TableName">table name</param>
        /// <param name="Columns">Statistics column</param>
        /// <param name="WhereSql">conditions</param>
        /// <returns>Set of parameters</returns>
        public static MySqlParameter[] GetCountParm(string TableName, string Columns, string WhereSql)
        {
            MySqlParameter[] parm = {
                                   MySqlHelper.CreateInParam("@TableName",     MySqlDbType.VarChar,  20,     TableName       ),
                                   MySqlHelper.CreateInParam("@CountColumn",  MySqlDbType.VarChar,  20,     Columns         ),
                                   MySqlHelper.CreateInParam("@WhereSql",      MySqlDbType.VarChar,  250,    WhereSql        ),
                                   MySqlHelper.CreateOutParam("@RecordCount",  MySqlDbType.Int32,      4                       )
                                   };
            return parm;
        }
        /// <summary>
        /// Get the sql that is Sorted 
        /// </summary>
        /// <param name="sort"> sort column and values</param>
        /// <returns>SQL sort string</returns>
        private static string GetSort(Hashtable sort)
        {
            string str = "";
            int i = 0;
            if (sort != null && sort.Count > 0)
            {
                foreach (DictionaryEntry de in sort)
                {
                    i++;
                    str += de.Key + " " + de.Value;
                    if (i != sort.Count)
                    {
                        str += ",";
                    }
                }
            }
            return str;
        }

        /// <summary>
        /// execute a trascation include one or more sql sentence(author:donne yin)
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="cmdType"></param>
        /// <param name="cmdTexts"></param>
        /// <param name="commandParameters"></param>
        /// <returns>execute trascation result(success: true | fail: false)</returns>
        public static bool ExecuteTransaction(string connectionString, CommandType cmdType, string[] cmdTexts, params MySqlParameter[][] commandParameters)
        {
            MySqlConnection myConnection = new MySqlConnection(connectionString);       //get the connection object
            myConnection.Open();                                                        //open the connection
            MySqlTransaction myTrans = myConnection.BeginTransaction();                 //begin a trascation
            MySqlCommand cmd = new MySqlCommand();
            cmd.Connection = myConnection;
            cmd.Transaction = myTrans;

            try
            {
                for (int i = 0; i < cmdTexts.Length; i++)
                {
                    PrepareCommand(cmd, myConnection, null, cmdType, cmdTexts[i], commandParameters[i]);
                    cmd.ExecuteNonQuery();
                    cmd.Parameters.Clear();
                }
                myTrans.Commit();
            }
            catch
            {
                myTrans.Rollback();
                return false;
            }
            finally
            {
                myConnection.Close();
            }
            return true;
        }





        #endregion


        #region 批量操作

        /// <summary>
        ///使用MySqlDataAdapter批量更新数据
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="table">数据表</param>
        public static void BatchUpdate(string connectionString, DataTable table)
        {
            MySqlConnection connection = new MySqlConnection(connectionString);

            MySqlCommand command = connection.CreateCommand();
            ////////////////command.CommandTimeout = CommandTimeOut;
            command.CommandType = CommandType.Text;
            MySqlDataAdapter adapter = new MySqlDataAdapter(command);
            MySqlCommandBuilder commandBulider = new MySqlCommandBuilder(adapter);
            commandBulider.ConflictOption = ConflictOption.OverwriteChanges;

            MySqlTransaction transaction = null;
            try
            {
                connection.Open();
                transaction = connection.BeginTransaction();
                //设置批量更新的每次处理条数
                adapter.UpdateBatchSize = BatchSize;
                //设置事物
                adapter.SelectCommand.Transaction = transaction;

                if (table.ExtendedProperties["SQL"] != null)
                {
                    adapter.SelectCommand.CommandText = table.ExtendedProperties["SQL"].ToString();
                }
                adapter.Update(table);
                transaction.Commit();/////提交事务
            }
            catch (MySqlException ex)
            {
                transaction?.Rollback();
                throw ex;
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }
        }

        /// <summary>
        ///大批量数据插入,返回成功插入行数
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="table">数据表</param>
        /// <returns>返回成功插入行数</returns>
        public static int BulkInsert(string connectionString, DataTable table)
        {
            if (string.IsNullOrEmpty(table.TableName)) throw new Exception("请给DataTable的TableName属性附上表名称");
            if (table.Rows.Count == 0) return 0;
            int insertCount;
            string tmpPath = Path.GetTempFileName();
            string csv = DataTableToCsv(table);
            File.WriteAllText(tmpPath, csv);
            using (MySqlConnection conn = new MySqlConnection(connectionString))
            {
                MySqlTransaction tran = null;
                try
                {
                    conn.Open();
                    tran = conn.BeginTransaction();
                    MySqlBulkLoader bulk = new MySqlBulkLoader(conn)
                    {
                        FieldTerminator = ",",
                        FieldQuotationCharacter = '"',
                        EscapeCharacter = '"',
                        LineTerminator = "\r\n",
                        FileName = tmpPath,
                        NumberOfLinesToSkip = 0,
                        TableName = table.TableName,
                    };
                    bulk.Columns.AddRange(table.Columns.Cast<DataColumn>().Select(colum => colum.ColumnName).ToList());
                    insertCount = bulk.Load();
                    tran.Commit();
                }
                catch (MySqlException ex)
                {
                    tran?.Rollback();
                    throw ex;
                }
            }
            File.Delete(tmpPath);
            return insertCount;
        }

        /// <summary>
        ///将DataTable转换为标准的CSV
        /// </summary>
        /// <param name="table">数据表</param>
        /// <returns>返回标准的CSV</returns>
        private static string DataTableToCsv(DataTable table)
        {
            //以半角逗号（即,）作分隔符，列为空也要表达其存在。
            //列内容如存在半角逗号（即,）则用半角引号（即""）将该字段值包含起来。
            //列内容如存在半角引号（即"）则应替换成半角双引号（""）转义，并用半角引号（即""）将该字段值包含起来。
            StringBuilder sb = new StringBuilder();
            foreach (DataRow row in table.Rows)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    var colum = table.Columns[i];
                    if (i != 0) sb.Append(",");
                    if (colum.DataType == typeof(string) && row[colum].ToString().Contains(","))
                    {
                        sb.Append("\"" + row[colum].ToString().Replace("\"", "\"\"") + "\"");
                    }
                    else sb.Append(row[colum]);
                }
                sb.AppendLine();
            }

            return sb.ToString();
        }

        #endregion 批量操作
    }
}
