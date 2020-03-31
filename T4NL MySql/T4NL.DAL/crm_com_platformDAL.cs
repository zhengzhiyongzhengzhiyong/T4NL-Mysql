 using System.Text;
 using System.Collections.Generic; 
 using System.Data;
 using T4NL.Model;
 using T4NL.Common;
 using MySql.Data.MySqlClient;
 using MySqlHelper = T4NL.Common.MySqlHelper;
 
 namespace T4NL.DAL
 {
      public partial class crm_com_platformDAL
      {
	    
		private MySqlDataReader _reader;

		/// <summary>
        /// 判断是否存在
        /// </summary>
	    public bool Exists(int Id)
		{
			StringBuilder strSql=new StringBuilder();
			strSql.Append("select count(1) from crm_com_platform");
			strSql.Append(" where Id=@Id");

			MySqlParameter[] parameters = {
                    new MySqlParameter("@Id", MySqlDbType.Int32,11)
            };

			parameters[0].Value = Id;
           
		    return int.Parse(MySqlHelper.ExecuteScalar(BasicConfig.ConnectionString, CommandType.Text, strSql.ToString(), parameters).ToString()) > 0 ? true : false;
		}

	    /// <summary>
		/// 增加一条数据
		/// </summary>
		public bool Add(crm_com_platform model)
		{
			StringBuilder strSql=new StringBuilder();
			strSql.Append("insert into crm_com_platform(");			
            strSql.Append(" ComPlatformValue,Type,IsSelected,UTime,CTime");
			strSql.Append(") values (");
			strSql.Append(" @ComPlatformValue,@Type,@IsSelected,@UTime,@CTime");
            strSql.Append(") ");
			strSql.Append(";select @@IDENTITY");

		    MySqlParameter[] parameters = {
			               new MySqlParameter("@ComPlatformValue", MySqlDbType.Double),            
                           new MySqlParameter("@Type", MySqlDbType.Int32,11),            
                           new MySqlParameter("@IsSelected", MySqlDbType.Int32,11),            
                           new MySqlParameter("@UTime", MySqlDbType.DateTime),            
                           new MySqlParameter("@CTime", MySqlDbType.DateTime)            
              
            };

		    			parameters[0].Value = model.ComPlatformValue;          
            			parameters[1].Value = model.Type;          
            			parameters[2].Value = model.IsSelected;          
            			parameters[3].Value = model.UTime;          
            			parameters[4].Value = model.CTime;          
              

			return MySqlHelper.ExecuteNonQuery(BasicConfig.ConnectionString, CommandType.Text, strSql.ToString(), parameters)>0;
		}

	    /// <summary>
		/// 更新一条数据
		/// </summary>
		public bool Update(crm_com_platform model)
		{
		    StringBuilder strSql=new StringBuilder();
			strSql.Append("update crm_com_platform set ");

						strSql.Append(" ComPlatformValue = @ComPlatformValue, ");           
            			strSql.Append(" Type = @Type, ");           
            			strSql.Append(" IsSelected = @IsSelected, ");           
            			strSql.Append(" UTime = @UTime, ");           
            			strSql.Append(" CTime = @CTime, ");           
            			strSql.Append(" where Id = @Id");

		    MySqlParameter[] parameters = {
			               new MySqlParameter("@Id", MySqlDbType.Int32,11),            
                           new MySqlParameter("@ComPlatformValue", MySqlDbType.Double),            
                           new MySqlParameter("@Type", MySqlDbType.Int32,11),            
                           new MySqlParameter("@IsSelected", MySqlDbType.Int32,11),            
                           new MySqlParameter("@UTime", MySqlDbType.DateTime),            
                           new MySqlParameter("@CTime", MySqlDbType.DateTime)            
              
            };

		    			parameters[0].Value = model.Id;          
            			parameters[1].Value = model.ComPlatformValue;          
            			parameters[2].Value = model.Type;          
            			parameters[3].Value = model.IsSelected;          
            			parameters[4].Value = model.UTime;          
            			parameters[5].Value = model.CTime;          
             

		    return MySqlHelper.ExecuteNonQuery(BasicConfig.ConnectionString, CommandType.Text, strSql.ToString(), parameters)>0;
		}

		/// <summary>
		/// 删除一条数据
		/// </summary>
		public bool Delete(int Id)
		{
			
			StringBuilder strSql=new StringBuilder();
			strSql.Append("delete from crm_com_platform ");
			strSql.Append(" where Id=@Id ");
						MySqlParameter[] parameters = {
					new MySqlParameter("@Id", MySqlDbType.Int32,11)			};
			parameters[0].Value = Id;

            return MySqlHelper.ExecuteNonQuery(BasicConfig.ConnectionString, CommandType.Text, strSql.ToString(), parameters)>0;
		}


		/// <summary>
		/// 得到一个对象实体
		/// </summary>
		public crm_com_platform GetModel(int Id)
		{
			
			StringBuilder strSql=new StringBuilder();
			strSql.Append("select  Id,ComPlatformValue,Type,IsSelected,UTime,CTime  ");			
			strSql.Append("  from crm_com_platform ");
			strSql.Append(" where Id=@Id ");
						MySqlParameter[] parameters = {
					new MySqlParameter("@Id", MySqlDbType.Int32,11)			};
			parameters[0].Value =Id;

			_reader = MySqlHelper.ExecuteReader(BasicConfig.ConnectionString, CommandType.Text, strSql.ToString(), parameters);

			crm_com_platform model =null;
			while (_reader.Read())
            {
			    model = new crm_com_platform{
				   
				       Id=_reader["Id"].ToInt_V(),				   
				       ComPlatformValue=_reader["ComPlatformValue"].ToDouble_V(),				   
				       Type=_reader["Type"].ToInt_V(),				   
				       IsSelected=_reader["IsSelected"].ToInt_V(),				   
				       UTime=_reader["UTime"].ToDateTime_V(),				   
				       CTime=_reader["CTime"].ToDateTime_V()				   
				};
			}

			return model;
	    }

		/// <summary>
		/// 分页
		/// </summary>
		public List<crm_com_platform> GetPage(string orderName, string order, int offset, int pageSize, string stime, string etime, string search,out int total)
		{
			
			StringBuilder strSql=new StringBuilder();
			strSql.Append("select  Id,ComPlatformValue,Type,IsSelected,UTime,CTime  ");			
			strSql.Append("  from crm_com_platform ");
			 
			strSql.Append($" where  UTime>='{stime}' AND UTime<'{etime}' "); 
						strSql.Append($"  ORDER BY {orderName} {order} LIMIT {offset},{pageSize}; ");

		    StringBuilder strSqlCount=new StringBuilder();
		    strSqlCount.Append("  SELECT COUNT(1) FROM crm_com_platform ");
		     
		    strSqlCount.Append($" where  UTime>='{stime}' AND UTime<'{etime}' "); 
		    
			_reader = MySqlHelper.ExecuteReader(BasicConfig.ConnectionString, CommandType.Text, strSql.ToString());

			List<crm_com_platform> list = new List<crm_com_platform>();
			while (_reader.Read())
            {
			    list.Add(new crm_com_platform{
				   
				       Id=_reader["Id"].ToInt_V(),				   
				       ComPlatformValue=_reader["ComPlatformValue"].ToDouble_V(),				   
				       Type=_reader["Type"].ToInt_V(),				   
				       IsSelected=_reader["IsSelected"].ToInt_V(),				   
				       UTime=_reader["UTime"].ToDateTime_V(),				   
				       CTime=_reader["CTime"].ToDateTime_V()				   
				});
			}
			total = int.Parse(MySqlHelper.ExecuteScalar(MySqlHelper.ConnectionString, CommandType.Text, strSqlCount.ToString()).ToString());;
			return list;
	    }


      }
 }

  