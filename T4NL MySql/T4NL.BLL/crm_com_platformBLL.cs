
 using System.Collections.Generic; 
 using T4NL.Model;
 using T4NL.DAL;
 
 namespace T4NL.BLL
 {
      public partial class crm_com_platformBLL
      {
	    
		private readonly crm_com_platformDAL dal=new crm_com_platformDAL();
		 
		/// <summary>
		/// 是否存在该记录
		/// </summary>
		public bool Exists(int Id)
		{
			return dal.Exists(Id);
		}

	    /// <summary>
		/// 增加一条数据
		/// </summary>
		public bool Add(crm_com_platform model)
		{
			return dal.Add(model);	
		}

	    /// <summary>
		/// 更新一条数据
		/// </summary>
		public bool Update(crm_com_platform model)
		{
			return dal.Update(model);	
		}

		/// <summary>
		/// 删除一条数据
		/// </summary>
		public bool Delete(int Id)
		{
			return dal.Delete(Id);
		}

	    /// <summary>
		/// 得到一个对象实体
		/// </summary>
		public crm_com_platform GetModel(int Id)
		{
			return dal.GetModel(Id);
		}

	    /// <summary>
		/// 分页
		/// </summary>
		public List<crm_com_platform> GetPage(string orderName, string order, int offset, int pageSize, string stime, string etime, string search,out int total)
		{
		   return dal.GetPage(orderName,order, offset,pageSize,stime, etime, search,out total);
		}

   
      }
 }
   