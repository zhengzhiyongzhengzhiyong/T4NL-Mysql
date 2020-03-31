using System.Web.Mvc;

namespace T4NL.Common
{
    public class JsonModel
    {
        /// <summary>
        /// 成功=ok ;失败=no
        /// </summary>
        private JsonParam res;
        private JsonResult jsonResult;
        public JsonModel()
        {
            res = new JsonParam();
            jsonResult = new JsonResult() { JsonRequestBehavior = JsonRequestBehavior.AllowGet };
        }
        /// <summary>
        /// 设置失败 *支持多语言*
        /// </summary>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        /// <returns></returns>
        public JsonResult Defeate(bool _IsTranslate = true)
        {
            return Defeate("操作失败！", _IsTranslate);
        }
        /// <summary>
        /// 设置成功 *支持多语言*
        /// </summary>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        /// <returns></returns>
        public JsonResult Success(bool _IsTranslate = true)
        {
            res.SetSuccess(_IsTranslate);
            jsonResult.Data = res;
            return jsonResult;
        }
        /// <summary>
        /// 设置成功 *支持多语言*
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        /// <returns></returns>
        public JsonResult Success(string msg, bool _IsTranslate = true)
        {
            res.SetSuccess(msg, _IsTranslate);
            jsonResult.Data = res;
            return jsonResult;
        }
        /// <summary>
        /// 设置成功 *支持多语言*
        /// </summary>
        /// <param name="data"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        /// <returns></returns>
        public JsonResult Success(object data, bool _IsTranslate = true)
        {
            res.SetSuccess(data, _IsTranslate);
            jsonResult.Data = res;
            return jsonResult;
        }
        /// <summary>
        /// 设置成功 *支持多语言*
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="data"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        /// <returns></returns>
        public JsonResult Success(string msg, object data, bool _IsTranslate = true)
        {
            res.SetSuccess(msg, data, _IsTranslate);
            jsonResult.Data = res;
            return jsonResult;
        }

        /// <summary>
        /// 设置失败 *支持多语言*
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        /// <returns></returns>
        public JsonResult Defeate(string msg, bool _IsTranslate = true)
        {
            res.SetDefeate(msg, _IsTranslate);
            jsonResult.Data = res;
            return jsonResult;
        }
        /// <summary>
        /// 发生异常 *支持多语言*
        /// </summary>
        /// <returns></returns>
        public JsonResult Exception()
        {
            return Defeate("服务器错误！");
        }
        /// <summary>
        /// 自定义响应结果 *支持多语言*
        /// </summary>
        /// <param name="param"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        /// <returns></returns>
        public JsonResult SetResult(JsonParam param, bool _IsTranslate = true)
        {
            if (param == null) return Defeate(_IsTranslate);
            jsonResult.Data = param;
            return jsonResult;
        }


    }
}
