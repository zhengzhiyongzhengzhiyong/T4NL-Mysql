

namespace T4NL.Common
{
    public class JsonParam
    {
        /// <summary>
        /// 成功=ok ;失败=no
        /// </summary>
        private string status;

        private string _message;
        public string Message
        {
            get { return _message.Replace("'", "\\'"); }
            set { _message = value; }
        }

        public object Data { get; set; }
        public string Status { get { return status.ToLower(); } set { status = value; } }
        public string Layer { get { return LayerMessage(); } }

        public bool IsSuccess { get => isSuccess; }

        private bool isSuccess;

        public JsonParam()
        {
            this.Status = "no";
            isSuccess = false;
        }

        /// <summary>
        /// 设置失败
        /// </summary>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        public void SetDefeate(bool _IsTranslate = true)
        {
            SetDefeate("操作失败！", _IsTranslate);
        }
        /// <summary>
        /// 设置成功
        /// </summary>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        public void SetSuccess(bool _IsTranslate = true)
        {
            SetSuccess("Success", "", _IsTranslate);
        }
        /// <summary>
        /// 设置成功
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        public void SetSuccess(string msg, bool _IsTranslate = true)
        {
            SetSuccess(msg, "", _IsTranslate);
        }
        /// <summary>
        /// 设置成功
        /// </summary>
        /// <param name="data"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        public void SetSuccess(object data, bool _IsTranslate = true)
        {
            SetSuccess("Success", data, _IsTranslate);
        }
        /// <summary>
        /// 设置成功
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="data"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        public void SetSuccess(string msg, object data, bool _IsTranslate = true)
        {
            this.Status = "ok";
            isSuccess = true;
            this.Message = LangString(msg, _IsTranslate);
            this.Data = data;
        }
        /// <summary>
        /// 设置失败
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="_IsTranslate">是否进行翻译（默认翻译）</param>
        public void SetDefeate(string msg, bool _IsTranslate = true)
        {
            this.Message = LangString(msg, _IsTranslate);
            this.isSuccess = false;
        }

        string LayerMessage()
        {
            string p;
            if (Status == "ok")
            {
                //p = string.Format("layui.use('layer', function () { layer.msg('{0}', { icon: 0 } ); });", Message);
                p = "layui.use('layer', function () { var layer=layui.layer; layer.msg('" + Message + "', { icon: 0 } ); });";
            }
            else
            {
                //p =string.Format("layui.use('layer', function () { layer.msg('{0}', { icon: 2 } ); });",Message);
                p = "layui.use('layer', function () { var layer=layui.layer; layer.msg('" + Message + "', { icon: 2 } ); });";
            }

            return p;
        }
        public string LangString(string key, bool _IsTranslate = true)
        {
            if (!_IsTranslate) return key;
            return key;
        }
    }
}
