using System;
using System.Data;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using System.Text;

namespace WebApp
{
    public partial class MainPage : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                // Set default dates
                txtStartDate.Text = DateTime.Now.AddMonths(-1).ToString("yyyy-MM-dd");
                txtEndDate.Text = DateTime.Now.ToString("yyyy-MM-dd");
                
                // Load initial data
                LoadData();
            }
        }
        
        protected void btnSearch_Click(object sender, EventArgs e)
        {
            LoadData();
        }
        
        private void LoadData()
        {
            try
            {
                DateTime startDate = DateTime.Parse(txtStartDate.Text);
                DateTime endDate = DateTime.Parse(txtEndDate.Text);
                
                // Call data access layer
                DataAccess.DataAccess da = new DataAccess.DataAccess();
                DataTable dt = da.GetRecords(startDate, endDate);
                
                gvResults.DataSource = dt;
                gvResults.DataBind();
                
                lblMessage.Text = $"Found {dt.Rows.Count} records";
                lblMessage.CssClass = "message success";
                lblMessage.Visible = true;
            }
            catch (Exception ex)
            {
                lblMessage.Text = "Error loading data: " + ex.Message;
                lblMessage.CssClass = "message error";
                lblMessage.Visible = true;
            }
        }
        
        protected void gvResults_PageIndexChanging(object sender, GridViewPageEventArgs e)
        {
            gvResults.PageIndex = e.NewPageIndex;
            LoadData();
        }
        
        protected void gvResults_Sorting(object sender, GridViewSortEventArgs e)
        {
            // Implement sorting logic
            LoadData();
        }
        
        protected void btnExportExcel_Click(object sender, EventArgs e)
        {
            try
            {
                Response.Clear();
                Response.Buffer = true;
                Response.AddHeader("content-disposition", "attachment;filename=Export.xls");
                Response.Charset = "";
                Response.ContentType = "application/vnd.ms-excel";
                
                StringWriter sw = new StringWriter();
                HtmlTextWriter hw = new HtmlTextWriter(sw);
                
                gvResults.RenderControl(hw);
                
                Response.Output.Write(sw.ToString());
                Response.Flush();
                Response.End();
            }
            catch (Exception ex)
            {
                lblMessage.Text = "Error exporting: " + ex.Message;
                lblMessage.CssClass = "message error";
                lblMessage.Visible = true;
            }
        }
        
        protected void btnExportPDF_Click(object sender, EventArgs e)
        {
            // Implement PDF export
            lblMessage.Text = "PDF export functionality to be implemented";
            lblMessage.CssClass = "message info";
            lblMessage.Visible = true;
        }
        
        public override void VerifyRenderingInServerForm(Control control)
        {
            // Required for export
        }
    }
}
