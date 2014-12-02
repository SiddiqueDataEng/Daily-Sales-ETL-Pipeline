<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="MainPage.aspx.cs" Inherits="WebApp.MainPage" %>

<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title>MainPage</title>
    <link href="~/Styles/Site.css" rel="stylesheet" type="text/css" />
    <script src="Scripts/jquery-1.11.1.min.js" type="text/javascript"></script>
</head>
<body>
    <form id="form1" runat="server">
        <div class="page-header">
            <h1>MainPage</h1>
            <p>Automated nightly batch loads</p>
        </div>
        
        <div class="content">
            <asp:Panel ID="SearchPanel" runat="server" CssClass="search-panel">
                <h3>Search Criteria</h3>
                <table>
                    <tr>
                        <td>Start Date:</td>
                        <td><asp:TextBox ID="txtStartDate" runat="server" TextMode="Date"></asp:TextBox></td>
                        <td>End Date:</td>
                        <td><asp:TextBox ID="txtEndDate" runat="server" TextMode="Date"></asp:TextBox></td>
                        <td><asp:Button ID="btnSearch" runat="server" Text="Search" OnClick="btnSearch_Click" CssClass="button" /></td>
                    </tr>
                </table>
            </asp:Panel>
            
            <asp:Panel ID="ResultsPanel" runat="server" CssClass="results-panel">
                <h3>Results</h3>
                <asp:GridView ID="gvResults" runat="server" 
                    AutoGenerateColumns="False"
                    CssClass="grid-view"
                    AllowPaging="True"
                    PageSize="20"
                    AllowSorting="True"
                    OnPageIndexChanging="gvResults_PageIndexChanging"
                    OnSorting="gvResults_Sorting">
                    <Columns>
                        <asp:BoundField DataField="RecordID" HeaderText="ID" SortExpression="RecordID" />
                        <asp:BoundField DataField="RecordName" HeaderText="Name" SortExpression="RecordName" />
                        <asp:BoundField DataField="RecordDate" HeaderText="Date" SortExpression="RecordDate" DataFormatString="{0:MM/dd/yyyy}" />
                        <asp:BoundField DataField="Amount" HeaderText="Amount" SortExpression="Amount" DataFormatString="{0:C}" />
                        <asp:BoundField DataField="Status" HeaderText="Status" SortExpression="Status" />
                        <asp:TemplateField HeaderText="Actions">
                            <ItemTemplate>
                                <asp:LinkButton ID="lnkEdit" runat="server" Text="Edit" CommandName="Edit" CommandArgument='<%# Eval("RecordID") %>' />
                                <asp:LinkButton ID="lnkDelete" runat="server" Text="Delete" CommandName="Delete" CommandArgument='<%# Eval("RecordID") %>' OnClientClick="return confirm('Are you sure?');" />
                            </ItemTemplate>
                        </asp:TemplateField>
                    </Columns>
                    <PagerStyle CssClass="pager" />
                    <HeaderStyle CssClass="header" />
                    <RowStyle CssClass="row" />
                    <AlternatingRowStyle CssClass="alt-row" />
                </asp:GridView>
                
                <asp:Label ID="lblMessage" runat="server" CssClass="message" Visible="false"></asp:Label>
            </asp:Panel>
            
            <asp:Panel ID="ExportPanel" runat="server" CssClass="export-panel">
                <asp:Button ID="btnExportExcel" runat="server" Text="Export to Excel" OnClick="btnExportExcel_Click" CssClass="button" />
                <asp:Button ID="btnExportPDF" runat="server" Text="Export to PDF" OnClick="btnExportPDF_Click" CssClass="button" />
            </asp:Panel>
        </div>
    </form>
</body>
</html>
