namespace Arkhos.Web.Models;
public class InfoDialogData
{
    public string Title {get;set;} = string.Empty;

    public string FormulaLatex {get;set;} = string.Empty;

    public List<DefinitionItem> Definitions {get;set;} = [];
    public string Explanation {get;set;} = string.Empty;
}