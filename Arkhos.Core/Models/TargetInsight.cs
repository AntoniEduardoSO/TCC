namespace Arkhos.Core.Models;
public class TargetInsight
{
    public int Id {get;set; }
    public string Axis {get;set;} = string.Empty;
    public string Level {get;set;} = string.Empty;
    public int Ano {get;set;}
    public string TipoInsight {get;set;} = string.Empty;    
    public string Titulo {get;set;} = string.Empty;
    public string ValorDestaque {get;set;} = string.Empty;
    public string Descricao {get;set;} = string.Empty;     
    public string Recomendacao {get;set;} = string.Empty;
    public double ValorBaseline {get;set;}
    public int IdAlvo {get;set;}
}