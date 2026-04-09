namespace Arkhos.Core.Models;
public class SchoolInfraDict
{
    public int Id { get; set; }
    public string Variavel {get;set;} = string.Empty;
    public string Descricao {get;set;} = string.Empty;
    public string Tipo {get;set;} = string.Empty;
    public int Tamanho {get;set;}
    public string Grupo {get;set;} = string.Empty;

    public ICollection<SchoolInfraValues> SchoolInfraValues {get;set;} = [];
}