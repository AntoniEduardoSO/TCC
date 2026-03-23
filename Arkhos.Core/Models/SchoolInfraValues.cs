namespace Arkhos.Core.Models;
public class SchoolInfraValues
{
    public int Id {get;set;}
    public int Ano { get; set; }
    public int IdEscolaInfraValues { get; set; }
    public SchoolInfo SchoolInfo { get; set; } = null!;

    public int AtributoId {get;set;}
    public SchoolInfraDict SchoolInfraDict {get;set;} = null!;

    public string TipoAtributo {get;set;} = string.Empty;

    public double Valor {get;set;}
}