namespace Arkhos.Core.Models;
public class SchoolInfraValues
{
    public int Ano { get; set; }
    public int IdEscolaInfraValues { get; set; }
    public int AtributoId {get;set;}
    public string TipoAtributo {get;set;} = string.Empty;
    public double Valor {get;set;}

    public SchoolInfraDict SchoolInfraDict {get;set;} = null!;
    public SchoolInfo SchoolInfo { get; set; } = null!;
}