using System.Diagnostics.Contracts;

namespace Arkhos.Core.Models;
public class SchoolEnrollValues
{
    public int Ano { get; set; }
    public int IdEscolaEnrollValues { get; set; }
    public int AtributoId {get;set;}
    public string TipoAtributo {get;set;} = string.Empty;
    public double Valor {get;set;}

    public SchoolInfo SchoolInfo { get; set; } = null!;
    public SchoolEnrollDict SchoolEnrollDict {get;set;} = null!;
}