using System.Diagnostics.Contracts;

namespace Arkhos.Core.Models;
public class SchoolEnrollValues
{
    public int Id {get;set;}
    public int Ano { get; set; }
    public int IdEscolaValues { get; set; }
    public SchoolInfo SchoolInfo { get; set; } = null!;

    public int AtributoId {get;set;}
    public SchoolEnrollDict SchoolEnrollDict {get;set;} = null!;

    public string TipoAtributo {get;set;} = string.Empty;

    public double Valor {get;set;}

}