namespace Arkhos.Core.Models;

public class SchoolInfo
{
    public int Id {get;set;}
    public int IdEscola { get; set; }
    public string NomeEscola {get;set;} = string.Empty;
    public short Dependencia {get;set;}
    public short? Localizacao {get;set;}
    public short Funcionamento {get;set;}
    public int? Sede {get;set;}
    public short Alocacao {get;set;}
    public short Ocupacao {get;set;}
    public int Ano {get;set;}
    public string Endereco {get;set;} = string.Empty;
    public string Telefone {get;set;} = string.Empty;

    public double? Lat {get;set;}

    public double? Lon {get;set;}

    public SchoolRating SchoolRating {get;set;} = null!;
    public int CityInfoId {get;set;}
    public CityInfo CityInfo {get;set;} = null!;
}