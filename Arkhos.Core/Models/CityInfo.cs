namespace Arkhos.Core.Models;

public class CityInfo
{
    public int Id {get;set;}
    public int MunicipioId {get;set;}
    public string NomeMunicipio {get;set;} = string.Empty;
    public int Ano {get;set;}
    public string NomeMesorregiao {get;set;} = string.Empty;
    public int IdMesorregiao {get;set;}
    public string NomeMicrorregiao {get;set;} = string.Empty;
    public int IdMicrorregiao {get;set;}
    public int AreaTerritorial {get;set;}
    public int PopulacaoTotal {get;set;}
    public double DensidadeDemografica {get;set;}
    public ICollection<SchoolInfo> SchoolInfos {get;} = [];
}