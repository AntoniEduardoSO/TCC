namespace Arkhos.Core.Models;

public class CityInfo
{
    public long Id {get;set;}

    public long MunicipioId {get;set;}
    public long NomeMunicipio {get;set;}
    public long Ano {get;set;}
    public string NomeMesorregiao {get;set;} = string.Empty;
    public long IdMesorregiao {get;set;}
    public string NomeMicrorregiao {get;set;} = string.Empty;
    public long IdMicrorregiao {get;set;}
    public long AreaTerritorial {get;set;}
    public long PopulacaoTotal {get;set;}
    public double DensidadeDemografica {get;set;}
    public ICollection<SchoolInfo> SchoolInfos {get;} = [];
}