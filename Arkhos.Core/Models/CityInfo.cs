namespace Arkhos.Core.Models;

public class CityInfo
{
    public long Id {get;set;}

    public string NomeMunicipio {get;set;} = string.Empty;

    public string NomeMesorregiao {get;set;} = string.Empty;

    public long IdMesorregiao {get;set;}

    public string NomeMicrorregiao {get;set;} = string.Empty;

    public long IdMicrorregiao {get;set;}
}