using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto;
public class CityInfoMapDto
{
    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}
    [JsonPropertyName("municipio_nome")]
    public string NomeMunicipio {get;set;} = string.Empty;
    [JsonPropertyName("ano")]
    public int Ano {get;set;}
    [JsonPropertyName("meso_nome")]
    public string NomeMesorregiao {get;set;} = string.Empty;
    [JsonPropertyName("meso_id")]
    public int IdMesorregiao {get;set;}
    [JsonPropertyName("micro_nome")]
    public string NomeMicrorregiao {get;set;} = string.Empty;
    [JsonPropertyName("micro_id")]
    public int IdMicrorregiao {get;set;}   
}