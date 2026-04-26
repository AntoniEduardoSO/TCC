namespace Arkhos.Core.Models.Dto.SchoolInfo;

using System.Text.Json.Serialization;
using Arkhos.Core.Interfaces;

public class SchoolInfoMapDto : ILocationContext
{
    [JsonPropertyName("escola_id")]
    public int IdEscola { get; set; }
    [JsonPropertyName("escola_nome")]
    public string NomeEscola { get; set; } = string.Empty;
    [JsonPropertyName("escola_endereco")]
    public string Endereco { get; set; } = string.Empty;
    [JsonPropertyName("ano")]
    public int Ano { get; set; }

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}
    [JsonPropertyName("dependencia")]
    public int Dependencia { get; set; }
    public short Localizacao { get; set; }

    public double? Lat {get;set;}
    public double? Lon {get;set;}
    public string NomeMunicipio { get; set; } = string.Empty;
    public string NomeMicrorregiao { get; set; } = string.Empty;
    public string NomeMesorregiao { get; set; } = string.Empty;
}