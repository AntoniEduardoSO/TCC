namespace Arkhos.Core.Models.Dto.SchoolInfo;

using System.Text.Json.Serialization;
public class SchoolInfoMapDto
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

    public double? Lat {get;set;}
    public double? Lon {get;set;}
}