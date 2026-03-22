namespace Arkhos.Core.Models.Dto;

using System.Text.Json.Serialization;
public class SchoolInfoMapDto
{
    public int IdEscola { get; set; }
    public string NomeEscola { get; set; } = string.Empty;
    public string Endereco { get; set; } = string.Empty;
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