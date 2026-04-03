using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto.SchoolRating;
public class SchoolRatingMapDto
{
    [JsonPropertyName("school_id")]
    public int SchoolInfoId { get; set; }

    [JsonPropertyName("ano")]
    public int Ano {get;set;}
    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}
}