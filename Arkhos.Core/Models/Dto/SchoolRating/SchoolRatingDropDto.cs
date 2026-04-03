using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto.SchoolRating;
public class SchoolRatingDropDto
{
    [JsonPropertyName("ano")]
    public  int Ano {get;set;}
    [JsonPropertyName("escola_id")]
    public int SchoolInfoId { get; set; }
    [JsonPropertyName("aproval_rate")]
    public double? ApprovalRate {get;set;}
    [JsonPropertyName("failure_rate")]
    public double? FailureRate {get;set;}
    [JsonPropertyName("dropout_rate")]
    public double? DropoutRate {get;set;}
    
    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}

}