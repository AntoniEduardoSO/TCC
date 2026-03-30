using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto;
public class SchoolRatingDropDto
{
    public  int Ano {get;set;}
    public int SchoolInfoId { get; set; }
    public double? ApprovalRate {get;set;}
    public double? FailureRate {get;set;}
    public double? DropoutRate {get;set;}
    
    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}

}