using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto;
public class SchoolRatingSpendingDto
{
    public int Ano {get;set;}
    public int SchoolInfoId { get; set; }
    public double SpendingPerStudent {get;set;}
    public double SpendingPerTeacher {get;set;}
    public double PedagogicalSpendingPerStudent {get;set;}
    public double InfrastructureSpendingPerStudent {get;set;}
    public double MealSpendingPerStudent {get;set;}
    public double TransportSpendingPerStudent {get;set;}

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}
}