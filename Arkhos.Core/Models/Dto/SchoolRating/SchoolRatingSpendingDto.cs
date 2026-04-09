using System.Text.Json.Serialization;
using Arkhos.Core.Interfaces;

namespace Arkhos.Core.Models.Dto.SchoolRating;
public class SchoolRatingSpendingDto : ILocationContext
{
    [JsonPropertyName("ano")]
    public int Ano {get;set;}
    [JsonPropertyName("escola_id")]
    public int SchoolInfoId { get; set; }
    [JsonPropertyName("spending_per_student")]
    public double SpendingPerStudent {get;set;}
    [JsonPropertyName("spending_per_teacher")]
    public double SpendingPerTeacher {get;set;}
    [JsonPropertyName("spending_pedagogical_per_student")]
    public double PedagogicalSpendingPerStudent {get;set;}
    [JsonPropertyName("spending_infrastructure_per_student")]
    public double InfrastructureSpendingPerStudent {get;set;}
    [JsonPropertyName("spending_meal_per_student")]
    public double MealSpendingPerStudent {get;set;}
    [JsonPropertyName("spending_transport_per_student")]
    public double TransportSpendingPerStudent {get;set;}

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}
}