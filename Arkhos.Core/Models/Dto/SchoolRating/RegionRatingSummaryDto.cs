using System.Text.Json.Serialization;
using Arkhos.Core.Interfaces;

namespace Arkhos.Core.Models.Dto.SchoolRating;

public class RegionRatingSummaryDto : ILocationContext
{
    [JsonPropertyName("ano")] public int Ano { get; set; }
    [JsonPropertyName("meso_id")] public int MesorregiaoId { get; set; }
    [JsonPropertyName("micro_id")] public int MicrorregiaoId { get; set; }
    [JsonPropertyName("municipio_id")] public int MunicipioId { get; set; }

    // Médias Financeiras
    [JsonPropertyName("avg_spending_student")] public double AvgSpendingPerStudent { get; set; }
    [JsonPropertyName("avg_spending_teacher")] public double AvgSpendingPerTeacher { get; set; }
    [JsonPropertyName("avg_infra_spending")] public double AvgInfrastructureSpending { get; set; }

    // Médias de Taxas
    [JsonPropertyName("approval_rate")] public double? ApprovalRate { get; set; }
    [JsonPropertyName("failure_rate")] public double? FailureRate { get; set; }
    [JsonPropertyName("dropout_rate")] public double? DropoutRate { get; set; }

    [JsonPropertyName("avg_accessibility")] public double AvgAccessibilityRating { get; set; }
}