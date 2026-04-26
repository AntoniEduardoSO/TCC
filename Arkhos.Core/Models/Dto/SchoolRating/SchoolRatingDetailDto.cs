using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto.SchoolRating;

public class SchoolRatingDetailDto
{
    [JsonPropertyName("escola_id")] public int EscolaId { get; set; }
    [JsonPropertyName("nome_escola")] public string NomeEscola { get; set; } = string.Empty;
    [JsonPropertyName("ano")] public int Ano { get; set; }

    [JsonPropertyName("spending_student")] public double SpendingPerStudent { get; set; }
    [JsonPropertyName("spending_teacher")] public double SpendingPerTeacher { get; set; }
    
    [JsonPropertyName("approval_rate")] public double? ApprovalRate { get; set; }
    [JsonPropertyName("failure_rate")] public double? FailureRate { get; set; }
    [JsonPropertyName("dropout_rate")] public double? DropoutRate { get; set; }

    [JsonPropertyName("accessibility_rating")] public double AccessibilityRating { get; set; }
}