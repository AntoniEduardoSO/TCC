using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto.SchoolInfraValues;

public class SchoolInfraDetailDto
{
    [JsonPropertyName("escola_id")] public int EscolaId { get; set; }
    [JsonPropertyName("nome_escola")] public string NomeEscola { get; set; } = string.Empty;
    [JsonPropertyName("ano")] public int Ano { get; set; }

    [JsonPropertyName("wellbeing_rating")] public double WellbeingRating { get; set; }
    [JsonPropertyName("pedagogical_rating")] public double PedagogicalRating { get; set; }
}