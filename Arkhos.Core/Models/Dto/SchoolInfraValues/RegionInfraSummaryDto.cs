using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto.SchoolInfraValues;

public class RegionInfraSummaryDto
{
    [JsonPropertyName("ano")] public int Ano { get; set; }
    [JsonPropertyName("meso_id")] public int MesorregiaoId { get; set; }
    [JsonPropertyName("micro_id")] public int MicrorregiaoId { get; set; }
    [JsonPropertyName("municipio_id")] public int MunicipioId { get; set; }

    [JsonPropertyName("avg_wellbeing")] public double AvgWellbeingRating { get; set; }
    [JsonPropertyName("avg_pedagogical")] public double AvgPedagogicalRating { get; set; }
}