using System.Text.Json.Serialization;
using Arkhos.Core.Interfaces;

namespace Arkhos.Core.Models.Dto.SchoolInfraValues;

public class SchoolInfraValuesWellbeingDto : ILocationContext 
{
    [JsonPropertyName("escola_id")]
    public int EscolaId { get; set; }

    [JsonPropertyName("ano")]
    public int Ano { get; set; }

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId { get; set; }
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId { get; set; }

    [JsonPropertyName("municipio_id")]
    public int MunicipioId { get; set; }
    [JsonPropertyName("alimentacao_escolar")]
    public double AlimentacaoEscolar { get; set; }
    [JsonPropertyName("refeitorio")]
    public double Refeitorio { get; set; }
    [JsonPropertyName("cozinha")]
    public double Cozinha { get; set; }
    [JsonPropertyName("despensa")]
    public double Despensa { get; set; }
    [JsonPropertyName("terreirao")]
    public double Terreirao { get; set; }
    [JsonPropertyName("patio_coberto")]
    public double PatioCoberto { get; set; }
    [JsonPropertyName("patio_descoberto")]
    public double PatioDescoberto { get; set; }
    [JsonPropertyName("quadra_esportes")]
    public double QuadraEsportes { get; set; }
    [JsonPropertyName("area_verde")]
    public double AreaVerde { get; set; }
    [JsonPropertyName("parque_infantil")]
    public double ParqueInfantil { get; set; }
    [JsonPropertyName("piscina")]
    public double Piscina { get; set; }
    [JsonPropertyName("sala_repouso_aluno")]
    public double SalaRepousoAluno { get; set; }
    [JsonPropertyName("banheiro_alunos")]
    public double BanheiroAlunos { get; set; }
    [JsonPropertyName("banheiro_educacao_infantil")]
    public double BanheiroEducacaoInfantil { get; set; }
}