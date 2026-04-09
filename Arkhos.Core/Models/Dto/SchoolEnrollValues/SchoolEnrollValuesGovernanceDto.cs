using System.Text.Json.Serialization;
using Arkhos.Core.Interfaces;

namespace Arkhos.Core.Models.Dto.SchoolEnrollValues;
public class SchoolEnrollValuesGovernanceDto : ILocationContext
{
    [JsonPropertyName("ano")]
    public int Ano { get; set; }
    [JsonPropertyName("escola_id")]
    public int EscolaId {get;set;}

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}
    [JsonPropertyName("psicologo")]
    public double Psicologo {get;set;}
    [JsonPropertyName("fonaudiologo")]
    public double Fonaudiologo {get;set;}
    [JsonPropertyName("assistente_social")]
    public double AssistenteSocial {get;set;}
    [JsonPropertyName("tradutor_libras")]
    public double TradutorLibras {get;set;}
    [JsonPropertyName("associacao_pais_e_mestres")]
    public double AssociacaoPaiMestres {get;set;}
    [JsonPropertyName("conselho_escolar")]
    public double ConselhoEscolar {get;set;}
    [JsonPropertyName("gremio_estudantil")]
    public double GremioEstudantil {get;set;}
}