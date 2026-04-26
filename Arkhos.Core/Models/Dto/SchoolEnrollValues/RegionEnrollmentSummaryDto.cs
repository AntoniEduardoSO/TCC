using System.Text.Json.Serialization;
using Arkhos.Core.Interfaces;

namespace Arkhos.Core.Models.Dto.SchoolEnrollValues;
public class RegionEnrollmentSummaryDto : ILocationContext
{
    [JsonPropertyName("ano")]
    public int Ano { get; set; }

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}

    // TOTAIS GERAIS
    
    [JsonPropertyName("total_escolas")]
    public int TotalEscolas { get; set; }

    [JsonPropertyName("total_escolas_urbanas")]
    public int TotalEscolasUrbanas { get; set; }

    [JsonPropertyName("total_escolas_rurais")]
    public int TotalEscolasRurais { get; set; }

    // DETALHAMENTO MUNICIPAL (Rede 3)
    [JsonPropertyName("escolas_municipais_total")]
    public int EscolasMunicipaisTotal { get; set; }

    [JsonPropertyName("escolas_municipais_urbanas")]
    public int EscolasMunicipaisUrbanas { get; set; }

    [JsonPropertyName("escolas_municipais_rurais")]
    public int EscolasMunicipaisRurais { get; set; }

    [JsonPropertyName("escolas_municipais_com_creche")]
    public int EscolasMunicipaisComCreche { get; set; }

    [JsonPropertyName("escolas_municipais_com_fundamental")]
    public int EscolasMunicipaisComFundamental { get; set; }

    [JsonPropertyName("escolas_municipais_com_medio")]
    public int EscolasMunicipaisComMedio { get; set; }

    // DETALHAMENTO ESTADUAL (Rede 2)
    [JsonPropertyName("escolas_estaduais_total")]
    public int EscolasEstaduaisTotal { get; set; }

    [JsonPropertyName("escolas_estaduais_urbanas")]
    public int EscolasEstaduaisUrbanas { get; set; }

    [JsonPropertyName("escolas_estaduais_rurais")]
    public int EscolasEstaduaisRurais { get; set; }

    [JsonPropertyName("escolas_estaduais_com_creche")]
    public int EscolasEstaduaisComCreche { get; set; }

    [JsonPropertyName("escolas_estaduais_com_fundamental")]
    public int EscolasEstaduaisComFundamental { get; set; }

    [JsonPropertyName("escolas_estaduais_com_medio")]
    public int EscolasEstaduaisComMedio { get; set; }

    // Matriculas.
    [JsonPropertyName("matricula_creche")]
    public double MatriculaCreche { get; set; }
    [JsonPropertyName("matricula_pre_escola")]
    public double MatriculaPreEscola { get; set; }

    [JsonPropertyName("matricula_ensino_fundamental_ai")]
    public double MatriculaEnsinoFundamentalAI { get; set; }
    
    [JsonPropertyName("matricula_ensino_fundamental_af")]
    public double MatriculaEnsinoFundamentalAF { get; set; }
    
    [JsonPropertyName("matricula_ensino_medio")]
    public double MatriculaEnsinoMedio { get; set; }

    public double MatriculaTotal { get; set; }

    // Corpo Docente.
    [JsonPropertyName("professor_total")]
    public double ProfessorTotal { get; set; }

    [JsonPropertyName("professor_creche")]
    public double ProfessorCreche { get; set; }
    [JsonPropertyName("professor_pre_escola")]
    public double ProfessorPreEscola { get; set; }
    [JsonPropertyName("professor_ef_iniciais")]
    public double ProfessorEFIniciais { get; set; }
    [JsonPropertyName("professor_ef_finais")]
    public double ProfessorEFFinais { get; set; }
    [JsonPropertyName("professor_medio")]
    public double ProfessorMedio { get; set; }
    [JsonPropertyName("professor_especial")]
    public double ProfessorEspecial { get; set; }
    [JsonPropertyName("psicologo")]

    // Apoio pedagogico.
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

