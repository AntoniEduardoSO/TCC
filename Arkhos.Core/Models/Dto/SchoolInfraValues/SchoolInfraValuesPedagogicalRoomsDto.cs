using System.Text.Json.Serialization;
using Arkhos.Core.Interfaces;

namespace Arkhos.Core.Models.Dto.SchoolInfraValues;
public class SchoolInfraValuesPedagogicalRoomsDto : ILocationContext
{
    [JsonPropertyName("escola_id")]
    public int EscolaId {get;set;}
    
    [JsonPropertyName("auditorio")]
    public double Auditorio {get;set;}
    [JsonPropertyName("biblioteca")]
    public double Biblioteca {get;set;}
    [JsonPropertyName("laboratorio_ciencias")]
    public double LaboratorioCiencias {get;set;}
    [JsonPropertyName("laboratorio_informatica")]
    public double LaboratorioInformatica {get;set;}
    [JsonPropertyName("sala_atelie_artes")]
    public double SalaAtelieArtes {get;set;}
    [JsonPropertyName("sala_musica_coral")]
    public double SalaMusicaCoral {get;set;}
    [JsonPropertyName("sala_estudio_danca")]
    public double SalaEstudioDanca {get;set;}
    [JsonPropertyName("sala_multiuso")]
    public double SalaMultiuso {get;set;}
    [JsonPropertyName("sala_estudio_gravacao")]
    public double SalaEstudioGravacao {get;set;}
    [JsonPropertyName("sala_leitura")]
    public double SalaLeitura {get;set;}

    [JsonPropertyName("ano")]
    public int Ano { get; set; }

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}
}