namespace Arkhos.Core.Models;

public class SchoolInfo
{
    public long Id {get;set;}

    public string NomeEscola {get;set;} = string.Empty;

    public short Depedencia {get;set;}

    public short Localizacao {get;set;}

    public short Funcionamento {get;set;}

    public short Sede {get;set;}

    public short Alocacao {get;set;}

    public short Ocupacao {get;set;}

    public int Ano {get;set;}

    public string Endereco {get;set;} = string.Empty;

    public string Telefone {get;set;} = string.Empty;
  
}