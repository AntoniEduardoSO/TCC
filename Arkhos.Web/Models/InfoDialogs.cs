namespace Arkhos.Web.Models;
public static class InfoDialogs
{
    public static readonly Dictionary<string, InfoDialogData> Data = new()
    {
        ["gastoAluno"] = new InfoDialogData
        {
            Title = "Gasto por Aluno - Como é Calculado?",
            FormulaLatex = @"
            \begin{aligned}
            \text{Gasto por aluno} =
            \frac{
            G_{\text{docente}} +
            G_{\text{infraestrutura}} +
            G_{\text{transporte}} +
            G_{\text{alimentação}} +
            G_{\text{pedagógico}} +
            G_{\text{administração}}
            }{
            \sum A
            }
            \end{aligned}",

            Definitions =
            [
                new() { SymbolLatex = "(G_{\\text{docente}}\\)", Description = "Gasto com docentes (Encargos, remuneração e contratos temporários)" },
                new() { SymbolLatex = "(G_{\\text{infraestrutura}}\\)", Description = "Infraestrutura escolar" },
                new() { SymbolLatex = "(G_{\\text{transporte}}\\)", Description = "Transporte escolar" },
                new() { SymbolLatex = "(G_{\\text{alimentação}}\\)", Description = "Alimentação escolar" },
                new() { SymbolLatex = "(G_{\\text{pedagógico}}\\)", Description = "Apoio pedagógico" },
                new() { SymbolLatex = "(G_{\\text{administração}}\\)", Description = "Gestão administrativa" },
                new() { SymbolLatex = @"(\sum A\)", Description = "Total de alunos" }
            ],

            Explanation = "O valor é distribuído proporcionalmente ao número de alunos pelo Municipio/Microregião/Mesorregião"
        },

        ["gastoProfessor"] = new InfoDialogData
        {
            Title = "Gasto por Professor - Como é Calculado?",
            FormulaLatex = @"
            \begin{aligned}
            \text{Gasto por professor} =
            \frac{
            G_{\text{remuneração}} +
            G_{\text{encargos}} +
            G_{\text{benefícios}} +
            G_{\text{contratos_temporários}} +
            G_{\text{outros}} 
            }{
            \sum P
            }
            \end{aligned}",

            Definitions =
            [
                new() { SymbolLatex = "(G_{\\text{remuneração}}\\)", Description = "Remuneração dos professores" },
                new() { SymbolLatex = "(G_{\\text{encargos}}\\)", Description = "Encargos trabalhistas" },
                new() { SymbolLatex = "(G_{\\text{benefícios}}\\)", Description = "Benefícios aos professores" },
                new() { SymbolLatex = "(G_{\\text{contratos_temporários}}\\)", Description = "Contratos temporários" },
                new() { SymbolLatex = "(G_{\\text{outros}}\\)", Description = "Outros gastos" }
            ],

            Explanation = "O valor é distribuído proporcionalmente ao número de professores pelo Municipio/Microregião/Mesorregião"
        },
        ["gastoInfraestrutura"] = new InfoDialogData
        {
            Title = "Gasto Infraestrutura - Como é Calculado?",
            FormulaLatex = @"
            \begin{aligned}
            \text{Gasto Infraestrutura} =
            {
            G_{\text{obras}} +
            G_{\text{aluguel}} +
            G_{\text{equipamentos}} +
            G_{\text{contratos}} +
            G_{\text{manutenção}} +
            G_{\text{remuneração}} +
            G_{\text{outros}} 
            }
            \end{aligned}",

            Definitions =
            [
                new() { SymbolLatex = "(G_{\\text{obras}}\\)", Description = "Obras em andamento e finalizadas" },
                new() { SymbolLatex = "(G_{\\text{aluguel}}\\)", Description = "Aluguel de imóveis" },
                new() { SymbolLatex = "(G_{\\text{equipamentos}}\\)", Description = "Equipamentos para construção cívil" },
                new() { SymbolLatex = "(G_{\\text{contratos}}\\)", Description = "Contratos de empreiteiras" },
                new() { SymbolLatex = "(G_{\\text{manutenção}}\\)", Description = "Manutenção de propriedades" },
                new() { SymbolLatex = "(G_{\\text{remuneração}}\\)", Description = "Remuneração de funcionários" },
                new() { SymbolLatex = "(G_{\\text{outros}}\\)", Description = "Outros gastos" }
            ],

            Explanation = "O valor é distribuído proporcionalmente pelo Municipio/Microregião/Mesorregião"
        }
    };
}