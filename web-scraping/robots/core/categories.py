def categorize_cost(description, elemento):

    if not isinstance(description, str): description = ""
    if not isinstance(elemento, str): elemento = ""
    
    desc_upper = description.upper()
    elem_upper = elemento.upper()

    if 'ALIMENT' in elem_upper or 'MERENDA' in elem_upper:
        return "Nutrição e Segurança Alimentar"
        
    if 'OBRA' in elem_upper or 'INSTALAÇÕES' in elem_upper or 'INSTALACOES' in elem_upper:
        return "Infraestrutura e Ambiente Escolar"
        
    if 'EQUIPAMENTO' in elem_upper or 'MATERIAL PERMANENTE' in elem_upper:
        return "Materiais e Equipamentos"
        
    if any(termo in elem_upper for termo in ['PASSAGENS', 'DIÁRIAS', 'DIARIAS', 'INDENIZAÇÕES']):
        return "Custo Administrativo e Ruído"
        
    if any(termo in elem_upper for termo in ['VENCIMENTOS', 'OBRIGAÇÕES PATRONAIS', 'OBRIGACOES PATRONAIS', 'TEMPO DETERMINADO', 'APOSENTADORIAS', 'PENSÕES', 'PENSOES']):
        return "Pedagógico e Capital Humano"
    
    # 1. Nutrição (Foco: Estoque e Merenda)
    termos_nutricao = [
        'MERENDA', 'ALIMENT', 'GENERO ALIMENTICIO', 'HORTIFRUTI', 'CARNE', 'LEITE', 
        'PÃO', 'PAO', 'GÁS', 'GAS DE COZINHA', 'GLP', 'AGUA MINERAL', 'ÁGUA MINERAL', 
        'NUTRICIONISTA', 'KITS DE ALIMENTAÇÃO', 'KIT MERENDA', 'REFEIÇÕES', 'FORNECIMENTO DE ÁGUA', 'FORNECIMENTO DE AGUA'
    ]
    if any(termo in desc_upper for termo in termos_nutricao):
        return "Nutrição e Segurança Alimentar"

    # 2. Infraestrutura (Foco: Manutenção e Obras)
    termos_infra = [
        'ENERGIA', 'ELETRIC', 'EQUATORIAL', 'AGUA E ESGOTO', 'CASAL', 'ALUGUEL', 
        'LOCAÇÃO DE IMÓVEL', 'OBRA', 'REFORMA', 'ENGENHARIA', 'MANUTENÇÃO PREDIAL', 
        'AR CONDICIONADO', 'DEDETIZAÇÃO', 'LIMPEZA DE FOSSA', 'PINTURA', 'HIDRAULI',
        'MATERIAL DE CONSTRUÇÃO', 'REPAROS', 'ENÉRGIA', "CONSTRUÇÃO", "CONSTRUCAO", "CONSTRUCÕES",
        "CONSTRUÇÕES", "CONSTRUCOES", "COSNTRUÇÃO", 'OBRA', 'REFORMA', 'CONSTRUÇÃO', 'AMPLIAÇÃO', 
        'MANUTENÇÃO', 'ENGENHARIA', 'MATERIAL DE CONSTRUÇÃO', 'TINTA', 'CIMENTO', 'TIJOLO', 'HIDRAULICO',
        'IMÓVEL', 'IMOVEL'
    ]
    if any(termo in desc_upper for termo in termos_infra):
        return "Infraestrutura e Ambiente Escolar"

    # 3. Logística (Foco: Transporte Escolar)
    termos_logistica = [
        'TRANSPORTE', 'ONIBUS', 'ÔNIBUS', 'VEICULO', 'VEÍCULO', 'LOCAÇÃO DE VEÍCULO', 
        'COMBUSTIVEL', 'COMBUSTÍVEL', 'DIESEL', 'GASOLINA', 'LUBRIFICANTE', 'PEÇA', 
        'PNEU', 'MANUTENÇÃO DE VEÍCULO', 'MOTORISTA', 'CNH', 'VISTORIA', 'EMPLACAMENTO',
        'BORRACHARIA', 'MECANICA'
    ]
    if any(termo in desc_upper for termo in termos_logistica):
        return "Logística e Acesso"

    # 4. Pedagógico (Foco: RH e Ensino)
    termos_pedagogico = [
        'FOLHA', 'PESSOAL', 'SALARIO', 'VENCIMENTO', 'PROFESSOR', 'MAGISTERIO', 
        'FUNDEB', '13º', 'FERIAS', 'FÉRIAS', 'INSS', 'FGTS', 'CAPACITAÇÃO', 'CURSO', 
        'TREINAMENTO', 'DIDATICO', 'DIDÁTICO', 'PEDAGOGICO', 'PEDAGÓGICO', 'LIVRO', 
        'KIT ESCOLAR', 'BRINQUEDOTECA', 'MATERIAL DE EXPEDIENTE', "SUBSTITUIÇÃO DA PROF",
        "SUBSTITUICAO DA PROF", "SUBSTITUIÇÃO DO PROF", "SUBSTITUICAO DO PROF", "SUBSTITUIÇÃO DA PROF.",
        "BOLSA", "ESTUDO"
    ]
    if any(termo in desc_upper for termo in termos_pedagogico):
        return "Pedagógico e Capital Humano"

    # 5. Custo Administrativo (Eventos e Burocracia)
    termos_admin = [
        'EVENTO', 'FESTIVIDADE', 'DECORAÇÃO', 'COFFEE', 'BUFFET', 'PALCO', 'SOM', 
        'PUBLICIDADE', 'ASSESSORIA', 'CONSULTORIA', 'SISTEMA', 'SOFTWARE', 'INTERNET', 
        'TARIFA', 'BANCARIA', 'BANCÁRIA', 'BANCÁRIO', 'BANCARIO', 'INDENIZAÇÃO', 'RESTITUIÇÃO', 
        'CARTORIO', 'DIARIA', 'PASSAGEM', 'HOSPEDAGEM', 'CAMISAS', 'TROFEUS', 'PREMIAÇÃO', 'BANNER',
        'PRECATÓRIO', 'PRECATORIO', 'DÍVIDA', 'DIVIDA', 'AMORTIZAÇÃO', 'AMORTIZACAO'
    ]
    if any(termo in desc_upper for termo in termos_admin):
        return "Custo Administrativo e Ruído"

    # 6. Materiais e Equipamentos
    termos_materiais = [
        'MATERIAL DE LIMPEZA', 'HIGIENE', 'MOBILIARIO', 'CADEIRA', 'MESA', 'ARMARIO',
        'ESTANTE', 'COMPUTADOR', 'NOTEBOOK', 'IMPRESSORA', 'ELETRODOMESTICO', 
        'VENTILADOR', 'FREEZER', 'GELADEIRA', 'FOGÃO', 'CARTUCHO', "MANUTENÇÃO EM EQUIPAMENTOS",
        "MANUTENCAO EM EQUIPAMENTOS"
    ]
    if any(termo in desc_upper for termo in termos_materiais):
        return "Materiais e Equipamentos"

    return "Outros"