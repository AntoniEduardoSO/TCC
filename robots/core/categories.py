def categorize_cost(description):
    """
    Categoriza a despesa para o Dashboard Preditivo.
    """
    if not isinstance(description, str):
        return "Outros"
    
    desc_upper = description.upper()
    
    # 1. Nutrição (Foco: Estoque e Merenda)
    termos_nutricao = [
        'MERENDA', 'ALIMENT', 'GENERO ALIMENTICIO', 'HORTIFRUTI', 'CARNE', 'LEITE', 
        'PÃO', 'PAO', 'GÁS', 'GAS DE COZINHA', 'GLP', 'AGUA MINERAL', 'ÁGUA MINERAL', 
        'NUTRICIONISTA', 'KITS DE ALIMENTAÇÃO', 'KIT MERENDA', 'REFEIÇÕES'
    ]
    if any(termo in desc_upper for termo in termos_nutricao):
        return "Nutrição e Segurança Alimentar"

    # 2. Infraestrutura (Foco: Manutenção e Obras)
    termos_infra = [
        'ENERGIA', 'ELETRIC', 'EQUATORIAL', 'AGUA E ESGOTO', 'CASAL', 'ALUGUEL', 
        'LOCAÇÃO DE IMÓVEL', 'OBRA', 'REFORMA', 'ENGENHARIA', 'MANUTENÇÃO PREDIAL', 
        'AR CONDICIONADO', 'DEDETIZAÇÃO', 'LIMPEZA DE FOSSA', 'PINTURA', 'HIDRAULI',
        'MATERIAL DE CONSTRUÇÃO', 'REPAROS'
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
        'KIT ESCOLAR', 'BRINQUEDOTECA', 'MATERIAL DE EXPEDIENTE'
    ]
    if any(termo in desc_upper for termo in termos_pedagogico):
        return "Pedagógico e Capital Humano"

    # 5. Custo Administrativo (Eventos e Burocracia)
    termos_admin = [
        'EVENTO', 'FESTIVIDADE', 'DECORAÇÃO', 'COFFEE', 'BUFFET', 'PALCO', 'SOM', 
        'PUBLICIDADE', 'ASSESSORIA', 'CONSULTORIA', 'SISTEMA', 'SOFTWARE', 'INTERNET', 
        'TARIFA', 'BANCARIA', 'INDENIZAÇÃO', 'RESTITUIÇÃO', 'CARTORIO', 'DIARIA', 
        'PASSAGEM', 'HOSPEDAGEM', 'CAMISAS', 'TROFEUS', 'PREMIAÇÃO', 'BANNER'
    ]
    if any(termo in desc_upper for termo in termos_admin):
        return "Custo Administrativo e Ruído"

    # 6. Materiais e Equipamentos
    termos_materiais = [
        'MATERIAL DE LIMPEZA', 'HIGIENE', 'MOBILIARIO', 'CADEIRA', 'MESA', 'ARMARIO',
        'ESTANTE', 'COMPUTADOR', 'NOTEBOOK', 'IMPRESSORA', 'ELETRODOMESTICO', 
        'VENTILADOR', 'FREEZER', 'GELADEIRA', 'FOGÃO'
    ]
    if any(termo in desc_upper for termo in termos_materiais):
        return "Materiais e Equipamentos"

    return "Outros"