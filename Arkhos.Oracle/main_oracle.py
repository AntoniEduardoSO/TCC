import pandas as pd
import os
from sqlalchemy import create_engine, text

from controllers.state_controller import StateController
from controllers.school_controller import SchoolController
from controllers.municipality_controller import MunicipalityController
from controllers.microregion_controller import MicroregionController
from controllers.mesoregion_controller import MesoregionController
from models.predictor import RiskPredictor

from core.plot_generator import ArkhosPlotter

DB_URL = "postgresql://postgres:311200@localhost:5432/arkhos"

def init_insights_db():
    engine = create_engine(DB_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS insights (
                    id SERIAL PRIMARY KEY,
                    axis TEXT,
                    level TEXT,
                    ano INTEGER,
                    tipo_insight TEXT,
                    titulo TEXT,
                    valor_destaque REAL,
                    descricao TEXT,
                    recomendacao TEXT,
                    valor_baseline REAL,
                    id_alvo INTEGER
                );
            """))

            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_insights_filter ON insights(level, id_alvo, ano);"))
            print(f" [INIT] Tabela 'insights' verificada/criada com sucesso no PostgreSQL.")
    except Exception as e:
        print(f" [ERRO INIT] Falha ao inicializar tabela de insights: {e}")

def salvar_insights(df_results: pd.DataFrame):
    if df_results.empty: return

    if 'id_alvo' in df_results.columns:
        df_results['id_alvo'] = pd.to_numeric(df_results['id_alvo'], errors='coerce').fillna(0).astype(int)

    colunas_banco = [
        'axis', 'level', 'ano', 'tipo_insight', 'titulo', 
        'valor_destaque', 'descricao', 'recomendacao', 'valor_baseline', 'id_alvo'
    ]
    
    for col in colunas_banco:
        if col not in df_results.columns:
            df_results[col] = None
            
    df_banco = df_results[colunas_banco]

    engine = create_engine(DB_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM insights"))
            
        # O Pandas faz o insert em massa super rápido no Postgres usando SQLAlchemy
        df_banco.to_sql('insights', engine, if_exists='append', index=False)
        print(f" [DB] Insights salvos com sucesso na tabela 'insights' do PostgreSQL!")
    except Exception as e:
        print(f" [ERRO DB] Falha ao salvar os insights no banco: {e}")

def carregar_dados_prescritivos(ano: int) -> pd.DataFrame:
    engine = create_engine(DB_URL)
    
    query = """
        SELECT 
            r.id_escola_fk, r.ano, r.acessibility_rating, r.teacher_instability_rating, 
            r.administrative_burden_rating, r.spending_per_student, r.dropout_rate,
            s.id_municipio_fk as id_municipio, c.id_microrregiao, c.id_mesorregiao,
            
            -- [INFRA] Acessibilidade
            MAX(CASE WHEN i.id_atributo = 32 THEN i.valor ELSE 0 END) AS IN_BANHEIRO_PNE,
            MAX(CASE WHEN i.id_atributo = 60 THEN i.valor ELSE 0 END) AS IN_SALA_ATENDIMENTO_ESPECIAL,
            MAX(CASE WHEN i.id_atributo = 63 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_CORRIMAO,
            MAX(CASE WHEN i.id_atributo = 65 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_PISOS_TATEIS,
            MAX(CASE WHEN i.id_atributo = 66 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_VAO_LIVRE,
            MAX(CASE WHEN i.id_atributo = 67 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_RAMPAS,
            MAX(CASE WHEN i.id_atributo = 68 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_SINAL_SONORO,
            MAX(CASE WHEN i.id_atributo = 69 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_SINAL_TATIL,

            -- [INFRA] Recreação, Bem-Estar e Pedagógico
            MAX(CASE WHEN i.id_atributo = 47 THEN i.valor ELSE 0 END) AS IN_QUADRA_ESPORTES,
            MAX(CASE WHEN i.id_atributo = 43 THEN i.valor ELSE 0 END) AS IN_PATIO_COBERTO,
            MAX(CASE WHEN i.id_atributo = 45 THEN i.valor ELSE 0 END) AS IN_PARQUE_INFANTIL,
            MAX(CASE WHEN i.id_atributo = 75 THEN i.valor ELSE 0 END) AS QT_SALAS_UTILIZA_CLIMATIZADAS,
            MAX(CASE WHEN i.id_atributo = 35 THEN i.valor ELSE 0 END) AS IN_BIBLIOTECA,
            MAX(CASE WHEN i.id_atributo = 1 THEN i.valor ELSE 0 END) AS IN_AGUA_POTAVEL,
            MAX(CASE WHEN i.id_atributo = 48 THEN i.valor ELSE 0 END) AS IN_REFEITORIO,
            MAX(CASE WHEN i.id_atributo = 12 THEN i.valor ELSE 0 END) AS IN_ESGOTO_REDE_PUBLIC,
            MAX(CASE WHEN i.id_atributo = 41 THEN i.valor ELSE 0 END) AS IN_LABORATORIO_INFORMATICA,
            MAX(CASE WHEN i.id_atributo = 93 THEN i.valor ELSE 0 END) AS IN_INTERNET_ALUNOS,
            
            -- [ENROLL] Docentes
            MAX(CASE WHEN e.id_atributo = 82 THEN e.valor ELSE 0 END) AS QT_DOC_INF,
            MAX(CASE WHEN e.id_atributo = 86 THEN e.valor ELSE 0 END) AS QT_DOC_FUND_AI,
            MAX(CASE WHEN e.id_atributo = 87 THEN e.valor ELSE 0 END) AS QT_DOC_FUND_AF,
            MAX(CASE WHEN e.id_atributo = 88 THEN e.valor ELSE 0 END) AS QT_DOC_MED,
            
            -- [ENROLL] Suporte Humano e Gestão
            MAX(CASE WHEN e.id_atributo = 8  THEN e.valor ELSE 0 END) AS QT_PROF_PSICOLOGO,
            MAX(CASE WHEN e.id_atributo = 15 THEN e.valor ELSE 0 END) AS QT_PROF_ASSIST_SOCIAL,
            MAX(CASE WHEN e.id_atributo = 19 THEN e.valor ELSE 0 END) AS IN_ORGAO_ASS_PAIS_MESTRES,
            MAX(CASE WHEN e.id_atributo = 20 THEN e.valor ELSE 0 END) AS IN_ORGAO_CONSELHO_ESCOLAR,
            MAX(CASE WHEN e.id_atributo = 21 THEN e.valor ELSE 0 END) AS IN_ORGAO_GREMIO_ESTUDANTIL

        FROM school_rating r
        JOIN school_info s ON r.id_escola_fk = s.escola_id AND s.ano = r.ano
        LEFT JOIN city_info c ON s.id_municipio_fk = c.municipio_id AND c.ano = r.ano
        
        LEFT JOIN school_infra_values i ON r.id_escola_fk = i.id_escola_fk AND i.ano = r.ano 
             AND i.id_atributo IN (1, 12, 32, 35, 41, 43, 45, 47, 48, 60, 63, 65, 66, 67, 68, 69, 75, 93)
             
        LEFT JOIN school_enroll_values e ON r.id_escola_fk = e.id_escola_fk AND e.ano = r.ano 
             AND e.id_atributo IN (8, 15, 19, 20, 21, 82, 86, 87, 88)
        
        WHERE r.ano = %(ano)s 
        
        -- Postgres EXIGE que todas as colunas do SELECT sem MAX() entrem no GROUP BY
        GROUP BY 
            r.id_escola_fk, r.ano, r.acessibility_rating, r.teacher_instability_rating, 
            r.administrative_burden_rating, r.spending_per_student, r.dropout_rate,
            s.id_municipio_fk, c.id_microrregiao, c.id_mesorregiao;
    """
    
    df = pd.read_sql_query(query, engine, params={'ano': ano})
    return df

def carregar_historico_ml() -> pd.DataFrame:
    engine = create_engine(DB_URL)
    query = """
        SELECT 
            r.*,
            s.id_municipio_fk as id_municipio,
            c.id_microrregiao,
            c.id_mesorregiao
        FROM school_rating r
        JOIN school_info s ON r.id_escola_fk = s.escola_id AND s.ano = r.ano
        LEFT JOIN city_info c ON s.id_municipio_fk = c.municipio_id AND c.ano = r.ano;
    """
    df = pd.read_sql_query(query, engine)
    if 'id_escola_fk' in df.columns:
        df = df.rename(columns={'id_escola_fk': 'id_escola'})
    return df

def run_prescriptive_engine(anos_analise: list):
    prescricoes_regras = []

    for ano in anos_analise:
        try:
            df_ano = carregar_dados_prescritivos(ano)
            if df_ano.empty:
                print(f"[Prescritivo] Aviso: Sem dados para {ano}.")
                continue
        except Exception as e:
            print(f"[Prescritivo] Erro no ano {ano}: {e}")
            continue

        baselines = {
            'acessibilidade': df_ano['acessibility_rating'].median(),
            'instabilidade': df_ano['teacher_instability_rating'].median(),
            'admin': df_ano['administrative_burden_rating'].median(),
            'custo_aluno_ano': df_ano.get('spending_per_student', pd.Series(dtype=float)).median(),
            'acessibilidade_mean': df_ano['acessibility_rating'].mean(),
            'acessibilidade_std': df_ano['acessibility_rating'].std(),
            'instabilidade_mean': df_ano['teacher_instability_rating'].mean(),
            'instabilidade_std': df_ano['teacher_instability_rating'].std(),
            'evasao_mean': df_ano.get('dropout_rate', pd.Series(dtype=float)).mean(), 
            'evasao_std': df_ano.get('dropout_rate', pd.Series(dtype=float)).std(),
            'custo_aluno_std': df_ano.get('spending_per_student', pd.Series(dtype=float)).std()
        }

        ctrls = [
            StateController(baselines),
            SchoolController(baselines),
            MunicipalityController(baselines),
            MicroregionController(baselines),
            MesoregionController(baselines)
        ]

        resultados_ano = []
        for ctrl in ctrls:
            if isinstance(ctrl, StateController): resultados_ano.extend(ctrl.process_state(df_ano))
            elif isinstance(ctrl, SchoolController): resultados_ano.extend(ctrl.process_all_schools(df_ano))
            elif isinstance(ctrl, MunicipalityController): resultados_ano.extend(ctrl.process_all_municipalities(df_ano))
            elif isinstance(ctrl, MicroregionController): resultados_ano.extend(ctrl.process_all_microregions(df_ano))
            elif isinstance(ctrl, MesoregionController): resultados_ano.extend(ctrl.process_all_mesoregions(df_ano))
            
        for req in resultados_ano:
            req['tipo_insight'] = 'Prescritivo'
            req['ano'] = ano
            
        prescricoes_regras.extend(resultados_ano)
        print(f"[Prescritivo] {ano}: {len(resultados_ano)} prescrições geradas.")
        
    return prescricoes_regras

def run_predictive_engine(df_historico: pd.DataFrame, ano_atual: int, df_detalhado: pd.DataFrame) -> list:
    if df_historico.empty: return []
        
    df_atual = df_historico[df_historico['ano'] == ano_atual]

    baselines_ml = {
        'evasao_mean': df_atual['dropout_rate'].mean() if not df_atual.empty else 0.05,
        'evasao_std': df_atual['dropout_rate'].std() if not df_atual.empty else 0.02,
        
        'acessibilidade_mean': df_atual['acessibility_rating'].mean() if not df_atual.empty else 0.5,
        'acessibilidade_std': df_atual['acessibility_rating'].std() if not df_atual.empty else 0.1,
        
        'instabilidade_mean': df_atual['teacher_instability_rating'].mean() if not df_atual.empty else 0.6,
        'instabilidade_std': df_atual['teacher_instability_rating'].std() if not df_atual.empty else 0.15
    }
    
    preditor = RiskPredictor(baselines=baselines_ml)
    prescricoes_ml = preditor.generate_shap_prescriptions(df_historico, df_detalhado, ano_atual=ano_atual)
    
    return prescricoes_ml

def run_oracle():
    diretorio_oracle = os.path.dirname(os.path.abspath(__file__))
    PASTA_DATA = os.path.join(diretorio_oracle, "data")
    CAMINHO_CSV = os.path.join(PASTA_DATA, "dummy_data.csv")

    # Inicia a tabela diretamente no Postgres
    init_insights_db()

    if os.path.exists(CAMINHO_CSV):
        print(f"\n[ORACLE - CACHE] Arquivo '{CAMINHO_CSV}' encontrado!")
        print("[ORACLE - CACHE] Pulando geração pesada. Enviando dados direto para o banco PostgreSQL...")
        
        df_csv = pd.read_csv(CAMINHO_CSV, encoding='utf-8-sig')
        salvar_insights(df_csv)
        
        print("\n[ORACLE - OK] Processo finalizado usando dados cacheados.")
    
    else:
        ANOS_PRESCRITIVOS = list(range(2017, 2026)) 
        ANOS_PREDITIVOS = [2023, 2024, 2025]           
        
        print("="*50)
        print(" INICIANDO MOTOR ARKHOS (PRESCRITIVO + PREDITIVO)")
        print("="*50)
        
        lista_prescritiva = run_prescriptive_engine(ANOS_PRESCRITIVOS)

        lista_preditiva = []
        df_historico_ml = carregar_historico_ml()
        preditor = RiskPredictor(baselines={})

        if not df_historico_ml.empty:
            print("\n" + "="*50)
            print(" [TCC] INICIANDO VALIDAÇÃO DO MODELO (BACKTESTING 2024)")
            print("="*50)
            
            # Roda a função de Backtesting que criamos na classe RiskPredictor
            df_validacao = preditor.executar_backtesting(df_historico_ml, ano_treino_max=2023, ano_teste=2024)
            
            print("\n[Resumo da Validação Estatística do Motor Arkhos]")
            print(df_validacao.to_markdown(index=False)) # Exibe a tabela bonitinha no terminal
            
            # Gera e salva o gráfico da taxa de acerto
            plotter = ArkhosPlotter(output_dir="docs_tcc_graficos")
            plotter.plot_model_validation(df_validacao)
            
            print("="*50 + "\n")
        
        for ano_pred in ANOS_PREDITIVOS:
            print(f"\n[Preditivo] Iniciando projeções a partir do ano-base {ano_pred}...")
            df_ano_detalhado = carregar_dados_prescritivos(ano_pred)
            alertas_ano = run_predictive_engine(df_historico_ml, ano_pred, df_ano_detalhado)
            lista_preditiva.extend(alertas_ano)
        
        todas_prescricoes = lista_prescritiva + lista_preditiva
        df_results = pd.DataFrame(todas_prescricoes)
    
        if not df_results.empty:
            if 'id_alvo' in df_results.columns:
                df_results['id_alvo'] = pd.to_numeric(df_results['id_alvo'], errors='coerce').fillna(0).astype(int)
            
            df_results.to_csv(CAMINHO_CSV, index=False, encoding='utf-8-sig')
            salvar_insights(df_results)

            print(f"\n=======================================================")
            print(f" SUCESSO! Total de {len(df_results)} registros exportados.")
            print(f" Motor cobriu regras de {min(ANOS_PRESCRITIVOS)} a {max(ANOS_PRESCRITIVOS)} e projeções de {ANOS_PREDITIVOS}.")
            print(f" Arquivo consolidado em: {CAMINHO_CSV}")
            print(f"=======================================================")
        else:
            print("\nNenhuma prescrição foi gerada pelos motores.")


if __name__ == "__main__":
    run_oracle()