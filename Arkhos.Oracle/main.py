import pandas as pd
import sqlite3
import os
from controllers.school_controller import SchoolController
from controllers.municipality_controller import MunicipalityController
from controllers.microregion_controller import MicroregionController
from controllers.mesoregion_controller import MesoregionController

def carregar_dados_banco(db_path: str, ano: int) -> pd.DataFrame:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Banco de dados não encontrado em: {db_path}")

    conn = sqlite3.connect(db_path)
    
    query = """
        SELECT 
            r.id_escola_fk,
            r.ano,
            r.acessibility_rating,
            r.teacher_instability_rating,
            r.administrative_burden_rating,
            s.id_municipio_fk as id_municipio,
            c.id_microrregiao,
            c.id_mesorregiao,
            
            -- Pivot de Infraestrutura (Tabela school_infra_values)
            MAX(CASE WHEN i.id_atributo = 32 THEN i.valor ELSE 0 END) AS IN_BANHEIRO_PNE,
            MAX(CASE WHEN i.id_atributo = 60 THEN i.valor ELSE 0 END) AS IN_SALA_ATENDIMENTO_ESPECIAL,
            MAX(CASE WHEN i.id_atributo = 63 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_CORRIMAO,
            MAX(CASE WHEN i.id_atributo = 65 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_PISOS_TATEIS,
            MAX(CASE WHEN i.id_atributo = 66 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_VAO_LIVRE,
            MAX(CASE WHEN i.id_atributo = 67 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_RAMPAS,
            MAX(CASE WHEN i.id_atributo = 68 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_SINAL_SONORO,
            MAX(CASE WHEN i.id_atributo = 69 THEN i.valor ELSE 0 END) AS IN_ACESSIBILIDADE_SINAL_TATIL,
            
            -- Pivot de Docentes/Matrículas (Tabela school_enroll_values)
            MAX(CASE WHEN e.id_atributo = 82 THEN e.valor ELSE 0 END) AS QT_DOC_INF,
            MAX(CASE WHEN e.id_atributo = 86 THEN e.valor ELSE 0 END) AS QT_DOC_FUND_AI,
            MAX(CASE WHEN e.id_atributo = 87 THEN e.valor ELSE 0 END) AS QT_DOC_FUND_AF,
            MAX(CASE WHEN e.id_atributo = 88 THEN e.valor ELSE 0 END) AS QT_DOC_MED

        FROM school_rating r
        JOIN school_info s 
            ON r.id_escola_fk = s.escola_id 
            AND s.ano = r.ano
        LEFT JOIN city_info c 
            ON s.id_municipio_fk = c.municipio_id 
            AND c.ano = r.ano
        -- Join focado em Infraestrutura
        LEFT JOIN school_infra_values i 
            ON r.id_escola_fk = i.id_escola_fk 
            AND i.ano = r.ano
            AND i.id_atributo IN (32, 60, 63, 65, 66, 67, 68, 69)
        -- Join focado em Matrículas/Docentes (Nova Tabela)
        LEFT JOIN school_enroll_values e 
            ON r.id_escola_fk = e.id_escola_fk 
            AND e.ano = r.ano
            AND e.id_atributo IN (82, 86, 87, 88)
        
        WHERE r.ano = ?
        
        GROUP BY 
            r.id_escola_fk, 
            r.ano, 
            r.acessibility_rating,
            r.teacher_instability_rating,
            r.administrative_burden_rating,
            s.id_municipio_fk,
            c.id_microrregiao,
            c.id_mesorregiao;
    """
    
    print(f"Extraindo dados consolidados do arkhos.db para o ano {ano}...")
    df = pd.read_sql_query(query, conn, params=(ano,))
    conn.close()
    
    print("\nCOLUNAS CARREGADAS NO PANDAS:")
    print(df.columns.tolist())
    print("="*40)
    
    return df

def run_prescriptive_engine(db_path: str, anos_analise: list):
    final_prescriptions = []
    
    for ano in anos_analise:
        try:
            df_ano = carregar_dados_banco(db_path, ano)
            
            if df_ano.empty:
                print(f"Aviso: Nenhum dado encontrado para o ano {ano}.")
                continue
                
            print(f"[{ano}] Dados carregados! Total de escolas: {len(df_ano)}")
        except Exception as e:
            print(f"Erro ao carregar dados do ano {ano}: {e}")
            continue
    
    baselines = {
            'acessibilidade': df_ano['acessibility_rating'].median(),
            'instabilidade': df_ano['teacher_instability_rating'].median(),
            'admin': df_ano['administrative_burden_rating'].median()
    }

    school_ctrl = SchoolController(baselines)
    municipality_ctrl = MunicipalityController(baselines)
    microregion_ctrl = MicroregionController(baselines)
    mesoregion_ctrl = MesoregionController(baselines)

    resultados_ano = []
    resultados_ano.extend(school_ctrl.process_all_schools(df_ano))
    resultados_ano.extend(municipality_ctrl.process_all_municipalities(df_ano))
    resultados_ano.extend(microregion_ctrl.process_all_microregions(df_ano))
    resultados_ano.extend(mesoregion_ctrl.process_all_mesoregions(df_ano))

    
    for req in resultados_ano:
            req['ano'] = ano
    
    final_prescriptions.extend(resultados_ano)
    print(f"[{ano}] Processamento concluído. {len(resultados_ano)} prescrições geradas.")
    print("-" * 50)
    
    return pd.DataFrame(final_prescriptions)

if __name__ == "__main__":
    CAMINHO_DB = "../arkhos.db"

    ANOS_PARA_PROCESSAR = [2024]
    
    df_results = run_prescriptive_engine(CAMINHO_DB, ANOS_PARA_PROCESSAR)
    
    if df_results is not None and not df_results.empty:
        pasta_data = "data"
        os.makedirs(pasta_data, exist_ok=True)
        
        caminho_csv = os.path.join(pasta_data, "dummy_data.csv")

        cols = df_results.columns.tolist()
        if 'ano' in cols:
            cols.insert(2, cols.pop(cols.index('ano')))
            df_results = df_results[cols]
            
        df_results.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
        
        print(f"\n=======================================================")
        print(f"SUCESSO! Total de {len(df_results)} prescrições exportadas.")
        print(f"Arquivo gerado em: {caminho_csv}")
        print(f"=======================================================")