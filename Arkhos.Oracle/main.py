import pandas as pd
import sqlite3
import os
from controllers.school_controller import SchoolController
from controllers.municipality_controller import MunicipalityController
from controllers.microregion_controller import MicroregionController
from controllers.mesoregion_controller import MesoregionController

def carregar_dados_banco(db_path: str) -> pd.DataFrame:
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
        
        WHERE r.ano = 2024
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
    
    print("Extraindo dados consolidados do arkhos.db...")
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("\nCOLUNAS CARREGADAS NO PANDAS:")
    print(df.columns.tolist())
    print("="*40)
    
    return df

def run_prescriptive_engine(db_path: str):
    
    try:
        df = carregar_dados_banco(db_path)
        print(f"Dados carregados! Total de escolas a analisar: {len(df)}")
    except Exception as e:
        print(f"Erro ao carregar dados do banco: {e}")
        return None
    

    school_ctrl = SchoolController()
    municipality_ctrl = MunicipalityController()
    microregion_ctrl = MicroregionController()
    mesoregion_ctrl = MesoregionController()

    
    final_prescriptions = []
    
    final_prescriptions.extend(school_ctrl.process_all_schools(df))
    final_prescriptions.extend(municipality_ctrl.process_all_municipalities(df))
    final_prescriptions.extend(microregion_ctrl.process_all_microregions(df))
    final_prescriptions.extend(mesoregion_ctrl.process_all_mesoregions(df))
    
    results_df = pd.DataFrame(final_prescriptions)
    
    print(f"Processamento concluído. {len(results_df)} prescrições geradas.")
    return results_df

if __name__ == "__main__":
    CAMINHO_DB = "../arkhos.db" 
    
    df_results = run_prescriptive_engine(CAMINHO_DB)
    
    if df_results is not None and not df_results.empty:
        
        pasta_data = "data"
        os.makedirs(pasta_data, exist_ok=True)
        
        caminho_csv = os.path.join(pasta_data, "dummy_data.csv")
        
        df_results.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
        
        print(f"Resultados exportados com sucesso para visualizar por completo em: {caminho_csv}")