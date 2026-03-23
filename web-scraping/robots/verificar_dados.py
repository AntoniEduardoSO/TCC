import pandas as pd
import os

def verificar_dados():
    # Caminho exato que você passou 
    caminho_arquivo = "/Users/katochi/Dev/TCC/data/Transparencia/BARRAO_DE_SAO_MIGUEL_CONSOLIDADO.csv.gz"

    if os.path.exists(caminho_arquivo):
        print(f"📂 Lendo arquivo: {caminho_arquivo} ...")
        
        try:
            # O segredo é compression='gzip' e o separador ';' que usamos ao salvar
            df = pd.read_csv(caminho_arquivo, compression='gzip', sep=';', encoding='utf-8')
            
            print("\n" + "="*40)
            print("RESUMO DOS DADOS")
            print("="*40)
            print(f"✅ Total de Registros (Linhas): {df.shape[0]}")
            print(f"✅ Total de Colunas:           {df.shape[1]}")
            
            print("\n📋 Colunas Encontradas:")
            print(df.columns.tolist())
            
            print("\n👀 Amostra das 5 primeiras linhas:")
            pd.set_option('display.max_columns', None) # Mostra todas as colunas no print
            print(df.head())
            
            print("\n🗓️ Verificação de Anos:")
            if 'ano_referencia' in df.columns:
                print(df['ano_referencia'].value_counts().sort_index())
            else:
                print("⚠️ Coluna 'ano_referencia' não encontrada!")

        except Exception as e:
            print(f"❌ Erro ao ler o arquivo: {e}")
    else:
        print(f"❌ Arquivo não encontrado no caminho: {caminho_arquivo}")