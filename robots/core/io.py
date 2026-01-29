import os
import glob
import time
import pandas as pd

def clean_tmp_folder(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)
        return
        
    files = glob.glob(os.path.join(folder, "*"))
    for f in files:
        try:
            os.remove(f)
        except Exception:
            pass

def wait_and_read_csv(downloads_folder, max_wait_time=60):
    start = time.time()

    while (time.time() - start) < max_wait_time:
        files = [
            f for f in os.listdir(downloads_folder)
            if os.path.isfile(os.path.join(downloads_folder, f))
            and not f.startswith('.')
        ]

        if not files:
            time.sleep(1)
            continue

        path = os.path.join(downloads_folder, files[0])

        # Ignora arquivos temporários de download do Chrome
        if path.endswith(('.crdownload', '.tmp')):
            time.sleep(1)
            continue

        # Tenta ler com diferentes configurações
        encodings = ['utf-8', 'latin-1', 'cp1252']
        seps = [';', ',', '\t']

        for enc in encodings:
            for sep in seps:
                try:
                    df = pd.read_csv(
                        path, sep=sep, encoding=enc,
                        engine='python', on_bad_lines='skip'
                    )
                    # Validação básica se leu corretamente
                    if df.shape[1] > 2:
                        # Remove o arquivo após ler com sucesso (limpeza)
                        try:
                            os.remove(path)
                        except:
                            pass
                        return df
                except:
                    pass
        
        # Se falhou todas as tentativas de leitura no arquivo atual
        try:
            os.remove(path)
        except:
            pass
        return None

    return None


def save_consolidated_df(df, output_folder, filename):
    if df is None or df.empty:
        print(f" [!] DataFrame vazio. Nada salvo para {filename}.")
        return False

    try:
        os.makedirs(output_folder, exist_ok=True)
        
        file_path = os.path.join(output_folder, filename)
        
        df.to_csv(file_path, index=False, sep=';', encoding='utf-8-sig')
        
        print(f" [ok] ARQUIVO SALVO: {file_path} com {len(df)} registros.")
        return True
        
    except Exception as e:
        print(f" [X] Erro crítico ao salvar {filename}: {e}")
        return False