import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import shap

class ArkhosPlotter:
    def __init__(self, output_dir: str = "output_plots"):
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        sns.set_theme(style="whitegrid", palette="muted")
        plt.rcParams.update({'font.size': 12, 'figure.dpi': 300})

    def plot_correlation_matrix(self, X: pd.DataFrame, target_name: str):
        plt.figure(figsize=(12, 10))
        corr = X.corr()
        
        mask = np.triu(np.ones_like(corr, dtype=bool))
        
        sns.heatmap(corr, mask=mask, annot=False, cmap="coolwarm", 
                    vmax=1, vmin=-1, center=0, square=True, 
                    linewidths=.5, cbar_kws={"shrink": .7})
        
        plt.title(f"Matriz de Correlação das Features ({target_name})", pad=20, fontsize=16)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, f"correlacao_{target_name}.png")
        plt.savefig(filepath, bbox_inches='tight')
        plt.close()
        print(f"[Plot] Matriz de correlação salva em: {filepath}")
    
    def plot_feature_importance(self, model, feature_names: list, target_name: str):
        """
        Extrai a importância nativa do modelo de árvore (Gini Importance).
        Gera um gráfico de barras com os top N indicadores mais relevantes.
        """
        importances = model.feature_importances_
        indices = np.argsort(importances)[::-1]
        
        top_n = min(10, len(feature_names))
        top_indices = indices[:top_n]
        top_features = [feature_names[i] for i in top_indices]
        top_importances = importances[top_indices]

        plt.figure(figsize=(10, 6))
        sns.barplot(x=top_importances, y=top_features, palette="viridis")
        
        plt.title(f"Top {top_n} Indicadores de Maior Impacto ({target_name})", pad=15, fontsize=14, fontweight='bold')
        plt.xlabel("Importância Relativa (Peso no Modelo)", fontsize=12)
        plt.ylabel("Indicadores", fontsize=12)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, f"model_importance_{target_name}.png")
        plt.savefig(filepath, bbox_inches='tight')
        plt.close()
        print(f"[Plot] Importância dos atributos salva em: {filepath}")

    def plot_model_validation(self, df_metricas: pd.DataFrame):
        plt.figure(figsize=(10, 6))
        
        nomes_eixos = {
            'Performance': 'Risco de Evasão',
            'Infrastructure': 'Déficit de Acessibilidade',
            'Management': 'Instabilidade Docente'
        }
        df_metricas['Eixo_PT'] = df_metricas['Eixo'].map(nomes_eixos)

        ax = sns.barplot(
            data=df_metricas, 
            x='Eixo_PT', 
            y='Detecção de Risco (%)', 
            palette=["#ff6b6b", "#4ecdc4", "#45b7d1"]
        )
        
        for i in ax.containers:
            ax.bar_label(i, fmt='%.1f%%', padding=5, fontsize=12, fontweight='bold')

        plt.title("Acurácia de Detecção de Risco pelo Arkhos (Backtesting 2024)", pad=20, fontsize=16, fontweight='bold')
        plt.xlabel("Modelo Analítico", fontsize=12)
        plt.ylabel("Acerto na Classificação de Risco (%)", fontsize=12)
        plt.ylim(0, 105)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, "validacao_modelos_acuracia_risco.png")
        plt.savefig(filepath, bbox_inches='tight')
        plt.close()
        print(f"[Plot] Gráfico de risco salvo em: {filepath}")

    def plot_shap_summary(self, shap_values, X_eval: pd.DataFrame, target_name: str):
        plt.figure(figsize=(10, 8))
        
        # O shap.summary_plot renderiza diretamente na figura atual do matplotlib
        shap.summary_plot(shap_values, X_eval, show=False, plot_size=(10, 8))
        
        plt.title(f"Impacto Marginal dos Indicadores (SHAP) - {target_name}", pad=20, fontsize=14)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, f"shap_summary_{target_name}.png")
        plt.savefig(filepath, bbox_inches='tight')
        plt.close()
        print(f"[Plot] SHAP Summary salvo em: {filepath}")

    def plot_risk_distribution(self, df_results: pd.DataFrame, target_name: str):
        plt.figure(figsize=(8, 5))
        
        sns.histplot(data=df_results, x='risco_predito', kde=True, color="crimson", bins=30)
        
        plt.title(f"Distribuição do Risco Predito ({target_name})", pad=15)
        plt.xlabel("Risco Projetado")
        plt.ylabel("Densidade de Unidades Escolares")
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, f"distribuicao_risco_{target_name}.png")
        plt.savefig(filepath, bbox_inches='tight')
        plt.close()
        print(f"[Plot] Distribuição de risco salva em: {filepath}")