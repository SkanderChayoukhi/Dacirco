import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Configuration globale
sns.set(style="whitegrid")
plt.rcParams.update({'figure.autolayout': True})

# Fonction pour analyser et comparer les métriques
def analyze_and_plot(csv_files, output_dir="output_graphs"):
    """
    Compare les métriques des fichiers CSV et génère des graphiques comparatifs.
    
    :param csv_files: Liste des chemins vers les fichiers CSV.
    :param output_dir: Dossier où sauvegarder les graphiques.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    combined_data = []

    # Lire chaque fichier CSV et ajouter une colonne 'algorithm' pour différencier
    for file in csv_files:
        try:
            algo_name = os.path.basename(file).split('-')[0]  # Extraire le nom de l'algorithme
            print(f"Traitement du fichier : {file} (algorithme: {algo_name})")
            df = pd.read_csv(file)
            
            # Vérifier que la longueur des colonnes est cohérente
            if len(df) == 0:
                print(f"Le fichier {file} est vide.")
                continue
            
            # Ajouter une colonne 'algorithm' et vérifier la structure des colonnes
            df['algorithm'] = algo_name
            combined_data.append(df)

        except Exception as e:
            print(f"Erreur avec le fichier {file} : {e}")

    # Fusionner toutes les données
    all_data = pd.concat(combined_data, ignore_index=True)
    print(f"Données fusionnées, taille finale : {all_data.shape}")

    # Vérification des colonnes et nettoyage
    print("\n--- Aperçu des données fusionnées ---")
    print(all_data.info())

    # Fonction pour convertir les colonnes de temps en float (en secondes)
    def convert_time_to_seconds(df, column_name):
        try:
            df[column_name] = pd.to_timedelta(df[column_name], errors='coerce').dt.total_seconds()
            df = df.dropna(subset=[column_name])  # Supprimer les lignes avec NaN dans cette colonne
            print(f"--- Statistiques sur '{column_name}' ---")
            print(df[column_name].describe())
        except Exception as e:
            print(f"Erreur lors de la conversion de la colonne {column_name} : {e}")
        return df

    # Convertir les colonnes de temps (en secondes)
    for time_column in ['t_diff_wait', 't_diff_tc', 't_diff_dwnld', 't_diff_upld']:
        all_data = convert_time_to_seconds(all_data, time_column)


    # Comparer 't_diff_wait' (temps d'attente)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=all_data, x='algorithm', y='t_diff_wait', palette="Set2")
    plt.title("Comparaison du Temps d'Attente par Algorithme")
    plt.ylabel("Temps d'Attente (s)")
    plt.xlabel("Algorithme")
    plt.savefig(f"{output_dir}/compare_t_diff_wait.png")
    plt.close()

    # Comparer 't_diff_tc' (temps de traitement)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=all_data, x='algorithm', y='t_diff_tc', palette="Set3")
    plt.title("Comparaison du Temps de Traitement par Algorithme")
    plt.ylabel("Temps de Traitement (s)")
    plt.xlabel("Algorithme")
    plt.savefig(f"{output_dir}/compare_t_diff_tc.png")
    plt.close()

    # Comparer 't_diff_dwnld' (temps de téléchargement)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=all_data, x='algorithm', y='t_diff_dwnld', palette="muted")
    plt.title("Comparaison du Temps de Téléchargement par Algorithme")
    plt.ylabel("Temps de Téléchargement (s)")
    plt.xlabel("Algorithme")
    plt.savefig(f"{output_dir}/compare_t_diff_dwnld.png")
    plt.close()

    # Comparer 't_diff_upld' (temps de téléchargement)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=all_data, x='algorithm', y='t_diff_upld', palette="coolwarm")
    plt.title("Comparaison du Temps de Téléchargement (Upload) par Algorithme")
    plt.ylabel("Temps de Téléchargement (s)")
    plt.xlabel("Algorithme")
    plt.savefig(f"{output_dir}/compare_t_diff_upld.png")
    plt.close()
    # Vérification des valeurs des colonnes d'intérêt
    if 'load1_avg' in all_data.columns:
        print("\n--- Statistiques sur 'load1_avg' ---")
        print(all_data['load1_avg'].describe())

    if 'used_mem_avg' in all_data.columns:
        print("\n--- Statistiques sur 'used_mem_avg' ---")
        print(all_data['used_mem_avg'].describe())



    # Comparer 'load1_avg' (charge moyenne des nœuds)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=all_data, x='algorithm', y='load1_avg', palette="Set2")
    plt.title("Comparaison de la Charge Moyenne des Nœuds par Algorithme")
    plt.ylabel("Charge Moyenne")
    plt.xlabel("Algorithme")
    plt.savefig(f"{output_dir}/compare_load1_avg.png")
    plt.close()

    # Comparer 'used_mem_avg' (mémoire moyenne utilisée)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=all_data, x='algorithm', y='used_mem_avg', palette="Set3")
    plt.title("Comparaison de la Mémoire Moyenne Utilisée par Algorithme")
    plt.ylabel("Mémoire Moyenne Utilisée")
    plt.xlabel("Algorithme")
    plt.savefig(f"{output_dir}/compare_used_mem_avg.png")
    plt.close()

    print(f"Graphiques sauvegardés dans le dossier : {output_dir}")

# Liste des fichiers à comparer
csv_files = [
    "agp-kubernetes-ok-requests-node-stats.csv",
    "pbd-kubernetes-ok-requests-node-stats.csv",
    "swq-kubernetes-ok-requests-node-stats.csv",
    "swr-kubernetes-ok-requests-node-stats.csv"
]

# Appel de la fonction
analyze_and_plot(csv_files)
