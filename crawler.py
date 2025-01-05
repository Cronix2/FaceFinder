import threading
import queue
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
global error

error = 0

# Configuration
start_site = "https://en.wikipedia.org/wiki/Main_Page"  # Point de départ
sites_per_thread = 100  # Nombre de sites par thread
num_threads = 10  # Nombre de threads
result_file = "result.txt"
error_file = "error_crawling.txt"

# Structures pour le crawler
waiting_list = queue.Queue()
waiting_list.put(start_site)
visited_lock = threading.Lock()
stop_crawling = threading.Event()  # Événement global pour arrêter les threads


# Fonction principale pour crawler les sites
def crawl_site():
    global error
    local_sites_visited = 0
    while not stop_crawling.is_set():
        try:
            site = waiting_list.get(timeout=1)  # Récupérer un site ou lever une exception si vide
        except queue.Empty:
            continue  # Réessayer tant que le signal d'arrêt n'est pas défini

        try:
            response = requests.get(site, timeout=5)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extraire les liens
                new_links = set(a['href'] for a in soup.find_all('a', href=True))
                absolute_links = {
                    link if link.startswith('http') else requests.compat.urljoin(site, link)
                    for link in new_links
                }

                # Ajouter les nouveaux liens à la liste d'attente
                with visited_lock:
                    for link in absolute_links:
                        if not stop_crawling.is_set():
                            waiting_list.put(link)

                # Ajouter le site visité au fichier result.txt
                with visited_lock:
                    with open(result_file, "a", encoding="utf-8") as file:
                        file.write(site + "\n")

                local_sites_visited += 1
                if local_sites_visited >= sites_per_thread:
                    break  # Arrêter ce thread si la limite locale est atteinte

        except Exception as e:
            # Sauvegarder les erreurs
            with open(error_file, "a", encoding="utf-8") as f:
                f.write(f"Error crawling site {site}: {e}\n")
            error += 1
        finally:
            waiting_list.task_done()

    # Vérification finale
    if waiting_list.empty() or stop_crawling.is_set():
        stop_crawling.set()  # Signaler aux autres threads d'arrêter


# Vérification et suppression des doublons
def check_duplicates(file_path):
    seen = set()
    duplicates = []

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                site = line.strip()
                if site in seen:
                    duplicates.append(site)
                else:
                    seen.add(site)
    except FileNotFoundError:
        print(f"Fichier {file_path} introuvable.")
    except Exception as e:
        print(f"Erreur lors de la vérification des doublons : {e}")

    return len(duplicates), list(seen)


def remove_duplicates(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            sites = file.read().splitlines()

        unique_sites = sorted(set(sites))
        num_duplicates = len(sites) - len(unique_sites)

        with open(file_path, "w", encoding="utf-8") as file:
            for site in unique_sites:
                file.write(site + "\n")

        print(f"Les doublons ont été supprimés : {num_duplicates} doublons supprimés.")
        print(f"Total de {len(unique_sites)} sites uniques restants.")
    except FileNotFoundError:
        print(f"Fichier {file_path} introuvable.")
    except Exception as e:
        print(f"Erreur lors de la suppression des doublons : {e}")


# Création et lancement des threads
if __name__ == "__main__":
    threads = []
    total_sites_to_crawl = sites_per_thread * num_threads

    with tqdm(total=total_sites_to_crawl, desc="Crawling Progress") as pbar:
        for _ in range(num_threads):
            thread = threading.Thread(target=crawl_site)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        while True:
            with visited_lock:
                try:
                    with open(result_file, "r", encoding="utf-8") as file:
                        sites_found = len(file.readlines())
                except FileNotFoundError:
                    sites_found = 0

            pbar.n = sites_found
            pbar.refresh()

            if sites_found >= total_sites_to_crawl:
                stop_crawling.set()  # Indiquer aux threads d'arrêter
                break
            # time.sleep(0.5)

    # Attendre la fin des threads
    for thread in threads:
        thread.join(timeout=5)  # Timeout pour éviter un blocage infini

    # Suppression des doublons
    print("\nVérification et suppression des doublons dans le fichier...")
    num_duplicates, unique_sites = check_duplicates(result_file)
    remove_duplicates(result_file)

    # Résumé des résultats
    print(f"\nRésumé :")
    print(f"- Nombre de doublons trouvés : {num_duplicates}")
    print(f"- Nombre de sites uniques restants : {len(unique_sites)}")
    print(f"- Nombre d'erreurs de crawling : {error}")
