import subprocess

# Variables globales
file_name = "./docker-compose.yml"
number_of_workers = "2"
number_of_lean_workers = "2"
number_of_joiners_ratings = "1"
number_of_reduce_by_country_sum_budgets = "1"
number_of_reduce_top_5_by_budgets = "1"
number_of_reduce_by_sentiment = "1"
number_of_reduce_by_actor = "1"
number_of_reduce_top_10_by_actor = "1"
number_of_reduce_top_bottom_avg_ratings = "1"
number_of_clients = "2"

# Ejecutar el script con todas las variables en orden
subprocess.run([
    "./generate-compose.sh",
    file_name,
    number_of_workers,
    number_of_lean_workers,
    number_of_joiners_ratings,
    number_of_reduce_by_country_sum_budgets,
    number_of_reduce_top_5_by_budgets,
    number_of_reduce_by_sentiment,
    number_of_reduce_by_actor,
    number_of_reduce_top_10_by_actor,
    number_of_reduce_top_bottom_avg_ratings,
    number_of_clients,
])