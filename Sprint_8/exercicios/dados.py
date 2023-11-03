import random
import time
import os
import names

random.seed(40)
qtd_nomes_unicos = 3000
qtd_nomes_aleatorios = 10000000

aux = [names.get_full_name() for _ in range(qtd_nomes_unicos)]
print(f"Gerando {qtd_nomes_aleatorios} nomes aleatórios")
dados = [random.choice(aux) for _ in range(qtd_nomes_aleatorios)]

file = "nomes_aleatorios.txt"
with open(file, "w") as arquivo:
    for nome in dados:
        arquivo.write(nome + "\n")

print(f"Dataset de nomes gerado e salvo em '{file}'")
