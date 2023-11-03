import random

lista_inteiros = [random.randint(1, 1000) for _ in range(250)]

lista_inteiros.reverse()

print(lista_inteiros)

animais = ["Elefante", "Leao", "Girafa", "Tigre", "Rinoceronte", "Zebra", "Hipopotamo", "Cobra", "Panda", "Kanguru", "Gorila", "Jacare", "Orangotango", "Pinguim", "Macaco", "Urso Polar", "Cavalo", "Camelo", "Avestruz", "Lobo"]


animais_ordenados = sorted(animais)


[print(animal) for animal in animais_ordenados]


with open("animais.csv", "w") as arquivo:
    for animal in animais_ordenados:
        arquivo.write(animal + "\n")
        