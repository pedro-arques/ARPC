import csv
import pandas as pd
from collections import Counter

class Construtor:
    def __init__(self):
        self.nome_arquivo = 'a.csv'
        self.lista_valores_divididos = []

    def abrirCSV(self):
        df = pd.read_csv(self.nome_arquivo)
        coluna_produto = df['produto']
        for index, row in df.iterrows():
            valores_divididos = row['produto'].split(',')
            self.lista_valores_divididos.extend(valores_divididos)  # Usamos extend para adicionar todos os valores à lista

        # Usamos Counter para contar a frequência dos valores
        contagem_valores = Counter(self.lista_valores_divididos)

        # Obter os 10 valores mais comuns
        top_10 = contagem_valores.most_common(200)

        print("Top 10 valores mais comuns:")
        for valor, frequencia in top_10:
            print(f"{valor},{frequencia}")

a = Construtor()
a.abrirCSV()

