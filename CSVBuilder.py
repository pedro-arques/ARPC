import csv
from Conexão import Conectar

class Construtor():

    def __init__(self):
        self.database = Conectar()

    def montarCSVARPC(self, dados):
        self.campos = ['cpf', 'nome', 'filial', 'telefone','produto']
        self.nome_arquivo = 'a.csv'
        with open(self.nome_arquivo, mode='w', newline='', encoding='utf-8') as arquivo_csv:
            escritor_csv = csv.writer(arquivo_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            escritor_csv.writerow(self.campos)
            for i in range(len(dados)):
                escritor_csv.writerow(dados[i])
    
    def exportarARPC(self, dados):
        self.campos = ['idsexo', 'idade', 'profissão', 'marca', 'tipo', 'subtipo', 'idproduto', 'idmarca']
        self.nome_arquivo = 'a.csv'
        with open(self.nome_arquivo, mode='w', newline='', encoding='utf-8') as arquivo_csv:
            escritor_csv = csv.writer(arquivo_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            escritor_csv.writerow(self.campos)
            for i in range(len(dados)):
                escritor_csv.writerow(dados[i])