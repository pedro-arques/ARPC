import pandas as pd
import psycopg2
import time

class CSVinput:
    
    def __init__(self):
        self.login, self.cur = self.conexaoDatabase()
        self.lista = self.lerCSV()
        
    def conexaoDatabase(self):
        con = psycopg2.connect(host='X', 
                            database='X',
                            user='X', 
                            password='X')
        cur = con.cursor()
        return con, cur

    def lerCSV(self):
        lista_perfis = pd.read_csv('a.csv', header=0)
        return lista_perfis

    def insert(self):
        for i in self.lista.itertuples(index=False):
            print(i)
            print('-------------------')
            time.sleep(1)
            # Divida a string em uma lista de números inteiros
            self.cur.execute('''
                INSERT INTO X (X, X, X, X, X, X, X)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (i.idsexo, i.idade, i.profissão, i.idmarca, i.subtipo, i.tipo, i.idproduto))

        if not self.login.closed:
            self.login.commit()
        self.cur.close()
# Criando uma instância da classe e chamando o método
a = CSVinput()
a.insert()
