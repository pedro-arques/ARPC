import joblib
import numpy as np
#import sklearn
import pandas as pd
import unidecode
#import csv
from Conexão import Conectar
from CSVBuilder import Construtor

class ARPC():

    def __init__(self):
        self.KNN_modelo_tipo, self.KNN_modelo_subtipo, self.KNN_modelo_marca = self.carregarModelos()
        self.database = Conectar()
        self.csv = Construtor()
        self.filial = (34,)

        self.produtos_csv = pd.read_csv('Produtos 4.0.csv', on_bad_lines='skip', header = 0, delimiter = ';',encoding='latin-1')
        self.produtos = self.dataframeProdutosAtivos()
        self.lista_cpfs = self.carregarCsv()
        self.dataframe_perfil, self.cpfs, self.filiais, self.telefone, self.nome, self.perfilf = self.perfil_cliente()
        self.perfil = self.perfilCliente(str(self.dataframe_perfil))
        self.pred_tipo, self.pred_subtipo, self.pred_marca = self.modelosPred()
        self.coluna_subtipo, self.coluna_tipo, self.coluna_marca = self.carregarColunas()

    def carregarPerfis(self,lista_cpfs):
        resultadoQueryLista = self.database.executarQueryPerfiles(self.filial,lista_cpfs)
        return resultadoQueryLista

    def perfilCliente(self, entrada):
        self.perfil = np.array(eval(entrada))
        return self.perfil
    
    def buscarPerfis(self,perfil):
        resultadoQueryPerfil = self.database.executarQueryPerfil(perfil)
        return resultadoQueryPerfil

    def dataframeProdutosAtivos(self):
        df_produtos = pd.DataFrame(self.produtos_csv)
        df_produtos = df_produtos.applymap(lambda x: unidecode.unidecode(str(x).upper()) if pd.notnull(x) else x)
        df_produtos['tipo'] = df_produtos['tipo'].apply(lambda x: x.replace(' ', '') if isinstance(x, str) else x)
        df_produtos['subtipo'] = df_produtos['subtipo'].apply(lambda x: x.replace(' ', '') if isinstance(x, str) else x)
        df_produtos['marca'] = df_produtos['marca'].apply(lambda x: x.replace(' ', '') if isinstance(x, str) else x)
        df_produtos['idmarca'] = df_produtos['idmarca'].apply(lambda x: x.replace(' ', '') if isinstance(x, str) else x)
        df_produtos.to_csv('produtos dataframe.csv', sep=',', encoding='utf-8')
        return df_produtos

    def carregarModelos(self):
        KNN_modelo_tipo = joblib.load(r'modelo_tipo.pkl')
        KNN_modelo_subtipo = joblib.load(r'modelo_subtipo.pkl')
        KNN_modelo_marca = joblib.load(r'modelo_marca.pkl')
        return KNN_modelo_tipo, KNN_modelo_subtipo, KNN_modelo_marca

    def carregarColunas(self):
        coluna_subtipo = pd.read_csv('y_subtipo df.csv', on_bad_lines='skip', header = 0, delimiter = ',',encoding='latin-1')
        coluna_tipo = pd.read_csv('y_tipo df.csv', on_bad_lines='skip', header = 0, delimiter = ',',encoding='latin-1')
        coluna_marcas = pd.read_csv('y_marca df.csv', on_bad_lines='skip', header = 0, delimiter = ',',encoding='latin-1')
        return coluna_subtipo, coluna_tipo, coluna_marcas

    def modelosPred(self):
        pred_tipo_lista = []
        pred_subtipo_lista = []
        pred_marca_lista = []
        for perfil in self.perfil:
            pred_tipo_lista.append(self.KNN_modelo_tipo.predict(perfil.reshape(1,-1)))
            pred_subtipo_lista.append(self.KNN_modelo_subtipo.predict(perfil.reshape(1,-1)))
            pred_marca_lista.append(self.KNN_modelo_marca.predict(perfil.reshape(1,-1)))
        return pred_tipo_lista , pred_subtipo_lista, pred_marca_lista

    def resetContadores(self):
        self.dictt = {}
        self.contagem = 0
        return self.dictt, self.contagem

    def top(self):
        pred_tipo, pred_subtipo, pred_marca, = self.modelosPred()
        top_subtipos = []
        top_marcas = []
        for i in range(len(pred_subtipo)):
            dictt, contagem = self.resetContadores()
            for i in pred_subtipo[i][0]:
                dictt.update({self.coluna_subtipo.iloc[contagem].item(): i})
                contagem += 1
            top_subtipos.append(sorted(dictt.items(), key=lambda x: x[1], reverse=True)[:20])
        for i in range(len(pred_marca)):
            dictt, contagem = self.resetContadores()
            for i in pred_marca[i][0]:
                dictt.update({self.coluna_marca.iloc[contagem].item(): i})
                contagem += 1
            top_marcas.append(sorted(dictt.items(), key=lambda x: x[1], reverse=True)[:15])
        return top_subtipos, top_marcas
            
    # def topTipos(self):
    #     dictt, contagem = self.resetContadores()
    #     pred_tipo, _, _, = self.modelosPred()
    #     for i in list(pred_tipo[0]):
    #         dictt.update({self.coluna_tipo.iloc[contagem].item(): i})
    #         contagem += 1
    #     top_tipos = sorted(dictt.items(), key=lambda x: x[1], reverse=True)[:20]
    #     return top_tipos

    # def topSubtipos(self):
    #     _, pred_subtipo, _, = self.modelosPred()
    #     top_subtipos = []
    #     for i in range(len(pred_subtipo)):
    #         dictt, contagem = self.resetContadores()
    #         for i in pred_subtipo[i][0]:
    #             dictt.update({self.coluna_subtipo.iloc[contagem].item(): i})
    #             contagem += 1
    #         top_subtipos.append(sorted(dictt.items(), key=lambda x: x[1], reverse=True)[:20])
    #     return top_subtipos

    # def topMarcas(self):
    #     _, _, pred_marca = self.modelosPred()
    #     top_marcas = []
    #     for i in range(len(pred_marca)):
    #         dictt, contagem = self.resetContadores()
    #         for i in pred_marca[i][0]:
    #             dictt.update({self.coluna_marca.iloc[contagem].item(): i})
    #             contagem += 1
    #         top_marcas.append(sorted(dictt.items(), key=lambda x: x[1], reverse=True)[:15])
    #     return top_marcas

    def dfModeloSM(self):
        subtipos, marcas = self.top()
        d = []
        for i in range(len(subtipos)):
            dfsm = pd.concat([pd.DataFrame(subtipos[i]), pd.DataFrame(marcas[i])], axis=1)
            dfsm = dfsm.applymap(lambda x: unidecode.unidecode(str(x).upper()) if pd.notnull(x) else x)
            dfsm = dfsm.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            pc = dfsm.iloc[:, 0]
            tc = dfsm.iloc[:, 2]
            dfsm = pd.DataFrame({'subtipo': pc, 'marca': tc})
            d.append(dfsm)
        return d

    def dfModeloT(self):
        dft = pd.DataFrame({'tipo': self.topTipos()})
        dft = dft.applymap(lambda x: unidecode.unidecode(str(x).upper()) if pd.notnull(x) else x)
        dft.columns = ['tipo']
        return dft

    def filtragem(self):
        lista_idproduto = []
        lista_marca = []
        lista_tipo = []
        lista_subtipo = []
        lista_idmarca = []
        perfil_completo = []
        produtos_sm = self.dfModeloSM()
        for i in range(min(len(self.dataframe_perfil), len(produtos_sm))):
            df1_filtered1 = self.produtos[self.produtos['subtipo'].isin(produtos_sm[i]['subtipo'])]
            df1_filtered1 = df1_filtered1[df1_filtered1['marca'].isin(produtos_sm[i]['marca'])]
            lista_idproduto.append(','.join(df1_filtered1[['idproduto']].astype(str).agg(','.join, axis=1)))
            lista_marca.append(','.join(df1_filtered1[['marca']].astype(str).agg(','.join, axis=1)))
            lista_tipo.append(','.join(df1_filtered1[['tipo']].astype(str).agg(','.join, axis=1)))
            lista_subtipo.append(','.join(df1_filtered1[['subtipo']].astype(str).agg(','.join, axis=1)))
            lista_idmarca.append(','.join(df1_filtered1[['idmarca']].astype(str).agg(','.join, axis=1)))

        for i in range(min(len(self.dataframe_perfil), len(lista_idproduto))):
            perfil_completo.append([str(item) for item in self.perfilf[i]] + [lista_marca[i]] + [lista_tipo[i]] + [lista_subtipo[i]] + [lista_idproduto[i]] + [lista_idmarca[i]])
        self.csv.exportarARPC(perfil_completo)
        return lista_idproduto

    
    # def filtragem(self):
    #     lista_produto = []
    #     perfil_completo = []
    #     produtos_sm = self.dfModeloSM()
    #     for i in range(len(self.perfil)):
    #         df1_filtered1 = self.produtos[self.produtos['subtipo'].isin(produtos_sm[i]['subtipo'])]
    #         df1_filtered1 = df1_filtered1[df1_filtered1['marca'].isin(produtos_sm[i]['marca'])]
    #         # df1_filtered1 = df1_filtered1.reset_index(drop=True)
    #         lista_produto.append(','.join(df1_filtered1['produto']))
    #     for i in range(len(self.perfil)):
    #             perfil_completo.append([self.cpfs[i], self.nome[i], self.filiais[i], self.telefone[i], lista_produto[i]])
    #     self.csv.montarCSV(perfil_completo)
    #     return lista_produto

    # def filtragem(self):
    #     l = []
    #     li = []
    #     produtos_subtipo = self.dfModeloSM()
    #     produtos_marca = self.dfModeloSM()
    #     #lista_produtos = self.database.queryProdutos([64431,68653,65861,68660,67196,64783,66072,64886,67195,82838,55879,66250,65823,59451,66188,66075,67193,64536,59586,66280,66281])
    #     #lista_produtos = self.database.queryProdutos([68824, 65931, 68058, 65728, 68996, 68706, 68683, 68684, 66911, 67703, 68309, 68631, 68032, 59189, 68004, 68994])
    #     lista_produtos = self.database.queryProdutos([68684, 65931, 68032, 65728, 59189, 67996, 67102, 66382, 58196, 66982, 67542, 67544,68855, 67749, 67073, 68669, 54508, 67209, 68823, 68824, 68683, 68706, 68994, 68996, 67173, 65703, 67997, 67426, 68916, 65223, 68296, 64224, 68426, 67671, 68825, 67672, 68428, 68821, 67640, 67161, 68030, 65194, 67777, 65780, 65792, 68035, 65192, 65810, 66385, 68530, 68529, 68723, 68011, 66363, 68535, 68780, 68951, 68952, 68631, 68309, 68004])
    
    #     for i in range(len(self.perfil)):
    #         df1_filtered1 = self.produtos[self.produtos['subtipo'].isin(produtos_subtipo[i]['subtipo'])]
    #         df1_filtered1 = df1_filtered1[df1_filtered1['marca'].isin(produtos_marca[i]['marca'])]
    #         # df1_filtered1 = df1_filtered1.reset_index(drop=True)
    #         l.append(df1_filtered1)
    #     for i in range(len(self.perfil)):
    #         produto_buscado = l[i][l[i]['produto'].str.contains('|'.join(lista_produtos), case=False, na=False)]
    #         if not produto_buscado.empty:
    #             produto_string = produto_buscado['produto'].astype(str).str.cat(sep=', ')
    #             li.append([self.cpfs[i], self.nome[i], self.filiais[i], self.telefone[i], produto_string])
    #     self.csv.montarCSV(li)
    #     return l

    def carregarCsv(self):
        # lista_cpfs = pd.read_excel('Base clientes teste algoritmo.xlsx', header = 0)
        #lista_cpfs = self.database.executarQueryFilial(13)
        lista_cpfs = self.database.executarQueryFilial(self.filial)
        # lista_cpfs = pd.DataFrame(lista_cpfs)
        # lista_cpfs = lista_cpfs['cpf'].tolist()
        # lista_cpfs = [str(item) for item in lista_cpfs]
        return lista_cpfs

    def relacaoCsvPerfil(self):
        b = self.carregarPerfis(self.lista_cpfs)
        return b

    def perfil_cliente(self):
        lista = []
        listaf = []
        cpfs = []
        filiais = []
        telefone = []
        nome = []
        for i in self.relacaoCsvPerfil():
            if i[3] == 'Aposentado':
                lista.append([i[1], i[2], 1,0,0,0,0,0,0,0,0,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Assalariado':
                lista.append([i[1], i[2], 0,1,0,0,0,0,0,0,0,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Autônomo':
                lista.append([i[1], i[2], 0,0,1,0,0,0,0,0,0,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Do Lar':
                lista.append([i[1], i[2], 0,0,0,1,0,0,0,0,0,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Empresário':
                lista.append([i[1], i[2], 0,0,0,0,1,0,0,0,0,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Estudante':
                lista.append([i[1], i[2], 0,0,0,0,0,1,0,0,0,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Funcionário público':
                lista.append([i[1], i[2], 0,0,0,0,0,0,1,0,0,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Militar':
                lista.append([i[1], i[2], 0,0,0,0,0,0,0,1,0,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Outras':
                lista.append([i[1], i[2], 0,0,0,0,0,0,0,0,1,0,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Pensionista':
                lista.append([i[1], i[2], 0,0,0,0,0,0,0,0,0,1,0,0])
                listaf.append([i[1], i[2], i[3]])
            elif i[3] == 'Profissional liberal':
                lista.append([i[1], i[2], 0,0,0,0,0,0,0,0,0,0,1,0])
                listaf.append([i[1], i[2], i[3]])
            else:
                lista.append([i[1], i[2], 0,0,0,0,0,0,0,0,0,0,0,1])
                listaf.append([i[1], i[2], i[3]])
            cpfs.append(i[0])
            filiais.append(i[4])
            telefone.append(i[5])
            nome.append(i[6])
        listas_unicas = {tuple(lista) for lista in lista}
        lista = [list(tupla) for tupla in listas_unicas]
        listas_unicasf = {tuple(listaf) for listaf in listaf}
        listaf = [list(tupla) for tupla in listas_unicasf]
        return lista, cpfs, filiais, telefone, nome, listaf

app = ARPC()
app.modelosPred()
app.filtragem()
app.database.login.close()