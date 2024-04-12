import psycopg2

class Conectar:
    
    def __init__(self):
        self.login, self.cursor = self.conexaoDatabase()

    def conexaoDatabase(self):
        self.con = psycopg2.connect(host='X', 
                            database='X',
                            user='X', 
                            password='X')
        self.cur = self.con.cursor()
        return self.con, self.cur

    def executarQueryCliente(self, cpfs):
        query = '''select "X", 
                    case when X isnull then 1 else X end,case when date_part('year', age(X))::int isnull then 49 else date_part('year', age(X))::int end as idade, case when o."X" isnull then 'Outras' else o."X" end 
                    from X p 
                    left join X o
                    on X = X
                    where X = 1 and X in %s'''

        self.cur.execute(query, (tuple(cpfs),))
        self.login.commit()
        self.resultado = self.cursor.fetchall()
        return self.resultado
    
    def executarQueryPerfil(self, perfil):
        query = '''WITH X AS (
    SELECT
        X::varchar,
        X,
        COUNT(*) AS contagem,
        X() OVER (PARTITION BY X ORDER BY COUNT(*) DESC) AS rank
    FROM X
    GROUP BY X, X
)

select "X", 
                    case when X isnull then 1 else X end,
                    case when date_part('year', age(X))::int isnull then 49 else date_part('year', age(X))::int end as idade, 
                    case when X isnull then 'Outras' else X end,
                    X
                    from X p 
                    left join X o
                    on X= X
                    left join X fr on
                    X = X::varchar
                    where X = 1 and rank = 1 and X = %s and o."X" = %s and date_part('year', age(X))::int = %s'''
        self.cur.execute(query, (perfil))
        self.login.commit()
        self.resultado = self.cursor.fetchall()
        return self.resultado
    
    def executarQueryFilial(self, filial):
        query = '''WITH X AS (
    SELECT
        X,
        X,
        COUNT(*) AS contagem,
        RANK() OVER (PARTITION BY X ORDER BY COUNT(*) DESC) AS rank
    FROM X
    WHERE X BETWEEN CURRENT_DATE - INTERVAL '5 years' AND CURRENT_DATE
    GROUP BY X, X
)

SELECT
    X
    X
FROM X
WHERE rank = 1 and X in %s
            '''
        self.cur.execute(query, (filial,))
        self.login.commit()
        self.resultado = self.cur.fetchall()
        return self.resultado
    
    def executarQueryFiliais(self):
        query = '''select X from X'''

        lista_cpfs = []

        self.cur.execute(query)
        self.login.commit()
        self.resultado = self.cur.fetchall()
        
        for row in self.resultado:
            lista_cpfs.append(row)
        return lista_cpfs

    def executarQueryPerfile(self, cpfs):
        query = '''WITH X AS (
    SELECT
        X,
        X,
        COUNT(*) AS contagem,
        RANK() OVER (PARTITION BY X ORDER BY COUNT(*) DESC) AS rank
    FROM X
    GROUP BY X, X
)

select distinct "X",
                    case when X isnull then 1 else X end,
                    case when date_part('year', age(X))::int isnull then 49 else date_part('year', age(X))::int end as idade, 
                    case when o."X" isnull then 'Outras' else o."X" end,
                    X,
                    MAX(CONCAT(t.ddd::varchar, t.numero::varchar)),
                    p.nome
                    from X p 
                    left join sis.X o
                    on p.idX = o.idX
                    left join X fr on
                    fr.X = p.idX
                    left join X t
                    on t.idX = p.idX
                    where X = 1 and rank = 1 and p.idX in %s
                    group by 1,2,3,4,5,7'''
        self.cur.execute(query, (tuple(cpfs),))
        self.login.commit()
        self.resultado = self.cursor.fetchall()
        return self.resultado
    

    def executarQueryPerfiles(self, filial, cpfs):
        query = '''WITH X AS (
    SELECT
        X,
        X,
        COUNT(*) AS contagem,
        RANK() OVER (PARTITION BY X ORDER BY COUNT(*) DESC) AS rank
    FROM X
    where datamovimento >= (CURRENT_DATE - INTERVAL '5 years')
    GROUP BY X, X
)

select distinct "X",
                    case when X isnull then 1 else X end,
                    case when date_part('year', age(X))::int isnull then 49 else date_part('year', age(X))::int end as idade, 
                    case when o."X" isnull then 'Outras' else o."X" end,
                    X,
                    MAX(CONCAT(t.ddd::varchar, t.numero::varchar)),
                    p.X
                    from X p 
                    left join sis.X o
                    on p.idX = o.idX
                    left join X fr on
                    fr.X = p.idX
                    left join X t
                    on t.idX = p.idX
                    where X in %s and X = 1 and rank = 1 and p.idX in %s
                    group by 1,2,3,4,5,7'''
        self.cur.execute(query, (filial, tuple(cpfs),))
        self.login.commit()
        self.resultado = self.cursor.fetchall()
        return self.resultado
    
    def queryProdutos(self, codigos):
        query = '''
select distinct
p.X as produtos
from X ib
left join X p 
       on
	p.X = ib.X
left join X dpto1 
       on
	dpto1.X = p.X
left join X dpto2 
       on
	dpto2.X = dpto1.X
left join X dpto3 
       on
	dpto3.X = dpto2.X
left join X dpto4 
       on
	dpto4.X = dpto3.X
left join X dpto5 
       on
	dpto5.X = dpto4.X
left join X o d on
	d.X = dpto5.X
left join X 
	   on
	(pl.id = X)
left join X m 
       on
	(m.idmarca = p.idmarca)
where ib.X in %s
'''
        lista_produtos = []
        self.cur.execute(query, (tuple(codigos),))
        self.login.commit()
        self.resultado = self.cursor.fetchall()
        for i in range(len(self.resultado)):
            lista_produtos.append(self.resultado[i][0])
        return lista_produtos
    
a = Conectar()

a.con.close()