import csv
import psycopg2
from config.db_vr import get_db_vr

def inserir_fichas():
    conn_vr = get_db_vr()
    with conn_vr.cursor() as cur:
        with open('fichas.csv', mode='r') as file:
            reader = csv.reader(file, delimiter=';')  # Defina o delimitador como ponto e vírgula
            for row in reader:
                try:
                    ficha = int(row[0])  # Convertendo para inteiro
                    identificacao = row[1]  # Deixando como string (varchar)
                    cur.execute("SELECT COUNT(1) FROM pdv.dadosficha WHERE ficha = %s", (ficha,))
                    result = cur.fetchone()
                    if result[0] == 0:
                        cur.execute(
                            "INSERT INTO pdv.dadosficha (ficha, identificacao) VALUES (%s, %s)",
                            (ficha, identificacao)
                        )
                        print(f'Inserido: Ficha={ficha}, Identificacao={identificacao}')
                    else:
                        print(f'Ficha {ficha} já existe. Não inserido.')
                except ValueError:
                    print(f'Erro ao processar a linha: {row}. "ficha" precisa ser um número inteiro.')
        conn_vr.commit()
    conn_vr.close()
inserir_fichas()
