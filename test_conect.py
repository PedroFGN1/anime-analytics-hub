'''
Docstring para test_conect
Script para testar conexão com banco postgreSQL.
'''
import psycopg2
from psycopg2 import sql

def test_connection(host, database, user, password):
    '''
    Testa a conexão com o banco de dados PostgreSQL.
    
    Parâmetros:
    host (str): Endereço do servidor do banco de dados.
    database (str): Nome do banco de dados.
    user (str): Nome do usuário do banco de dados.
    password (str): Senha do usuário do banco de dados.
    
    Retorna:
    bool: True se a conexão for bem-sucedida, False caso contrário.
    '''
    try:
        # Estabelece a conexão
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        # Fecha a conexão
        connection.close()
        return True
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return False
    
# Exemplo de uso
if __name__ == "__main__":
    host = "localhost"
    database = "anime_analytics"
    user = "admin"
    password = "admin"
    if test_connection(host, database, user, password):
        print("Conexão bem-sucedida!")
    else:
        print("Falha na conexão.")