import pandas as pd
import mysql.connector
import schedule
import time
from datetime import datetime

# Configurações do banco de dados
db_config = {
    'host': 'seu_host',        # Ex: 'localhost' ou '127.0.0.1'
    'user': 'seu_usuario',
    'password': 'sua_senha',
    'database': 'seu_banco_de_dados'
}

# Função para realizar a consulta e salvar os resultados
def realizar_analise_tendencias():
    try:
        # Conectar ao banco de dados
        connection = mysql.connector.connect(**db_config)
        query = """
        SELECT data, valor
        FROM sua_tabela
        WHERE data >= CURDATE() - INTERVAL 30 DAY
        ORDER BY data;
        """
        
        # Executar a consulta
        df = pd.read_sql(query, connection)
        
        # Verifique se há dados
        if not df.empty:
            # Salvar resultados em um arquivo CSV com timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            df.to_csv(f'analise_tendencias_{timestamp}.csv', index=False)
            print(f'Resultados salvos em analise_tendencias_{timestamp}.csv')
        else:
            print('Nenhum dado encontrado para o período especificado.')
    
    except mysql.connector.Error as err:
        print(f'Erro: {err}')
    finally:
        if connection.is_connected():
            connection.close()

# Agendar a execução da análise a cada dia
schedule.every().day.at("10:00").do(realizar_analise_tendencias)  # Define a hora da execução

print("Agendamento de análises de tendências configurado. Aguardando execução...")
while True:
    schedule.run_pending()
    time.sleep(1)
