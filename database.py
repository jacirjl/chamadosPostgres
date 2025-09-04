import psycopg2
import os
import pandas as pd
from sqlalchemy import create_engine
from werkzeug.security import generate_password_hash

# --- Configurações ---
# DATABASE_URL = "postgresql://postgres:5451469@localhost:5432/chamados_db"
DATABASE_URL="postgresql://chamados_db_2toi_user:wOa88OelMosE8sE6c9Hv6fAmIOepZmek@dpg-d2sqr8h5pdvs739po9jg-a.oregon-postgres.render.com/chamados_db_2toi"
EXCEL_FILE = 'suporte.xlsx'
DEFAULT_PASSWORD = '12345'

# --- Conexão ---
try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    print("Conectado ao banco de dados PostgreSQL.")
except Exception as e:
    print(f"Não foi possível conectar ao banco de dados: {e}")
    print("Verifique se o servidor PostgreSQL está rodando e se a DATABASE_URL está correta.")
    exit()


def setup_tables():
    """Cria as tabelas com a estrutura para PostgreSQL."""

    cursor.execute('DROP TABLE IF EXISTS chamados, equipamentos, configuracoes, status, tipos_problema, users CASCADE;')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        municipio TEXT NOT NULL,
        responsavel TEXT NOT NULL,
        telefone TEXT NOT NULL,
        must_reset_password BOOLEAN DEFAULT TRUE,
        is_admin BOOLEAN DEFAULT FALSE
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS status (
        id SERIAL PRIMARY KEY,
        nome TEXT UNIQUE NOT NULL,
        e_inicial BOOLEAN DEFAULT FALSE NOT NULL,
        e_em_atendimento BOOLEAN DEFAULT FALSE NOT NULL,
        permite_reabertura BOOLEAN DEFAULT FALSE NOT NULL,
        e_final BOOLEAN DEFAULT FALSE NOT NULL
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tipos_problema (
        id SERIAL PRIMARY KEY,
        nome TEXT UNIQUE NOT NULL
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS configuracoes (
        chave TEXT PRIMARY KEY,
        valor TEXT NOT NULL
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS equipamentos (
        id SERIAL PRIMARY KEY,
        municipio TEXT NOT NULL,
        imei1 TEXT UNIQUE,
        imei2 TEXT,
        marca TEXT,
        modelo TEXT,
        capacidade TEXT,
        numerodeserie TEXT,
        dataentrega TEXT,
        localdeuso TEXT,
        situacao TEXT,
        patrimonio TEXT
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chamados (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        solicitante_email TEXT NOT NULL,
        municipio TEXT NOT NULL,
        smartphone_imei TEXT,
        tipo_problema_id INTEGER NOT NULL REFERENCES tipos_problema(id),
        observacoes TEXT NOT NULL,
        status_id INTEGER NOT NULL REFERENCES status(id),
        foto TEXT,
        solucao TEXT,
        admin_responsavel_id INTEGER REFERENCES users(id),
        resolvido_em TIMESTAMP
    );
    ''')
    print("Tabelas criadas com sucesso no PostgreSQL.")
    conn.commit()


def populate_lookup_tables():
    """Popula as tabelas com valores e comportamentos padrão."""
    default_status = [
        ('Aberto', True, False, False, False),
        ('Em Andamento', False, True, False, False),
        ('Aguardando Peça', False, False, False, False),
        ('Resolvido', False, False, True, True),
        ('Encerrado', False, False, False, True),
        ('Cancelado', False, False, False, True)
    ]
    default_problemas = [('Octostudio',), ('Sistema Operacional',), ('Hardware/Dispositivo',), ('Dúvidas/Outros',)]

    default_config = [
        ('prazo_vermelho', '10'),
        ('prazo_amarelo', '5'),
        ('prazo_reabrir', '3'),
        ('status_capturado_id', '2'),
        ('status_expirado_id', '5')
    ]

    cursor.executemany(
        "INSERT INTO status (nome, e_inicial, e_em_atendimento, permite_reabertura, e_final) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (nome) DO NOTHING",
        default_status)
    cursor.executemany("INSERT INTO tipos_problema (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING", default_problemas)
    cursor.executemany("INSERT INTO configuracoes (chave, valor) VALUES (%s, %s) ON CONFLICT (chave) DO NOTHING",
                       default_config)
    conn.commit()
    print("Tabelas de lookup populadas.")


def populate_from_excel():
    """Lê o arquivo Excel e popula as tabelas de usuários e equipamentos."""
    if not os.path.exists(EXCEL_FILE):
        print(f"AVISO: '{EXCEL_FILE}' não encontrado. Pulando a população de usuários e equipamentos.")
        return

    engine = create_engine(DATABASE_URL)

    # Lógica para Usuários
    try:
        df_users = pd.read_excel(EXCEL_FILE, sheet_name='Cadastro')
        df_users.columns = [str(col).lower().strip() for col in df_users.columns]
        hashed_password = generate_password_hash(DEFAULT_PASSWORD)

        for index, row in df_users.iterrows():
            cursor.execute("SELECT id FROM users WHERE email = %s", (row['email'],))
            if cursor.fetchone() is None:
                is_admin_flag = True if 'admin' in df_users.columns and str(
                    row.get('admin', '')).lower() == 'sim' else False
                cursor.execute(
                    "INSERT INTO users (email, password, municipio, responsavel, telefone, must_reset_password, is_admin) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (row['email'], hashed_password, row['município'], row['responsável'], str(row['telefone']), True,
                     is_admin_flag)
                )
        conn.commit()
        print("Usuários populados com sucesso.")
    except Exception as e:
        print(f"\nAVISO: Não foi possível popular usuários. Aba 'Cadastro' não encontrada ou erro: {e}")

    # Lógica para Equipamentos
    try:
        df_equip = pd.read_excel(EXCEL_FILE, sheet_name='equipamentos')
        # MUDANÇA AQUI: Normaliza os nomes das colunas, removendo acentos e espaços
        df_equip.columns = [
            str(col).lower().strip().replace(' ', '').replace('í', 'i').replace('ç', 'c').replace('ã', 'a') for col in
            df_equip.columns]

        # Garante que os nomes das colunas no dataframe correspondam exatamente aos do banco de dados
        # O ideal é que o CREATE TABLE e os nomes das colunas do Excel já estejam alinhados
        # Exemplo de renomeação explícita se necessário:
        # df_equip.rename(columns={'numerodeserie': 'numeroDeSerie'}, inplace=True)

        cursor.execute("DELETE FROM equipamentos;")  # Limpa a tabela antes de inserir para evitar duplicatas
        df_equip.to_sql('equipamentos', engine, if_exists='append', index=False)
        print("Equipamentos populados com sucesso.")
    except Exception as e:
        print(f"\nAVISO: Não foi possível popular equipamentos. Aba 'equipamentos' não encontrada ou erro: {e}")


if __name__ == '__main__':
    setup_tables()
    populate_lookup_tables()
    populate_from_excel()

    cursor.close()
    conn.close()
    print("\nProcesso de inicialização do banco de dados PostgreSQL concluído.")