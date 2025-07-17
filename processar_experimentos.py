import pandas as pd
import boto3
from datetime import datetime
import os

NOME_DO_BUCKET = "experimento-lucas-barbosa"  # Troque pelo nome do seu bucket
PASTA_RAW = "raw/"             # Onde estão os arquivos novos
PASTA_PROCESSED = "processed/" # Onde vamos salvar as métricas
PASTA_ARCHIVE = "archive/"     # Onde vamos mover os arquivos já processados

# Conectando ao S3
try:
    s3 = boto3.client('s3')
    print("   ✓ Conectado com sucesso!")
except Exception as erro:
    print(f"   ✗ Erro ao conectar: {erro}")
    exit(1)

def buscar_arquivos_novos():
    """
    Esta função busca arquivos na pasta 'raw' que ainda não foram processados
    """
    arquivos_novos = []
    
    try:
        # Listar todos os arquivos na pasta raw
        resposta = s3.list_objects_v2(
            Bucket=NOME_DO_BUCKET,
            Prefix=PASTA_RAW
        )
        
        # Se não tem nenhum arquivo
        if 'Contents' not in resposta:
            print("   → Nenhum arquivo encontrado na pasta raw/")
            return []
        
        # Para cada arquivo encontrado
        for item in resposta['Contents']:
            nome_arquivo = item['Key']
            
            # Pular se for a própria pasta
            if nome_arquivo == PASTA_RAW:
                continue
                
            # Verificar se é um arquivo CSV
            if nome_arquivo.endswith('.csv'):
                # Extrair apenas o nome do arquivo (sem o caminho)
                nome_simples = nome_arquivo.split('/')[-1]
                
                # Verificar se já foi processado
                # (checando se existe um arquivo de métricas correspondente)
                nome_metricas = f"{PASTA_PROCESSED}metricas_{nome_simples}"
                
                try:
                    # Tentar ver se o arquivo de métricas existe
                    s3.head_object(Bucket=NOME_DO_BUCKET, Key=nome_metricas)
                    print(f"   → {nome_simples} já foi processado (pulando)")
                except:
                    # Se não existe, é um arquivo novo!
                    print(f"   → {nome_simples} é NOVO! Será processado")
                    arquivos_novos.append(nome_arquivo)
        
        return arquivos_novos
        
    except Exception as erro:
        print(f"   ✗ Erro ao buscar arquivos: {erro}")
        return []

# Buscar arquivos
arquivos_para_processar = buscar_arquivos_novos()

if not arquivos_para_processar:
    print("\n✓ Nenhum arquivo novo para processar!")
    exit(0)

print(f"\n✓ Encontrados {len(arquivos_para_processar)} arquivos novos para processar")

# ============================================
# PASSO 3: PROCESSAR CADA ARQUIVO
# ============================================

def baixar_arquivo_do_s3(caminho_s3):
    """
    Baixa um arquivo do S3 e retorna como DataFrame (tabela)
    """
    print(f"\n   Baixando arquivo: {caminho_s3}")
    
    try:
        # Baixar o arquivo
        resposta = s3.get_object(
            Bucket=NOME_DO_BUCKET,
            Key=caminho_s3
        )
        
        # Converter para DataFrame (tabela)
        df = pd.read_csv(resposta['Body'])
        print(f"   ✓ Arquivo baixado: {len(df)} linhas")
        
        return df
        
    except Exception as erro:
        print(f"   ✗ Erro ao baixar: {erro}")
        return None

def validar_arquivo(df):
    """
    Verifica se o arquivo tem as colunas necessárias
    """
    print("\n   Validando arquivo...")
    
    # Colunas obrigatórias
    colunas_obrigatorias = ['id_experimento', 'id_customer', 'grupo', 'uso', 'valor', 'timestamp']
    
    # Verificar cada coluna
    colunas_faltando = []
    for coluna in colunas_obrigatorias:
        if coluna not in df.columns:
            colunas_faltando.append(coluna)
    
    if colunas_faltando:
        print(f"   ✗ Faltam colunas: {colunas_faltando}")
        return False
    
    print("   ✓ Todas as colunas presentes")
    
    # Verificar se tem grupo controle
    if 'GC' not in df['grupo'].values:
        print("   ✗ Não tem grupo controle (GC)")
        return False
    
    print("   ✓ Grupo controle encontrado")
    
    # Verificar se números são válidos
    if df['uso'].min() < 0 or df['valor'].min() < 0:
        print("   ✗ Valores negativos encontrados")
        return False
    
    print("   ✓ Arquivo válido!")
    return True

def calcular_metricas(df):
    """
    Calcula as métricas do experimento
    """
    print("\n   Calculando métricas...")
    
    # Pegar o ID do experimento
    id_experimento = df['id_experimento'].iloc[0]
    
    # Lista para guardar as métricas
    todas_metricas = []
    
    # Para cada grupo (GC, TESTE_A, etc)
    grupos = df['grupo'].unique()
    print(f"   Grupos encontrados: {list(grupos)}")
    
    for grupo in grupos:
        # Filtrar dados do grupo
        dados_grupo = df[df['grupo'] == grupo]
        total_clientes = len(dados_grupo)
        
        # ===== MÉTRICA 1: TAXA DE CONVERSÃO =====
        # Quantos % dos clientes usaram o produto
        clientes_que_usaram = len(dados_grupo[dados_grupo['uso'] > 0])
        taxa_conversao = (clientes_que_usaram / total_clientes * 100) if total_clientes > 0 else 0
        
        todas_metricas.append({
            'id_experimento': id_experimento,
            'grupo': grupo,
            'metrica': 'taxa_conversao',
            'valor': round(taxa_conversao, 2),
            'descricao': f'{clientes_que_usaram} de {total_clientes} clientes converteram'
        })
        
        # ===== MÉTRICA 2: USO MÉDIO =====
        # Quantas vezes em média cada cliente usou
        uso_medio = dados_grupo['uso'].mean()
        
        todas_metricas.append({
            'id_experimento': id_experimento,
            'grupo': grupo,
            'metrica': 'uso_medio',
            'valor': round(uso_medio, 2),
            'descricao': f'Média de {uso_medio:.2f} usos por cliente'
        })
        
        # ===== MÉTRICA 3: VALOR MÉDIO =====
        # Quanto em média cada cliente gastou
        valor_medio = dados_grupo['valor'].mean()
        
        todas_metricas.append({
            'id_experimento': id_experimento,
            'grupo': grupo,
            'metrica': 'valor_medio',
            'valor': round(valor_medio, 2),
            'descricao': f'Média de R$ {valor_medio:.2f} por cliente'
        })
        
        # ===== MÉTRICA 4: TICKET MÉDIO =====
        # Quanto gastou em média quem gastou algo
        clientes_que_gastaram = dados_grupo[dados_grupo['valor'] > 0]
        if len(clientes_que_gastaram) > 0:
            ticket_medio = clientes_que_gastaram['valor'].mean()
        else:
            ticket_medio = 0
            
        todas_metricas.append({
            'id_experimento': id_experimento,
            'grupo': grupo,
            'metrica': 'ticket_medio',
            'valor': round(ticket_medio, 2),
            'descricao': f'Ticket médio de R$ {ticket_medio:.2f} (apenas quem gastou)'
        })
        
        print(f"   ✓ Métricas calculadas para grupo {grupo}")
    
    # ===== CALCULAR LIFT (MELHORIA VS CONTROLE) =====
    print("\n   Calculando lift vs grupo controle...")
    
    # Criar DataFrame com as métricas
    df_metricas = pd.DataFrame(todas_metricas)
    
    # Pegar métricas do grupo controle
    metricas_gc = df_metricas[df_metricas['grupo'] == 'GC']
    
    # Para cada métrica do GC
    for _, metrica_gc in metricas_gc.iterrows():
        tipo_metrica = metrica_gc['metrica']
        valor_gc = metrica_gc['valor']
        
        # Calcular lift para cada grupo teste
        for grupo in grupos:
            if grupo != 'GC':
                # Pegar valor da métrica para este grupo
                valor_grupo = df_metricas[
                    (df_metricas['grupo'] == grupo) & 
                    (df_metricas['metrica'] == tipo_metrica)
                ]['valor'].iloc[0]
                
                # Calcular lift
                if valor_gc > 0:
                    lift = ((valor_grupo - valor_gc) / valor_gc) * 100
                else:
                    lift = 0
                
                # Adicionar métrica de lift
                todas_metricas.append({
                    'id_experimento': id_experimento,
                    'grupo': grupo,
                    'metrica': f'lift_{tipo_metrica}',
                    'valor': round(lift, 2),
                    'descricao': f'Lift de {lift:.2f}% vs GC'
                })
    
    print(f"   ✓ Total de métricas calculadas: {len(todas_metricas)}")
    
    # Retornar DataFrame final
    return pd.DataFrame(todas_metricas)
def salvar_metricas(df_metricas, nome_arquivo_original):
    """
    Salva as métricas calculadas no S3
    """
    print("\n   Salvando métricas...")
    
    # Criar nome do arquivo de métricas
    nome_simples = nome_arquivo_original.split('/')[-1]
    nome_metricas = f"{PASTA_PROCESSED}metricas_{nome_simples}"
    
    try:
        # Salvar em arquivo temporário
        arquivo_temp = f"/tmp/metricas_{nome_simples}"
        df_metricas.to_csv(arquivo_temp, index=False)
        
        # Upload para S3
        s3.upload_file(arquivo_temp, NOME_DO_BUCKET, nome_metricas)
        
        print(f"   ✓ Métricas salvas em: {nome_metricas}")
        
        # Remover arquivo temporário
        os.remove(arquivo_temp)
        
        return True
        
    except Exception as erro:
        print(f"   ✗ Erro ao salvar: {erro}")
        return False

def arquivar_arquivo_original(nome_arquivo_original):
    """
    Move o arquivo original para a pasta archive
    """
    print("\n   Arquivando arquivo original...")
    
    # Criar novo nome com timestamp
    nome_simples = nome_arquivo_original.split('/')[-1]
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nome_arquivo_novo = f"{PASTA_ARCHIVE}{nome_simples.replace('.csv', '')}_processado_{timestamp}.csv"
    
    try:
        # Copiar para archive
        s3.copy_object(
            Bucket=NOME_DO_BUCKET,
            CopySource={'Bucket': NOME_DO_BUCKET, 'Key': nome_arquivo_original},
            Key=nome_arquivo_novo
        )
        
        # Deletar original
        s3.delete_object(
            Bucket=NOME_DO_BUCKET,
            Key=nome_arquivo_original
        )
        
        print(f"   ✓ Arquivo movido para: {nome_arquivo_novo}")
        
    except Exception as erro:
        print(f"   ✗ Erro ao arquivar: {erro}")

for i, arquivo in enumerate(arquivos_para_processar, 1):
    print(f"\n{'='*50}")
    print(f"PROCESSANDO ARQUIVO {i}/{len(arquivos_para_processar)}: {arquivo}")
    print(f"{'='*50}")
    
    # 1. Baixar arquivo
    df = baixar_arquivo_do_s3(arquivo)
    if df is None:
        print("   ⚠ Pulando devido a erro no download")
        continue
    
    # 2. Validar
    if not validar_arquivo(df):
        print("   ⚠ Pulando devido a erro na validação")
        continue
    
    # 3. Calcular métricas
    df_metricas = calcular_metricas(df)
    
    # 4. Salvar métricas
    if salvar_metricas(df_metricas, arquivo):
        # 5. Arquivar original (só se salvou com sucesso)
        arquivar_arquivo_original(arquivo)
        print("\n   ✅ ARQUIVO PROCESSADO COM SUCESSO!")
    else:
        print("\n   ❌ ERRO NO PROCESSAMENTO")

print("\n" + "="*50)
print("PROCESSAMENTO FINALIZADO!")
print("="*50)