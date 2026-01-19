"""
Flexible Consolidation Processor - Processador flexível de consolidação.

Implementa consolidação totalmente dirigida por configurações do settings.py,
permitindo múltiplas regras de consolidação sem modificar código.

Suporta:
- Múltiplas tabelas principais (SoR/SoT)
- Joins dinâmicos entre auxiliares
- Ranking baseado em campos de decisão
- Casos com/sem auxiliares
- Casos com uma ou múltiplas origens
"""
import logging
from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.business.base_processor import BaseBusinessProcessor

logger = logging.getLogger(__name__)


class FlexibleConsolidationProcessor(BaseBusinessProcessor):
    """
    Processador flexível de consolidação dirigido por configurações.
    
    Lê configurações do settings.py via config.CONSOLIDACOES e processa
    consolidações dinamicamente sem necessidade de modificar código.
    
    Configurações esperadas em config.CONSOLIDACOES[tabela_consolidada]:
    - principais: dict com 'sor' e/ou 'sot' (nomes das tabelas principais)
    - auxiliares: dict opcional com auxiliares por origem
    - joins_auxiliares: dict com especificações de joins por origem
    - chaves_principais: lista de chaves para agrupamento
    - campos_decisao: lista de campos para ordenação no ranking
    """
    
    PARTITION_KEY = "anomesdia"  # Chave de partição padrão
    
    def __init__(
        self,
        glue_handler,
        journey_controller,
        dynamodb_handler,
        config
    ):
        super().__init__(glue_handler, journey_controller, dynamodb_handler, config)
        
        # Carregar configurações de consolidação do settings.py
        self.consolidacoes_config = getattr(config, 'consolidacoes_tabelas', {})
        if not self.consolidacoes_config:
            logger.warning(
                "Configurações das Tabelas não encontrado em config. "
                "Consolidações precisarão ser passadas via kwargs."
            )
    
    # -------------------------------------------------------------------------
    # READ - Hook Method (BaseBusinessProcessor)
    # -------------------------------------------------------------------------
    
    def _read_data(self, **kwargs) -> DataFrame:
        """
        Lê dados das origens (SoR/SoT) e unifica em um único DataFrame.
        
        Args:
            **kwargs: Deve conter:
                - database: str - Nome do banco de dados
                - tabela_consolidada: str - Chave em CONSOLIDACOES do settings.py
                - consolidation_config: dict (opcional) - Config override
        
        Returns:
            DataFrame unificado com coluna 'origem' marcada
        """
        database = kwargs.get('database')
        tabela_consolidada = kwargs.get('tabela_consolidada')
        consolidation_config = kwargs.get('consolidation_config')
        
        if not database:
            raise ValueError("Parâmetro 'database' é obrigatório")
        
        if not tabela_consolidada and not consolidation_config:
            raise ValueError(
                "É necessário fornecer 'tabela_consolidada' ou 'consolidation_config'"
            )
        
        # Obter configuração da consolidação e armazenar para uso posterior
        if consolidation_config:
            regra_cfg = consolidation_config
        else:
            if tabela_consolidada not in self.consolidacoes_config:
                raise ValueError(
                    f"Tabela consolidada '{tabela_consolidada}' não encontrada em CONSOLIDACOES. "
                    f"Disponíveis: {list(self.consolidacoes_config.keys())}"
                )
            regra_cfg = self.consolidacoes_config[tabela_consolidada]
        
        # Armazenar configuração para uso em _transform_data
        self._current_config = regra_cfg
        self._current_tabela_consolidada = tabela_consolidada
        
        principais = regra_cfg.get('principais', {})
        auxiliares = regra_cfg.get('auxiliares', {})
        joins_auxiliares = regra_cfg.get('joins_auxiliares', {})
        
        if not principais:
            raise ValueError("Configuração deve conter pelo menos uma tabela em 'principais'")
        
        # Ler dados para cada origem configurada e unificar
        dfs_marcados = []
        self._dataframes_originais = {}  # Armazenar originais para join posterior
        
        for origem in principais.keys():  # 'sor', 'sot', etc.
            origem_cfg = principais[origem]
            
            # Suportar nova estrutura (dict com database/table) e estrutura antiga (string)
            if isinstance(origem_cfg, dict):
                database_origem = origem_cfg.get('database', database)
                tabela_principal = origem_cfg.get('table')
            else:
                # Estrutura antiga: apenas nome da tabela
                database_origem = database
                tabela_principal = origem_cfg
            
            if not tabela_principal:
                raise ValueError(f"Configuração de origem '{origem}' deve conter 'table' ou ser uma string")
            
            logger.info(f"[{tabela_consolidada}] Lendo origem '{origem}': {database_origem}.{tabela_principal}")
            
            # Preparar lista de colunas necessárias (chaves_principais + campos_decisao)
            # Isso evita colunas duplicadas no union posterior, como no SQL original
            chaves_principais = regra_cfg.get('chaves_principais', [])
            campos_decisao = regra_cfg.get('campos_decisao', [])
            colunas_necessarias = list(set(chaves_principais + campos_decisao))  # Remove duplicatas
            
            # Ler dados com auxiliares (se houver) ou tabela principal (se não houver auxiliares)
            # IMPORTANTE: No SQL original, se há auxiliares, os joins iniciam APENAS com auxiliares
            # A tabela principal só é usada para obter a última partição e no join final
            df = self._read_origem_com_auxiliares(
                database=database_origem,
                tabela_principal=tabela_principal,
                auxiliares=auxiliares.get(origem, {}),
                joins_auxiliares=joins_auxiliares.get(origem, []),
                colunas_necessarias=colunas_necessarias if colunas_necessarias else None
            )
            
            # Armazenar referência da tabela principal para uso no join final
            # (não armazenamos o DataFrame completo aqui para economizar memória)
            if not hasattr(self, '_dataframes_originais'):
                self._dataframes_originais = {}
            # Armazenar apenas metadados da tabela principal, não o DataFrame completo
            # O DataFrame completo será lido no join final
            self._dataframes_originais[origem] = {
                'database': database_origem,
                'table': tabela_principal
            }
            
            # Marcar origem
            origem_label = 'online' if origem == 'sor' else 'batch'
            df_marcado = df.withColumn('origem', F.lit(origem_label))
            dfs_marcados.append(df_marcado)
        
        # Unificar todos os DataFrames
        if len(dfs_marcados) == 1:
            return dfs_marcados[0]
        
        # Unificar DataFrames - o Spark remove automaticamente colunas duplicadas de chaves de join
        # Se houver colunas duplicadas por nome (não de join), será detectado no unionByName
        df_unificado = dfs_marcados[0]
        for df in dfs_marcados[1:]:
            df_unificado = df_unificado.unionByName(df, allowMissingColumns=True)
        
        return df_unificado
    
    def _read_origem_com_auxiliares(
        self,
        database: str,
        tabela_principal: str,
        auxiliares: Dict[str, str],
        joins_auxiliares: List[Dict],
        colunas_necessarias: List[str] = None
    ) -> DataFrame:
        """
        Lê tabela principal e aplica joins com auxiliares dinamicamente.
        
        Args:
            database: Nome do banco de dados
            tabela_principal: Nome da tabela principal
            auxiliares: Dict alias -> nome_tabela (ex: {'oper': 'tbl_operacao_sor'})
            joins_auxiliares: Lista de especificações de joins
            colunas_necessarias: Lista opcional de colunas a selecionar após joins
                                (se None, retorna todas as colunas)
        
        Returns:
            DataFrame com joins aplicados (e colunas selecionadas se especificado)
        """
        # Obter última partição da tabela principal
        try:
            particao = self.glue_handler.get_last_partition(
                database=database,
                table_name=tabela_principal,
                partition_key=self.PARTITION_KEY,
                region_name=getattr(self.config, 'aws_region', None)
            )
            if particao:
                filtro = f"{self.PARTITION_KEY} = '{particao}'"
                logger.info(f"Filtrando {tabela_principal} por partição: {particao}")
            else:
                filtro = None
                logger.warning(f"Nenhuma partição encontrada para {tabela_principal}. Lendo todas.")
        except Exception as e:
            logger.warning(f"Erro ao obter última partição de {tabela_principal}: {e}. Lendo todas.")
            filtro = None
        
        # Ler tabela principal
        df_principal = self.glue_handler.read_from_catalog(
            database=database,
            table_name=tabela_principal,
            filter=filtro
        )
        
        # Se não há auxiliares, retornar direto
        if not auxiliares or not joins_auxiliares:
            return df_principal
        
        # Ler auxiliares
        dfs_aux = {}
        for alias, tabela_aux in auxiliares.items():
            logger.info(f"Lendo auxiliar '{alias}': {tabela_aux}")
            dfs_aux[alias] = self.glue_handler.read_from_catalog(
                database=database,
                table_name=tabela_aux
            )
        
        # Aplicar joins na ordem especificada
        # IMPORTANTE: No SQL original, os joins iniciam APENAS com auxiliares, não com a tabela principal
        # A tabela principal só é usada para obter a última partição e no join final
        # O primeiro join deve começar com o primeiro auxiliar especificado em 'left'
        df_resultado = None
        
        for join_spec in joins_auxiliares:
            left_alias = join_spec.get('left')
            right_alias = join_spec.get('right')
            join_on = join_spec.get('on', [])
            how = join_spec.get('how', 'inner')
            
            # Validar aliases
            if right_alias not in dfs_aux:
                logger.warning(
                    f"Alias '{right_alias}' não encontrado nos auxiliares "
                    f"(disponíveis: {list(dfs_aux.keys())}). Pulando join."
                )
                continue
            
            # Preparar DataFrame da esquerda
            # IMPORTANTE: No SQL original, os joins iniciam APENAS com auxiliares
            # Mas suportamos também o caso especial onde 'principal' é usado (para compatibilidade)
            if df_resultado is None:
                # Primeiro join
                if left_alias == 'principal':
                    # Caso especial: começar com tabela principal (não é o padrão do SQL original)
                    df_left = df_principal
                    logger.info("Iniciando joins com tabela principal (caso especial)")
                elif left_alias in dfs_aux:
                    # Caso normal: começar com auxiliar (conforme SQL original)
                    df_left = dfs_aux[left_alias]
                    logger.info(f"Iniciando joins com auxiliar '{left_alias}' (conforme SQL original)")
                else:
                    raise ValueError(
                        f"Primeiro join deve começar com um auxiliar ou 'principal', mas '{left_alias}' não está disponível. "
                        f"Auxiliares disponíveis: {list(dfs_aux.keys())}"
                    )
            elif left_alias == 'principal':
                # Se especificado 'principal' em join subsequente, usar df_resultado acumulado
                df_left = df_resultado
            elif left_alias in dfs_aux:
                # Se left_alias está em auxiliares, usar esse auxiliar
                df_left = dfs_aux[left_alias]
            else:
                # Caso contrário, usar resultado acumulado
                df_left = df_resultado
            
            df_right = dfs_aux[right_alias]
            
            # Construir condições de join usando nomes de colunas diretamente
            join_columns = []
            for left_col, right_col in join_on:
                # Verificar se as colunas existem nos DataFrames
                if left_col not in df_left.columns:
                    raise ValueError(
                        f"Coluna '{left_col}' não encontrada no DataFrame da esquerda. "
                        f"Colunas disponíveis: {df_left.columns}"
                    )
                if right_col not in df_right.columns:
                    raise ValueError(
                        f"Coluna '{right_col}' não encontrada no DataFrame da direita. "
                        f"Colunas disponíveis: {df_right.columns}"
                    )
                
                # Se os nomes são diferentes, renomear right para left
                if left_col != right_col:
                    df_right = df_right.withColumnRenamed(right_col, left_col)
                
                join_columns.append(left_col)
            
            # Antes do join, remover do df_right colunas que já existem em df_left
            # (exceto as chaves de join, que serão removidas automaticamente pelo Spark)
            colunas_left = set(df_left.columns)
            colunas_right = set(df_right.columns)
            colunas_join = set(join_columns)
            
            # Colunas a manter do df_right: apenas as que não estão em df_left (exceto chaves de join)
            colunas_para_manter_right = [col for col in df_right.columns 
                                        if col not in colunas_left or col in colunas_join]
            
            # Se há colunas duplicadas (não de join), selecionar apenas as necessárias
            if len(colunas_para_manter_right) < len(df_right.columns):
                df_right = df_right.select(*colunas_para_manter_right)
                logger.debug(f"Removendo {len(df_right.columns) - len(colunas_para_manter_right)} colunas duplicadas do DataFrame da direita antes do join")
            
            logger.info(
                f"Aplicando join {how}: {left_alias} -> {right_alias} "
                f"on {join_columns}"
            )
            
            # Aplicar join usando lista de colunas (Spark trata automaticamente)
            # Spark remove colunas duplicadas das chaves de join automaticamente
            df_resultado = df_left.join(df_right, on=join_columns, how=how)
        
        # Após todos os joins, selecionar apenas colunas necessárias (como no SQL)
        # Isso evita colunas duplicadas e ambiguidade no unionByName posterior
        if colunas_necessarias:
            # Verificar quais colunas realmente existem no DataFrame
            colunas_existentes = [col for col in colunas_necessarias if col in df_resultado.columns]
            if colunas_existentes:
                df_resultado = df_resultado.select(*colunas_existentes)
                logger.info(f"Selecionadas {len(colunas_existentes)} colunas após joins: {colunas_existentes}")
            else:
                logger.warning(f"Nenhuma das colunas necessárias encontrada: {colunas_necessarias}")
        
        return df_resultado
    
    # -------------------------------------------------------------------------
    # TRANSFORM - Hook Method (BaseBusinessProcessor)
    # -------------------------------------------------------------------------
    
    def _transform_data(self, df: DataFrame, **kwargs) -> Dict:
        """
        Transforma dados aplicando ranking e consolidação conforme configuração.
        
        Args:
            df: DataFrame unificado com coluna 'origem' já marcada
            **kwargs: Parâmetros adicionais
        
        Returns:
            Dict com DataFrame consolidado e metadados
        """
        # Obter configuração armazenada em _read_data
        regra_cfg = getattr(self, '_current_config', None)
        tabela_consolidada = getattr(self, '_current_tabela_consolidada', None)
        
        if not regra_cfg:
            # Tentar obter do kwargs
            consolidation_config = kwargs.get('consolidation_config')
            if consolidation_config:
                regra_cfg = consolidation_config
            else:
                tabela_consolidada = kwargs.get('tabela_consolidada')
                if tabela_consolidada and tabela_consolidada in self.consolidacoes_config:
                    regra_cfg = self.consolidacoes_config[tabela_consolidada]
        
        if not regra_cfg:
            raise ValueError(
                "Configuração de consolidação não encontrada. "
                "Certifique-se de fornecer 'tabela_consolidada' ou 'consolidation_config'."
            )
        
        chaves_principais = regra_cfg.get('chaves_principais', [])
        campos_decisao = regra_cfg.get('campos_decisao', [])
        
        # df já vem unificado com origem marcada de _read_data
        df_unificado = df
        
        # Se não há campos de decisão, retornar união sem ranking
        if not campos_decisao or not chaves_principais:
            logger.info("Sem campos de decisão ou chaves principais. Retornando união simples.")
            return {
                'df_consolidado': df_unificado,
                'metadata': {
                    'record_count': df_unificado.count(),
                    'ranking_applied': False,
                    'reason': 'No decision fields or primary keys'
                }
            }
        
        # Aplicar ranking baseado em campos_decisao
        logger.info(
            f"Aplicando ranking por: {campos_decisao} "
            f"agrupado por: {chaves_principais}"
        )
        
        # Construir window specification
        # IMPORTANTE: No SQL original, o ORDER BY do ranking NÃO inclui preferência por origem
        # A preferência por origem só aparece no join final (linha 121: and rank_oper.origem = 'online')
        partition_cols = [F.col(c) for c in chaves_principais]
        order_cols = [F.col(c).desc_nulls_last() for c in campos_decisao]
        
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        
        # Aplicar ranking e selecionar rank=1 (vencedor)
        df_ranked = (
            df_unificado
            .withColumn('_rank', F.row_number().over(window_spec))
            .filter(F.col('_rank') == 1)
            .drop('_rank')
        )
        
        # Se há múltiplas origens, fazer join final com tabelas principais para obter registros completos
        dataframes_originais = getattr(self, '_dataframes_originais', {})
        if len(dataframes_originais) > 1 and tabela_consolidada:
            df_ranked = self._join_com_registros_completos(
                df_ranked=df_ranked,
                data_originais=dataframes_originais,
                regra_cfg=regra_cfg,
                database=kwargs.get('database'),
                chaves_principais=chaves_principais
            )
        
        # Converter DataFrame para Dict para compatibilidade com BaseBusinessProcessor
        # O DataFrame será usado em _write_output se necessário
        record_count = df_ranked.count()
        sample_data = df_ranked.limit(1000).toPandas().to_dict('records') if record_count > 0 else []
        
        return {
            'df_consolidado': df_ranked,  # DataFrame para uso em _write_output
            'record_count': record_count,
            'tabela_consolidada': tabela_consolidada,
            'chaves_principais': chaves_principais,
            'campos_decisao': campos_decisao,
            'sample_data': sample_data  # Amostra para congregado
        }
    
    def _join_com_registros_completos(
        self,
        df_ranked: DataFrame,
        data_originais: Dict[str, Dict],
        regra_cfg: Dict,
        database: str,
        chaves_principais: List[str]
    ) -> DataFrame:
        """
        Faz join dos vencedores com registros completos das tabelas principais.
        
        Isso garante que o resultado final contenha todas as colunas originais,
        não apenas as colunas usadas no ranking.
        
        IMPORTANTE: No SQL original (linhas 113-133), o join final é feito com a tabela principal
        completa, usando apenas as chaves principais e filtrando por origem e rank=1.
        """
        principais = regra_cfg.get('principais', {})
        dfs_completos = []
        
        for origem in principais.keys():
            if origem not in data_originais:
                continue
            
            # Filtrar vencedores por origem (equivalente a: and rank_oper.origem = 'online'/'batch')
            origem_label = 'online' if origem == 'sor' else 'batch'
            vencedores_origem = df_ranked.filter(F.col('origem') == F.lit(origem_label))
            
            # Obter metadados da tabela principal armazenados
            metadata = data_originais[origem]
            if isinstance(metadata, dict) and 'database' in metadata and 'table' in metadata:
                # Nova estrutura: metadados armazenados
                database_origem = metadata['database']
                tabela_principal = metadata['table']
            else:
                # Fallback: obter da configuração
                origem_cfg = principais[origem]
                if isinstance(origem_cfg, dict):
                    database_origem = origem_cfg.get('database', database)
                    tabela_principal = origem_cfg.get('table')
                else:
                    database_origem = database
                    tabela_principal = origem_cfg
            
            if not tabela_principal:
                continue
            
            # Ler tabela principal completa (para obter todas as colunas)
            try:
                particao = self.glue_handler.get_last_partition(
                    database=database_origem,
                    table_name=tabela_principal,
                    partition_key=self.PARTITION_KEY,
                    region_name=getattr(self.config, 'aws_region', None)
                )
                filtro = f"{self.PARTITION_KEY} = '{particao}'" if particao else None
            except:
                filtro = None
            
            df_completo = self.glue_handler.read_from_catalog(
                database=database_origem,
                table_name=tabela_principal,
                filter=filtro
            )
            
            # Join vencedores com registros completos
            # IMPORTANTE: No SQL original, o join final traz todas as colunas da tabela principal (sor.* ou sot.*)
            # Mas o df_ranked já contém algumas colunas dos auxiliares. Precisamos garantir que não há duplicatas
            # Selecionar apenas colunas da tabela principal que não estão em vencedores_origem (exceto chaves de join)
            colunas_vencedores = set(vencedores_origem.columns)
            colunas_completo = set(df_completo.columns)
            colunas_join = set(chaves_principais)
            
            # Colunas a selecionar do df_completo: todas, mas se houver duplicatas (não de join), manter apenas uma
            # O Spark remove automaticamente colunas duplicadas de chaves de join, mas não de outras colunas
            colunas_para_join = [col for col in df_completo.columns 
                               if col not in colunas_vencedores or col in colunas_join]
            
            # Se há colunas duplicadas (não de join), selecionar apenas as necessárias
            if len(colunas_para_join) < len(df_completo.columns):
                df_completo = df_completo.select(*colunas_para_join)
            
            df_completo_join = vencedores_origem.join(
                df_completo,
                on=chaves_principais,
                how='inner'
            )
            
            dfs_completos.append(df_completo_join)
        
        # Unir resultados
        if not dfs_completos:
            # Se não há resultados, retornar df_ranked original
            return df_ranked
        
        if len(dfs_completos) == 1:
            return dfs_completos[0]
        
        # Unir resultados diretamente - o Spark lida com colunas duplicadas de join automaticamente
        # Se houver colunas duplicadas por nome (não de join), o unionByName falhará
        # Mas isso não deve acontecer se os joins foram feitos corretamente
        resultado = dfs_completos[0]
        for df in dfs_completos[1:]:
            resultado = resultado.unionByName(df, allowMissingColumns=True)
        
        return resultado
    
    # -------------------------------------------------------------------------
    # Congregado Methods (BaseBusinessProcessor)
    # -------------------------------------------------------------------------
    
    def _get_congregado_key(self, **kwargs) -> str:
        """Gera chave primária para congregado."""
        tabela_consolidada = kwargs.get('tabela_consolidada', 'unknown')
        database = kwargs.get('database', 'default')
        return f"{database}_{tabela_consolidada}"
    
    def _get_congregado_metadata(self, **kwargs) -> Dict:
        """Gera metadados para congregado."""
        return {
            'processor_type': self.get_processor_name(),
            'tabela_consolidada': kwargs.get('tabela_consolidada'),
            'database': kwargs.get('database'),
            'origens': list(kwargs.get('data', {}).keys()) if 'data' in kwargs else []
        }
    
    def _should_write_output(self, **kwargs) -> bool:
        """Determina se deve escrever output."""
        return kwargs.get('output_path') is not None
    
    def _write_output(self, df: DataFrame, transformed_data: Dict, output_path: str, **kwargs):
        """
        Escreve resultado no catálogo ou S3.
        
        Se tabela_consolidada for fornecida, escreve no catálogo.
        Caso contrário, escreve no S3.
        """
        tabela_consolidada = kwargs.get('tabela_consolidada')
        database = kwargs.get('database')
        
        # Obter DataFrame consolidado do transformed_data
        df_consolidado = transformed_data.get('df_consolidado')
        if df_consolidado is None:
            logger.warning("DataFrame consolidado não encontrado em transformed_data. Usando df original.")
            df_consolidado = df
        
        if tabela_consolidada and database:
            # Escrever no catálogo Glue
            logger.info(f"Escrevendo no catálogo: {database}.{tabela_consolidada}")
            self.glue_handler.write_to_catalog(
                df=df_consolidado,
                database=database,
                table_name=tabela_consolidada
            )
        else:
            # Escrever no S3
            logger.info(f"Escrevendo no S3: {output_path}")
            self.glue_handler.write_to_s3(
                df=df_consolidado,
                path=output_path,
                format=self.config.default_output_format
            )

    def get_processor_name(self) -> str:
        """Retorna o nome do processador."""
        return self.__class__.__name__
