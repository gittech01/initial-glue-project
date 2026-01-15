"""
Processador de Inventário - Exemplo de Nova Regra de Negócio.

Demonstra como criar uma nova regra de negócio seguindo todos os padrões:
- Template Method (herda BaseBusinessProcessor)
- Strategy (implementa hooks)
- Dependency Injection (recebe dependências)
"""
import logging
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.business.base_processor import BaseBusinessProcessor


logger = logging.getLogger(__name__)


class InventoryProcessor(BaseBusinessProcessor):
    """
    Processador de inventário - Exemplo completo de nova regra de negócio.
    
    Esta classe demonstra:
    - Como herdar de BaseBusinessProcessor
    - Como implementar os hooks necessários
    - Como adicionar lógica específica de negócio
    """
    
    def _read_data(self, **kwargs) -> DataFrame:
        """Lê dados de inventário do catálogo."""
        database = kwargs.get('database')
        table_name = kwargs.get('table_name')
        
        if not database or not table_name:
            raise ValueError("database e table_name são obrigatórios")
        
        return self.glue_handler.read_from_catalog(
            database=database,
            table_name=table_name
        )
    
    def _transform_data(self, df: DataFrame, **kwargs) -> Dict:
        """Transforma dados de inventário calculando estoques."""
        return self._calcular_estoque(df, kwargs.get('categoria'))
    
    def _get_congregado_key(self, **kwargs) -> str:
        """Gera chave primária para inventário."""
        database = kwargs.get('database')
        table_name = kwargs.get('table_name')
        categoria = kwargs.get('categoria', 'all')
        return f"inventory_{database}_{table_name}_{categoria}"
    
    def _get_congregado_metadata(self, **kwargs) -> Dict:
        """Gera metadados específicos para inventário."""
        metadata = super()._get_congregado_metadata(**kwargs)
        metadata.update({
            'database': kwargs.get('database'),
            'table_name': kwargs.get('table_name'),
            'categoria': kwargs.get('categoria'),
            'tipo': 'inventario'
        })
        return metadata
    
    def _calcular_estoque(self, df: DataFrame, categoria: str = None) -> Dict:
        """
        Calcula estoque total e por categoria.
        
        Args:
            df: DataFrame com dados de inventário
            categoria: Categoria específica (opcional)
        
        Returns:
            Dicionário com cálculos de estoque
        """
        total_items = df.count()
        
        resultado = {
            'total_items': total_items,
            'categoria_filtro': categoria or 'all'
        }
        
        # Filtrar por categoria se especificado
        if categoria and 'categoria' in df.columns:
            df = df.filter(F.col('categoria') == categoria)
            resultado['items_na_categoria'] = df.count()
        
        # Calcular totais se houver coluna de quantidade
        if 'quantidade' in df.columns:
            total_quantidade = df.agg(F.sum('quantidade').alias('total')).collect()[0]['total']
            resultado['total_quantidade'] = float(total_quantidade) if total_quantidade else 0.0
        
        # Calcular valor total se houver coluna de valor
        if 'valor' in df.columns:
            total_valor = df.agg(F.sum('valor').alias('total')).collect()[0]['total']
            resultado['valor_total'] = float(total_valor) if total_valor else 0.0
        
        # Agrupar por categoria se existir
        if 'categoria' in df.columns:
            por_categoria = df.groupBy('categoria').agg(
                F.count('*').alias('items'),
                F.sum('quantidade' if 'quantidade' in df.columns else F.lit(0)).alias('total_qtd')
            ).collect()
            resultado['por_categoria'] = [
                {'categoria': row['categoria'], 'items': row['items'], 'total_qtd': row['total_qtd']}
                for row in por_categoria
            ]
        
        return resultado
    
    def processar_inventario(
        self,
        database: str,
        table_name: str,
        categoria: str = None,
        output_path: str = None
    ) -> Dict:
        """
        Processa inventário usando o template method.
        
        Args:
            database: Nome do banco de dados
            table_name: Nome da tabela
            categoria: Categoria específica (opcional)
            output_path: Caminho de saída (opcional)
        
        Returns:
            Dicionário com resultado do processamento
        """
        return self.process(
            database=database,
            table_name=table_name,
            categoria=categoria,
            output_path=output_path
        )
