#!/usr/bin/env python3
"""
Script para criar a tabela DynamoDB para controle de jornada.

Uso:
    python scripts/create_journey_table.py --table-name journey_control --region sa-east-1
"""

import argparse
import boto3
from botocore.exceptions import ClientError


def create_journey_table(table_name: str, region_name: str = "sa-east-1"):
    """
    Cria a tabela DynamoDB para controle de jornada.
    
    Args:
        table_name: Nome da tabela
        region_name: Região AWS
    """
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    
    try:
        # Verificar se a tabela já existe
        table = dynamodb.Table(table_name)
        table.load()
        print(f"✓ Tabela '{table_name}' já existe na região '{region_name}'")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            print(f"✗ Erro ao verificar tabela: {e}")
            return False
    
    # Criar tabela
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'journey_id',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'journey_id',
                    'AttributeType': 'S'  # String
                }
            ],
            BillingMode='PAY_PER_REQUEST',  # On-demand pricing
            Tags=[
                {
                    'Key': 'Purpose',
                    'Value': 'JourneyControl'
                },
                {
                    'Key': 'ManagedBy',
                    'Value': 'GlueApplication'
                }
            ]
        )
        
        # Aguardar tabela ficar ativa
        print(f"⏳ Criando tabela '{table_name}' na região '{region_name}'...")
        table.wait_until_exists()
        print(f"✓ Tabela '{table_name}' criada com sucesso!")
        print(f"  - Chave primária: journey_id (String)")
        print(f"  - Modo de cobrança: PAY_PER_REQUEST (on-demand)")
        return True
        
    except ClientError as e:
        print(f"✗ Erro ao criar tabela: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cria tabela DynamoDB para controle de jornada')
    parser.add_argument(
        '--table-name',
        type=str,
        default='journey_control',
        help='Nome da tabela DynamoDB (padrão: journey_control)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='sa-east-1',
        help='Região AWS (padrão: sa-east-1)'
    )
    
    args = parser.parse_args()
    
    success = create_journey_table(args.table_name, args.region)
    exit(0 if success else 1)
