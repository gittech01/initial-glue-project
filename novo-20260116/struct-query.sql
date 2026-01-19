/*
Todas as tabelas envolvidas na construção do contexto da regra de negócio são todas fictícias.
-- Objetivo é exemplificar a estrutura da query para a regra aplicação da regra de negócio e ingestão dos dados na tabela final.

Tabela a ser feita ingestão: tbl_processado_operacao_consolidada
Tabela da camada SoR (Online): tbl_processado_operacao_sor
Tabela da camada SoT (Batch): tbl_processado_operacao_apropriada

Obs: As três (tbl_processado_operacao_sor, tbl_processado_operacao_apropriada, tbl_processado_operacao_consolidada) 
tem o mesmo schema (são completamente idêntica), porém respondem a perguntas diferentes.

Campos de cruzamento:
    - num_oper
    - cod_idef_ver_oper ou cod_idef_even_prcs (dependendo da situação)
    
Os campos principais e exclusivos que precisam ser aplicadas a regra: 
    - dat_vlr_even_oper
    - num_prio_even_oper
    - dat_recm_even_oper

As tabelas, SoR e SoT, possuem auxiliares que são usados para montar a comparação.
Auxiliares da tabela SoR:
    - tbl_operecao_sor
    - tbl_evento_processado_sor
    - tbl_posicao_operacao_sor

Auxiliares da tabela SoT:
    - tbl_operecao_apropriada
    - tbl_evento_processado_apropriada
    - tbl_posicao_operacao_apropriada


A regra de negócio aplicada a posição da operação é a seguinte:

    - se dat_vlr_even_oper maior
    - senao, num_prio_even_oper maior
    - senao, dat_recm_even_oper maior
    - senao, origem_registro_preferencial 'online'

*/


-- Traduzindo a regra de negócio para a query sql

-- Consulta base SoR (Online)
with cte_sql_sor as (
    select oper.num_oper as num_oper,
            oper.cod_idef_ver_oper as cod_idef_ver_oper,

            posi.dat_vlr_even_oper as dat_vlr_even_oper,
            posi.num_prio_even_oper as num_prio_even_oper,
            posi.dat_recm_even_oper as dat_recm_even_oper,
            'online' as origem
     from db_online.tbl_operecao_sor oper
              inner join db_online.tbl_evento_processado_sor event
                         on oper.num_oper = event.num_oper
                             and oper.cod_idef_ver_oper = event.cod_idef_ver_oper
              inner join db_online.tbl_posicao_operacao_sor posi
                         on oper.num_oper = posi.num_oper
                             and oper.cod_idef_even_prcs = posi.cod_idef_even_prcs
    -- maior partic da tabela principal SoR (Online)
    where anomesdia = (select max(anomesdia) from db_online.tbl_processado_operacao_sor)
)

-- Consulta base SoT (Batch)
, cte_sql_sot as (
    select 
         oper.num_oper as num_oper,
         oper.cod_idef_ver_oper as cod_idef_ver_oper,

         posi.dat_vlr_even_oper as dat_vlr_even_oper,
         posi.num_prio_even_oper as num_prio_even_oper,
         posi.dat_recm_even_oper as dat_recm_even_oper,
         'batch' as origem
    from db_batch.tbl_operecao_apropriada oper
           inner join db_batch.tbl_evento_processado_apropriada event
                      on oper.num_oper = event.num_oper
                          and oper.cod_idef_ver_oper = event.cod_idef_ver_oper
           inner join db_batch.tbl_posicao_operacao_apropriada posi
                      on oper.num_oper = posi.num_oper
                          and oper.cod_idef_even_prcs = posi.cod_idef_even_prcs
    -- maior partic da tabela principal SoT (Batch)
    where anomesdia = (select max(anomesdia) from db_batch.tbl_processado_operacao_apropriada)
)

-- União das consultas base SoR (Online) e SoT (Batch)
, union_cte as (
    select *
    from cte_sql_sor
    union
    select *
    from cte_sql_sot
)

-- Ranking das operações
, rank_oper as (
    select
        origem,
        num_oper,
        cod_idef_ver_oper,
        row_number() over (
            partition by
                num_oper
                , cod_idef_ver_oper
            order by
                dat_vlr_even_oper desc
                , num_prio_even_oper desc
                , dat_recm_even_oper desc
        ) as rank
    from union_cte
)

, cte_sor_final as (
    select
        sor.*
    from db_online.tbl_processado_operacao_sor sor
    inner join rank_oper
        on sor.num_oper = rank_oper.num_oper
        and sor.cod_idef_ver_oper = rank_oper.cod_idef_ver_oper
        and rank_oper.rank = 1
        and rank_oper.origem = 'online'
    where anomesdia = (select max(anomesdia) from db_online.tbl_processado_operacao_sor)
)
, cte_sot_final as (
    select
        sot.*
    from db_batch.tbl_processado_operacao_apropriada sot
    inner join rank_oper
        on sot.num_oper = rank_oper.num_oper
        and sot.cod_idef_ver_oper = rank_oper.cod_idef_ver_oper
        and rank_oper.rank = 1
        and rank_oper.origem = 'batch'
    where anomesdia = (select max(anomesdia) from db_batch.tbl_processado_operacao_apropriada)
)

-- União das consultas final SoR (Online) e SoT (Batch)
, union_cte_final as (
    select * from cte_sor_final
    union
    select * from cte_sot_final
)

-- resultado
-- ingestão na tabela final. A tabela sempre está no mesmo db que o da SoT (Batch)
insert into db_batch.tbl_processado_operacao_consolidada
select 
    *, 
    current_date() as anomesdia -- a partição
from union_cte_final



-- OBS:

Tu és um especialista em Engenharia de Analytics.
Objetivo: Implementar regra de negocio conforme mandate da arquitetura

Portanto sem rodeio e sem bajulação, eu quero respostas concretas e precisas que atendam a minha
necessidade conforme o arquivo que estarei passando  e flexível conforme é o do teu conhecimento
quanto as regras de negocio  e os joins conforme settings das tabelas.
--