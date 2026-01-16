with cte_sql_sor as (
    select oper.num_oper,
            oper.cod_idef_ver_oper,

            posi.dat_vlr_even_oper,
            posi.num_prio_even_oper,
            posi.dat_recm_even_oper,
            'online' as origem
     from db_online.tbl_operecao_sor oper
              inner join db_online.tbl_evento_processado_sor event
                         on oper.num_oper = event.num_oper
                             and oper.cod_idef_ver_oper = event.cod_idef_ver_oper
              inner join db_online.tbl_posicao_operacao_sor posi
                         on oper.num_oper = posi.num_oper
                             and oper.cod_idef_even_prcs = posi.cod_idef_even_prcs
     where anomesdia = '20260115' -- origem da tabela principal
)
, cte_sql_sot as (
    select oper.num_oper,
         oper.cod_idef_ver_oper,

         posi.dat_vlr_even_oper,
         posi.num_prio_even_oper,
         posi.dat_recm_even_oper,
         'batch' as origem
    from db_online.tbl_operecao_apropriada oper
           inner join db_online.tbl_evento_processado_apropriada event
                      on oper.num_oper = event.num_oper
                          and oper.cod_idef_ver_oper = event.cod_idef_ver_oper
           inner join db_online.tbl_posicao_operacao_apropriada posi
                      on oper.num_oper = posi.num_oper
                          and oper.cod_idef_even_prcs = posi.cod_idef_even_prcs
    where anomesdia = '20260115' -- origem da tabela principal
)
, union_cte as (
select *
from cte_sql_sor
union
select *
from cte_sql_sot
)

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

-- resultado
select
    origem,
    num_oper,
    cod_idef_ver_oper
from rank_oper
where rank = 1

Depois resultado serve como filtro com bases da SoR (tbl_processado_operacao_sor)
e SoT (tbl_processado_operacao_apropriada) com isso é montado a tbl_processado_operacao_consolidada