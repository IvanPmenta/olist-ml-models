-- Databricks notebook source
WITH tb_join AS (
 SELECT  DISTINCT 
       t1.idPedido,
       t1.idCliente,
       t2.idVendedor,
       t1.dtPedido,
       t3.descUF

 FROM silver.olist.pedido AS t1

 LEFT JOIN silver.olist.item_pedido t2
 ON T1.idPedido = t2.idPedido

 LEFT JOIN silver.olist.cliente as t3
 ON t1.idCliente = t3.idCliente

 WHERE t1.dtPedido <= '2018-01-01'
 AND t1.dtPedido >= add_months('2018-01-01', -6)
 AND t2.idVendedor IS NOT NULL
),

tb_grouped AS (

  SELECT 
      idVendedor,
      COUNT (DISTINCT descUf) as qtdUFsPedidos,
      COUNT(DISTINCT CASE WHEN descUf = 'AC' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoAC, 
      COUNT(DISTINCT CASE WHEN descUf = 'AL' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoAL, 
      COUNT(DISTINCT CASE WHEN descUf = 'AM' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoAM, 
      COUNT(DISTINCT CASE WHEN descUf = 'AP' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoAP, 
      COUNT(DISTINCT CASE WHEN descUf = 'BA' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoBA, 
      COUNT(DISTINCT CASE WHEN descUf = 'CE' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoCE, 
      COUNT(DISTINCT CASE WHEN descUf = 'DF' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoDF, 
      COUNT(DISTINCT CASE WHEN descUf = 'ES' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoES, 
      COUNT(DISTINCT CASE WHEN descUf = 'GO' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoGO, 
      COUNT(DISTINCT CASE WHEN descUf = 'MA' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoMA, 
      COUNT(DISTINCT CASE WHEN descUf = 'MG' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoMG, 
      COUNT(DISTINCT CASE WHEN descUf = 'MS' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoMS, 
      COUNT(DISTINCT CASE WHEN descUf = 'MT' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoMT, 
      COUNT(DISTINCT CASE WHEN descUf = 'PA' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPA, 
      COUNT(DISTINCT CASE WHEN descUf = 'PB' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPB, 
      COUNT(DISTINCT CASE WHEN descUf = 'PE' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPE, 
      COUNT(DISTINCT CASE WHEN descUf = 'PI' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPI, 
      COUNT(DISTINCT CASE WHEN descUf = 'PR' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPR, 
      COUNT(DISTINCT CASE WHEN descUf = 'RJ' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRJ, 
      COUNT(DISTINCT CASE WHEN descUf = 'RN' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRN, 
      COUNT(DISTINCT CASE WHEN descUf = 'RO' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRO, 
      COUNT(DISTINCT CASE WHEN descUf = 'RR' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRR, 
      COUNT(DISTINCT CASE WHEN descUf = 'RS' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRS, 
      COUNT(DISTINCT CASE WHEN descUf = 'SC' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoSC, 
      COUNT(DISTINCT CASE WHEN descUf = 'SE' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoSE, 
      COUNT(DISTINCT CASE WHEN descUf = 'SP' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoSP, 
      COUNT(DISTINCT CASE WHEN descUf = 'TO' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoTO

    FROM tb_join
    GROUP BY idVendedor
)

SELECT  
      '2018-01-01' AS DtReference,
       *  
  FROM tb_grouped
