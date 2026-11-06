# Streaming Orders — Kafka + Spark Structured Streaming

Pipeline de streaming de pedidos de e-commerce com arquitetura medalhão: **Bronze** (bruto, append-only) → **Silver** (schema, validação, deduplicação com watermark) → **Gold** (receita por categoria em janelas de 5 min por *event time*).

> **EN summary:** Spark Structured Streaming pipeline (Kafka or local file source) with bronze/silver/gold layers, event-time watermarking, deduplication of at-least-once duplicates, late-data handling and tumbling-window aggregates. Tested end-to-end with pytest and CI.

## Arquitetura
```
producer ──► Kafka (topic orders) ──► Bronze (raw) ──► Silver (parse+valida+dedup) ──► Gold (janelas 5 min)
             └─ fallback local: arquivos JSON-lines na landing
```

## Decisões técnicas
- **Fonte plugável**: `--source kafka` (docker compose, KRaft) ou `--source files` — o mesmo código roda localmente e nos testes sem broker.
- **Deduplicação** por `event_id` com `withWatermark(30 min)`: o estado é limitado, e reenvios *at-least-once* não inflam a receita.
- **Dados atrasados**: eventos com até 30 min de atraso entram; mais velhos que isso são descartados pelo watermark (trade-off explícito entre estado e completude).
- **Validação**: `amount > 0`, `quantity > 0`, chaves não nulas. Bronze preserva tudo para auditoria/reprocessamento.
- **Parquet** como formato local; trocar por Delta é só mudar `format("delta")` (requer pacote Delta no classpath).

## Como rodar
```bash
make install
make produce      # gera ~5 mil eventos (com duplicatas, atrasos e inválidos)
make run          # Bronze -> Silver -> Gold
make test && make lint
# Com Kafka real:
make kafka-up && KAFKA_BOOTSTRAP=localhost:9092 python -m src.producer && python -m src.stream_job --source kafka
```
Requer Java 17+.

## Resultado real de uma execução
| Camada | Linhas |
|---|---|
| Bronze | 5.154 |
| Silver | 4.883 (duplicatas e inválidos removidos; atrasados extremos descartados pelo watermark) |
| Gold | 225 (janela × categoria) |

## Limitações
- Gold é calculada em batch sobre a Silver (simplifica a leitura; versão 100% streaming com `outputMode("update")` é próximo passo).
- Sem schema registry; evolução de schema não tratada.

## Bullets para currículo
- Construí pipeline de streaming com Spark Structured Streaming e Kafka (arquitetura medalhão), com deduplicação por watermark, tratamento de eventos atrasados e agregações por janela de event time.
- Cobertura de testes end-to-end (pytest) e CI no GitHub Actions.
