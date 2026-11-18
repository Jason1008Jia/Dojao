# Dojao

**A distributed data science engine for the AI era** — query MySQL / TiDB / Doris / PostgreSQL / SQL Server /
Oracle / Hive / ClickHouse / Excel in one SQL, run Python vectorized UDFs, and let a built-in AI agent
explore data, execute SQL and render charts — without moving data.

## Repository

- GitHub: <https://github.com/Jason1008Jia/Dojao>
- Clone: `git clone https://github.com/Jason1008Jia/Dojao.git`

## Documentation

- Chinese README: [README.md](README.md)
- UDF protocol (DFP): [UDF_PROTOCOL.md](UDF_PROTOCOL.md)
- Custom connectors: [docs/custom-connector.md](docs/custom-connector.md)
- Connector example (`csvdir`, zero dependencies): [examples/dojao-connector-example/](examples/dojao-connector-example)
- Linux deployment: [docs/deploy-linux.md](docs/deploy-linux.md)
- Three-engine benchmark methodology: [docs/benchmark-dojao-trino-spark.md](docs/benchmark-dojao-trino-spark.md)
- AI capabilities and roadmap: [docs/ai-roadmap.md](docs/ai-roadmap.md)
- Memory model: [docs/memory-model-optimization.md](docs/memory-model-optimization.md)
- Architecture report: [docs/architecture-report.html](docs/architecture-report.html)

Known limitations are listed in the Chinese README under
[已知边界（Limitations）](README.md#已知边界limitations).

## Build

```bash
git clone https://github.com/Jason1008Jia/Dojao.git
cd Dojao
mvn clean package -DskipTests
```

Outputs: `dojao-dist/target/dojao-0.1.0.tar.gz` (self-contained distribution) and
`dojao-jdbc/target/dojao-jdbc-0.1.0-all.jar` (shaded JDBC driver, shipped separately).

## License

[Apache License 2.0](LICENSE)
