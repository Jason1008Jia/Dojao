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
- Linux deployment: [docs/deploy-linux.md](docs/deploy-linux.md)

## Build

```bash
git clone https://github.com/Jason1008Jia/Dojao.git
cd dojao
mvn clean package -DskipTests
```

## License

[Apache License 2.0](LICENSE)
