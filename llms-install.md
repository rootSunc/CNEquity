# Install the CNEquity MCP server

Install with Python 3.11 or newer and run the offline sample in an empty working directory.

```bash
python -m pip install cnequity
cne init --profile sample
```

The sample is synthetic and only suitable for an installation check. For market data, choose a real profile in the [quickstart](https://rootsunc.github.io/CNEquity/en/getting-started/quickstart/#init-scope).

Register the stdio server in the MCP client with command `cne` and arguments `["mcp", "--config", "/path/to/config.toml"]`. Use the absolute config path printed by initialization. For the default sample, the config is `configs/cnequity.sample.toml` under the initialization directory.

Call `describe_lake` to confirm the selected lake and its coverage. The [MCP guide](https://rootsunc.github.io/CNEquity/en/reference/mcp/) covers the client configuration and authenticated HTTP transport.
