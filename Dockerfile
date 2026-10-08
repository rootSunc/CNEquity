# CNEquity MCP server over stdio, ready to inspect or try in a container.
#
# The image ships an offline SAMPLE lake (synthetic rows, source=mock) so the
# server starts and lists its tools with no network and no setup. It is for
# trying the tools and for MCP directories that introspect servers — not for
# research. For real data, mount your own lake and config:
#
#   docker run -i --rm -v /abs/lake:/abs/lake -v /abs/cnequity.toml:/config.toml \
#     cnequity mcp --config /config.toml
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip install cnequity \
    && cne init --profile sample --data-root /opt/cnequity/sample \
        --config-out /opt/cnequity/sample.toml

ENTRYPOINT ["cne"]
CMD ["mcp", "--config", "/opt/cnequity/sample.toml"]
