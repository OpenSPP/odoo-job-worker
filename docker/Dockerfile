# syntax=docker/dockerfile:1
# Minimal Dockerfile for running job_worker tests against Odoo 19.0
#
# Build: docker build -f docker/Dockerfile -t job-worker-test .

ARG PYTHON_VERSION=3.13

# =============================================================================
# Stage 1: Builder - Install Python packages with build dependencies
# =============================================================================
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

ARG ODOO_SOURCE=OCA/OCB
ARG ODOO_VERSION=19.0

# Install build dependencies
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        libffi-dev \
        libfreetype6-dev \
        libjpeg-dev \
        liblcms2-dev \
        libldap2-dev \
        libopenjp2-7-dev \
        libpq-dev \
        libsasl2-dev \
        libssl-dev \
        libtiff5-dev \
        libwebp-dev \
        libxml2-dev \
        libxslt-dev \
        zlib1g-dev

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install uv for fast dependency management
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /opt/venv/bin/

# Install Odoo requirements and Odoo itself
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    --mount=type=cache,target=/tmp/downloads,sharing=locked \
    set -eux; \
    ODOO_REQS="/tmp/downloads/odoo-${ODOO_VERSION}-requirements.txt"; \
    if [ ! -s "$ODOO_REQS" ]; then \
        rm -f "$ODOO_REQS"; \
        curl -sSL -o "$ODOO_REQS" \
            "https://raw.githubusercontent.com/${ODOO_SOURCE}/${ODOO_VERSION}/requirements.txt"; \
    fi; \
    uv pip install -r "$ODOO_REQS"

# Download and install Odoo
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    --mount=type=cache,target=/tmp/downloads,sharing=locked \
    set -eux; \
    ODOO_TARBALL="/tmp/downloads/odoo-${ODOO_VERSION}.tar.gz"; \
    if [ ! -f "$ODOO_TARBALL" ] || ! gzip -t "$ODOO_TARBALL" 2>/dev/null; then \
        rm -f "$ODOO_TARBALL"; \
        curl -sSL -o "$ODOO_TARBALL" \
            "https://github.com/${ODOO_SOURCE}/archive/refs/heads/${ODOO_VERSION}.tar.gz"; \
    fi; \
    mkdir -p /opt/odoo; \
    tar -xzf "$ODOO_TARBALL" -C /opt/odoo --strip-components=1; \
    mv /opt/odoo /opt/odoo-src; \
    mkdir -p /opt/odoo; \
    mv /opt/odoo-src /opt/odoo/odoo; \
    uv pip install -e /opt/odoo/odoo

# Precompile Python files
RUN python -m compileall -q /opt/venv/lib/ || true

# =============================================================================
# Stage 2: Runtime - Minimal image for testing
# =============================================================================
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

ARG ODOO_VERSION=19.0
ARG UID=1000
ARG GID=1000
ARG PG_VERSION=16

# Environment variables
ENV ODOO_VERSION="${ODOO_VERSION}" \
    ODOO_RC=/etc/odoo/odoo.conf \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PATH="/opt/venv/bin:/home/odoo/.local/bin:$PATH" \
    VIRTUAL_ENV=/opt/venv \
    PYTHONUNBUFFERED=1

# Install runtime dependencies
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        gnupg2 \
        gettext-base \
        libfreetype6 \
        libjpeg62-turbo \
        liblcms2-2 \
        libldap-2.5-0 \
        libopenjp2-7 \
        libpq5 \
        libsasl2-2 \
        libtiff6 \
        libwebp7 \
        libxml2 \
        libxslt1.1 \
    ; \
    # Install PostgreSQL client
    echo 'deb https://apt.postgresql.org/pub/repos/apt/ bookworm-pgdg main' > /etc/apt/sources.list.d/postgresql.list; \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg; \
    apt-get update; \
    apt-get install -y --no-install-recommends postgresql-client-${PG_VERSION}

# Create odoo user and directories
RUN set -eux; \
    groupadd -g ${GID} odoo; \
    useradd -l -m -d /home/odoo -s /bin/bash -u ${UID} -g ${GID} odoo; \
    mkdir -p /var/lib/odoo /mnt/extra-addons /etc/odoo; \
    chown -R odoo:odoo /var/lib/odoo /mnt/extra-addons /etc/odoo

# Copy virtual environment and Odoo from builder
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/odoo /opt/odoo

# Configuration and entrypoint
COPY docker/odoo.conf.template /etc/odoo/odoo.conf.template
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh; \
    chown odoo:odoo /etc/odoo/odoo.conf.template /entrypoint.sh

WORKDIR /var/lib/odoo
USER odoo

ENTRYPOINT ["/entrypoint.sh"]
CMD ["odoo"]
