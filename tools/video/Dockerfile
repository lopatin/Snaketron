FROM mcr.microsoft.com/playwright:v1.52.0-noble

COPY tools/video/fonts-local.conf /etc/fonts/conf.d/99-snaketron-font-aliases.conf

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg fonts-liberation2 fontconfig \
    && rm -rf /var/lib/apt/lists/* \
    && fc-cache -f

WORKDIR /workspace
COPY client/web/package.json client/web/package-lock.json ./client/web/
RUN cd client/web \
    && npm ci \
    && npx playwright install chromium-headless-shell \
    && shell_path="$(find /ms-playwright -type f -name headless_shell | head -n 1)" \
    && test -n "$shell_path" \
    && ln -sf "$shell_path" /usr/local/bin/snaketron-headless-shell

ENV SNAKETRON_HEADLESS_SHELL=/usr/local/bin/snaketron-headless-shell

# Source is bind-mounted by the runbook so the image never hides a worktree's
# freshly built client/pkg behind an image-layer copy.
ENTRYPOINT ["node", "tools/video/capture.mjs"]
