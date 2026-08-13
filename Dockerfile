# The ticker has no dependencies, so there is nothing to install and no
# lockfile to drift. The image is node plus three files.
FROM node:20-alpine

WORKDIR /app
COPY src/ ./src/

# Where a `start=now` family's epoch is remembered. Mount a volume here if you
# use that mode; families with an explicit epoch need no state at all.
VOLUME /var/lib/ofm-ticker
ENV TICK_STATE=/var/lib/ofm-ticker/state.json

# Read-only access to the Docker socket is all it needs, and all it should have.
# Mount with :ro — this service only ever lists and watches.
ENV DOCKER_HOST_SOCKET=/var/run/docker.sock

USER node
CMD ["node", "src/ticker.js"]
