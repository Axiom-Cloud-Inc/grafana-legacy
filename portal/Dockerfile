FROM grafana/grafana

# Install simplejson plugin
RUN grafana-cli plugins install grafana-simple-json-datasource

# Copy in fixed assets
COPY ./assets/grafana_icon.svg /usr/share/grafana/public/img
COPY ./assets/fav32.png /usr/share/grafana/public/img

# Switch to root user for modifying asset files
USER root

# Run branding script
ADD branding.sh /usr/local/bin/branding.sh
RUN branding.sh

# Switch back to grafana user for all other ops
USER grafana
