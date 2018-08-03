FROM grafana/grafana

# Copy in fixed assets
COPY ./assets/grafana_icon.svg /usr/share/grafana/public/img
COPY ./assets/fav32.png /usr/share/grafana/public/img

USER root

ADD branding.sh /usr/local/bin/branding.sh
RUN branding.sh

USER grafana
