FROM grafana/grafana

COPY ./assets/grafana_icon.svg /usr/share/grafana/public/img

COPY ./assets/fav32.png /usr/share/grafana/public/img

USER root

RUN grep -rl '<i class="icon-gf icon-gf-grafana_wordmark"></i>' /usr/share/grafana/public | xargs sed -i 's/<i class="icon-gf icon-gf-grafana_wordmark"><\/i>/<p><font size="5">AE Portal<\/font><\/p>/g'

RUN grep -rl '<title>Grafana</title>' /usr/share/grafana/public | xargs sed -i 's/<title>Grafana<\/title>/<title>AE Portal<\/title>/g'

RUN grep -rl '\[hidden\],template' /usr/share/grafana/public/build | xargs sed -i 's/\[hidden\],template/\[hidden\],template,footer/g'

# RUN grep -rl '.pluginlist-section{.*}' /usr/share/grafana/public/build | xargs sed -i 's/.pluginlist-section{.*}/.pluginlist-section{display:none}/g'
