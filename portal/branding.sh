#!/usr/bin/env bash

set -e

# Script to edit all public assets and brand identity with Axiom Exergy

# Specific call outs
grep -rl '<i class="icon-gf icon-gf-grafana_wordmark"></i>' /usr/share/grafana/public | xargs sed -i 's/<i class="icon-gf icon-gf-grafana_wordmark"><\/i>/<p><font size="5">Axiom Exergy<\/font><\/p>/g'
grep -rl '\[hidden\],template' /usr/share/grafana/public/build | xargs sed -i 's/\[hidden\],template/\[hidden\],template,footer/g'
grep -rl '.pluginlist-section{.*}' /usr/share/grafana/public/build | xargs sed -i 's/.pluginlist-section{.*}/.pluginlist-section{display:none}/g'

# General asset strings
cd /usr/share/grafana/public
for name in $(grep --include="*.js" --include="*.ts" --include="*.html" -lR 'Grafana' .); do
  echo "Editing $name..."
  sed -i.sed1 -E -e 's/([^-/6][" >])Grafana([<.\ ?][^c])/\1Axiom Exergy\2/g' $name
  sed -i.sed2 -E -e 's/\\tGrafana/\\tAxiom Exergy/g' $name
  sed -i.sed3 -E -e "s/'Grafana'/'Axiom Exergy'/g" $name
  sed -i.sed4 -E -e 's/"Grafana"/"Axiom Exergy"/g' $name
  sed -i.sed5 -E -e "s/([' ])Grafana([' ])/\1Axiom Exergy\2/g" $name
  rm $name.sed?
  echo "Done."
done
