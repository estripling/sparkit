#!/usr/bin/env bash
set -e

echo 'Disable Oh My Zsh plugin: Git'
sed -i -e 's/plugins=(git)/plugins=()/g' ${HOME}/.zshrc

echo 'Add env variables to zshrc'
echo 'JAVA_HOME="/usr/local/sdkman/candidates/java/current"' >> ${HOME}/.zshrc
echo 'SPARK_HOME="/usr/local/sdkman/candidates/spark/current"' >> ${HOME}/.zshrc
echo 'export PATH="${JAVA_HOME}/bin:${PATH}"' >> ${HOME}/.zshrc
echo 'export PATH="${SPARK_HOME}/bin:${PATH}"' >> ${HOME}/.zshrc

echo 'Post creation script complete'
