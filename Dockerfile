# Попытка написания примера докерфайла.
# Скрипт создаст образ с Samtools и Biobambam,
# запихнутыми, в свою очередь, в Conda.

# Установка Docker и настройка rootless-доступа к нему:
# https://github.com/PlatonB/ngs-pipelines#установка-зависимостей

# Создание образа:
# cd путь/к/папке/с/докерфайлом
# docker build -t sambamfigam .

# К примеру, индексация какой-нибудь фасты:
# src=$HOME/ngs
# dst=/home
# docker run --mount type=bind,src=$src,dst=$dst sambamfigam samtools faidx $dst/assembly.fa.gz
# sudo chmod 777 $src/assembly.fa.gz.fai

FROM ubuntu
RUN useradd username
USER username
FROM continuumio/miniconda3
RUN conda config --add channels defaults \
&& conda config --add channels bioconda \
&& conda config --add channels conda-forge \
&& conda update -y --all
RUN conda install -y samtools=1.10
RUN conda install -y biobambam=2.0.87
CMD samtools --version \
&& bamsort --version
