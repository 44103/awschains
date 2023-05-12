FROM python:3.9

ARG DEBIAN_FRONTEND="noninteractive"
RUN apt-get update && apt-get -y install --no-install-recommends \
    git \
    zsh \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Tokyo
RUN zsh -c "`curl -L raw.github.com/44103/dotfiles/main/install.sh`"
