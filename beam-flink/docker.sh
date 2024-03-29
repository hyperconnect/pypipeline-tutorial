#!/usr/bin/env bash
# ref. https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/

set -euxo pipefail

readonly OS_ID=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly OS_CODE=$(lsb_release -cs)
# TODO: Allow this to be configured by metadata.
readonly DOCKER_VERSION="18.06.0~ce~3-0~${OS_ID}"
readonly CREDENTIAL_HELPER_VERSION='1.5.0'


function is_master() {
  local role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ "$role" == 'Master' ]] ; then
    true
  else
    false
  fi
}

function get_docker_gpg() {
  curl -fsSL https://download.docker.com/linux/${OS_ID}/gpg
}

function update_apt_get() {
  for ((i = 0; i < 10; i++)) ; do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_docker() {
  update_apt_get
  apt-get install -y apt-transport-https ca-certificates curl gnupg2
  get_docker_gpg | apt-key add -
  echo "deb [arch=amd64] https://download.docker.com/linux/${OS_ID} ${OS_CODE} stable" >/etc/apt/sources.list.d/docker.list
  update_apt_get
  apt-get install -y docker-ce="${DOCKER_VERSION}"
}

function configure_gcr() {
  # this standalone method is recommended here:
  # https://cloud.google.com/container-registry/docs/advanced-authentication#standalone_docker_credential_helper
  curl -fsSL "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v${CREDENTIAL_HELPER_VERSION}/docker-credential-gcr_linux_amd64-${CREDENTIAL_HELPER_VERSION}.tar.gz" \
    | tar xz --to-stdout ./docker-credential-gcr \
    > /usr/local/bin/docker-credential-gcr && chmod +x /usr/local/bin/docker-credential-gcr

  # this command configures docker on a per-user basis. Therefore we configure
  # the root user, as well as the yarn user which is part of the docker group.
  # If additional users are added to the docker group later, this command will
  # need to be run for them as well.
  docker-credential-gcr configure-docker
  su yarn --command "docker-credential-gcr configure-docker"
}

function configure_docker() {
  # The installation package should create `docker` group.
  usermod -aG docker yarn
  # configure docker to use Google Cloud Registry
  configure_gcr

  systemctl enable docker
  # Restart YARN daemons to pick up new group without restarting nodes.
  if is_master ; then
    systemctl restart hadoop-yarn-resourcemanager
  else
    systemctl restart hadoop-yarn-nodemanager
  fi
}

function main() {
  install_docker
  configure_docker
}

main "$@"
