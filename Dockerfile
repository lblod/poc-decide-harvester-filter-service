FROM semtech/mu-javascript-template:1.8.0
LABEL maintainer="Nordine Bittich <contact@bittich.be>"
ENV SUDO_QUERY_RETRY="true"
ENV SUDO_QUERY_RETRY_FOR_HTTP_STATUS_CODES="404,500,503"
